#include "client.h"

std::string my_ip;
std::string proxy_ip;
uint32_t offset;

static constexpr uint8_t kWarmup = 4;
static constexpr double
    kAppLatFac = 2.0;
volatile bool experiment_over = false;
size_t numa_node = 0;

// fwd decls for send_request
static void op_cont_func(void *, void *);
static void read_cont_func(void *, void *);
void send_request(ClientContext *, Operation *, bool);
size_t failover_to = 1;

// Handle connections initiated by us
void
sm_handler(int session_num __attribute__((unused)),
           erpc::SmEventType sm_event_type,
           erpc::SmErrType sm_err_type, void *_context) {
  auto *c = static_cast<ClientContext *>(_context);

  LOG_INFO("thread_id %zu: ", c->thread_id);

  erpc::rt_assert(
      sm_err_type == erpc::SmErrType::kNoError,
      "Got a SM error: " + erpc::sm_err_type_str(sm_err_type));

  if (!(sm_event_type == erpc::SmEventType::kConnected ||
      sm_event_type == erpc::SmEventType::kDisconnected)) {
    throw std::runtime_error("Unexpected SM event!");
  }

  if (sm_event_type == erpc::SmEventType::kConnected) {
    LOG_INFO("Got a connection, session %d!\n", session_num);
    c->nconnections++;
  } else {
    LOG_INFO("Lost a connection, session %d!\n", session_num);
    c->nconnections--;
  }
}

// Lifted nearly verbatim from erpc code:
// https://github.com/erpc-io/eRPC/blob/master/apps/small_rpc_tput/small_rpc_tput.cc
struct app_stats_t {
  double mrps;
  size_t num_re_tx;

  // Used only if latency stats are enabled
  double lat_us_50;
  double lat_us_99;
  double lat_us_999;
  double lat_us_9999;
  size_t pad[2];

  app_stats_t() { memset(this, 0, sizeof(app_stats_t)); }

  static std::string get_template_str() {
    std::string ret = "mrps num_re_tx";
    ret += " lat_us_50 lat_us_99 lat_us_999 lat_us_9999";
    return ret;
  }

  std::string to_string() {
    return std::to_string(mrps) + "," + std::to_string(lat_us_50) +
        "," + std::to_string(lat_us_99)
        + "," + std::to_string(lat_us_999)
        + "," + std::to_string(lat_us_9999)
        + "," + std::to_string(num_re_tx);
  }

  /// Accumulate stats
  app_stats_t &operator+=(const app_stats_t &rhs) {
    this->mrps += rhs.mrps;
    this->num_re_tx += rhs.num_re_tx;
    this->lat_us_50 += rhs.lat_us_50;
    this->lat_us_99 += rhs.lat_us_99;
    this->lat_us_999 += rhs.lat_us_999;
    this->lat_us_9999 += rhs.lat_us_9999;
    return *this;
  }
};

// Lifted (almost) verbatim from erpc code:
// https://github.com/erpc-io/eRPC/blob/master/apps/small_rpc_tput/small_rpc_tput.cc
void
ClientContext::print_stats() {
  double seconds = erpc::sec_since(tput_t0);
  double tput_mrps = stat_resp_rx_tot / (seconds * 1000000);
  app_stats[thread_id].mrps = tput_mrps;
  app_stats[thread_id].num_re_tx = rpc->pkt_loss_stats.num_re_tx;

  app_stats[thread_id].lat_us_50 = latency.perc(0.50) / kAppLatFac;
  app_stats[thread_id].lat_us_99 = latency.perc(0.99) / kAppLatFac;
  app_stats[thread_id].lat_us_999 = latency.perc(0.999) / kAppLatFac;
  app_stats[thread_id].lat_us_9999 = latency.perc(0.9999) / kAppLatFac;

  if (thread_id == 0) {
    stats = new char[100]();
    assert(stats != nullptr);

    app_stats_t accum;
    for (size_t i = 0; i < FLAGS_nthreads; i++) {
      accum += app_stats[i];
    }
    accum.lat_us_50 /= FLAGS_nthreads;
    accum.lat_us_99 /= FLAGS_nthreads;
    accum.lat_us_999 /= FLAGS_nthreads;
    accum.lat_us_9999 /= FLAGS_nthreads;
    sprintf(stats, "%s", accum.to_string().c_str());
  }

  stat_resp_rx_tot = 0;
  rpc->pkt_loss_stats.num_re_tx = 0;
  latency.reset();

  clock_gettime(CLOCK_REALTIME, &tput_t0);
}

// A way to record sequence numbers + timestamps for noops
void
noop_handler(erpc::ReqHandle *req_handle, void *_context) {
  auto *c = reinterpret_cast<ClientContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();

  client_payload_t *payload = reinterpret_cast<client_payload_t *>(
      req_msgbuf->buf);
  erpc::rt_assert(payload->reqtype == ReqType::kNoop,
                  "Got a non-noop request type in the handler!\n");

  if (DEBUG_SEQNUMS) {
    fprintf(c->fp, "%lu\n", payload->seqnum);
  } else if (PLOT_RECOVERY || CHECK_FIFO) {
    fprintf(c->fp, "%s %lu %lu\n",
            get_formatted_time_from_offset(offset).c_str(),
            payload->seqnum,
            payload->client_reqid);
  }

  LOG_FAILOVER("Client %zu received noop. Acking\n", c->thread_id);

  c->rpc->resize_msg_buffer(
      &req_handle->pre_resp_msgbuf, 1);
  c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}

// Check if operation's seqnum has been persisted at DTs; if yes,
// respond to client. If not, put in minPQ and wait for persistence.
inline void
corfu_append_cont_func(void *_context, void *_tag) {
  auto *c = reinterpret_cast<ClientContext *>(_context);
  auto *tag = reinterpret_cast<Tag *>(_tag);
  Operation *op = tag->op;

  tag->corfu_replies++;

  // have not finished replicating in Corfu
  if (tag->corfu_replies != kCorfuReplicationFactor) {
    int session_num = (*tag->c->log_pos_to_session_num(op->seqnum))[tag->corfu_replies];

    // assumes the entry is unperturbed in the msgbuffer buf
    c->rpc->enqueue_request(session_num,
                                 static_cast<uint8_t>(ReqType::kCorfuAppend),
                                 &tag->req_msgbuf, &tag->resp_msgbuf,
                                 corfu_append_cont_func, tag);
  } else {
    op_cont_func(c, tag);
  }
}

void seq_num_cont_func(void *_c, void *_tag) {
  ClientContext *c = reinterpret_cast<ClientContext *>(_c);
  Tag *tag = reinterpret_cast<Tag*>(_tag);
  payload_t *response = reinterpret_cast<payload_t *>(tag->resp_msgbuf.buf);
  tag->op->seqnum = response->seqnum;
  tag->corfu_replies = 0;
  c->rpc->resize_msg_buffer(&tag->req_msgbuf, sizeof(corfu_entry_t));
  erpc::rt_assert(tag->req_msgbuf.get_data_size() == sizeof(corfu_entry_t),
                  "req_msgbuf in submit_op was too small\n");

  auto *entry = reinterpret_cast<corfu_entry_t *>(tag->req_msgbuf.buf);
  entry->log_position = response->seqnum;

  // this is where callbacks will be
  int session_num;
  memcpy(entry->entry_val, tag->op->entry_val, MAX_CORFU_ENTRY_SIZE);
  session_num = (*c->log_pos_to_session_num(entry->log_position))[0];

  c->rpc->enqueue_request(session_num,
                          static_cast<uint8_t>(ReqType::kCorfuAppend),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          corfu_append_cont_func, reinterpret_cast<void *>(tag));
}

// this is the start of the append reqauest
void
send_request(ClientContext *c, Operation *op, bool new_request) {
  int i = op->local_idx;

  // only update this if it is a new request
  if (likely(new_request)) c->req_tsc[i] = erpc::rdtsc();
  c->last_retx[i] = erpc::rdtsc();

  Tag *tag = c->tag_pool.alloc();
  erpc::rt_assert(tag != nullptr, "send_request got alloc'd a null tag\n");
  tag->alloc_msgbufs(c, op);

  // setup corfu entry val
  memcpy(&op->entry_val, &(op->local_reqid), sizeof(op->local_reqid));

  erpc::rt_assert(tag->req_msgbuf.get_data_size() == sizeof(client_payload_t) &&
      sizeof(client_payload_t) >= MAX_CORFU_ENTRY_SIZE, "msgbuf wrong size\n");

  c->rpc->resize_msg_buffer(&tag->req_msgbuf, sizeof(payload_t));
  c->rpc->enqueue_request(c->seq_session_num,
                          static_cast<uint8_t>(ReqType::kGetSeqNum),
                          &(tag->req_msgbuf), &(tag->resp_msgbuf),
                          seq_num_cont_func, reinterpret_cast<void *>(tag));
}

void
send_read_request(ClientContext *c, Operation *op, bool new_request) {
  int i = op->local_idx;

  // only update this if it is a new request
  if (likely(new_request)) c->req_tsc[i] = erpc::rdtsc();

  Tag *tag = c->tag_pool.alloc();
  tag->alloc_msgbufs(c, op);
  tag->op = op;

  c->rpc->resize_msg_buffer(&tag->req_msgbuf, sizeof(corfu_entry_t));
  auto *req_entry = reinterpret_cast<corfu_entry_t *>(tag->req_msgbuf.buf);

  // choose a random position between 0 and max_log_position to read at
  if (FLAGS_max_log_position) {
    req_entry->log_position = static_cast<uint64_t>(
        static_cast<uint64_t>(std::rand())
        % c->max_log_position);
  } else {
    // this code should never actually execute?
    req_entry->log_position = op->read_pos;
  }

  erpc::rt_assert(tag->req_msgbuf.get_data_size() == sizeof(corfu_entry_t),
                  "req_msgbuf in submit_op was too small\n");

  // this is where callbacks will be
  int session_num;
  session_num = (*c->log_pos_to_session_num(req_entry->log_position))[kCorfuReplicationFactor - 1];

  c->rpc->enqueue_request(session_num,
                          static_cast<uint8_t>(ReqType::kCorfuRead),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          read_cont_func, reinterpret_cast<void *>(tag));
}

void
read_cont_func(void *_context, void *_tag) {
  auto tsc_now = erpc::rdtsc();

  auto tag = reinterpret_cast<Tag *>(_tag);
  auto op = tag->op;
  auto c = static_cast<ClientContext *>(_context);

  size_t i = op->local_idx;

  // this is a failure-with-continuation
  if (unlikely(tag->resp_msgbuf.get_data_size() == 0)) {
    erpc::rt_assert(false, "Client Failure with continuation!\n");
  }

  auto *payload =
      reinterpret_cast<corfu_entry_t *>(tag->resp_msgbuf.buf);

  // This is a good response! was 10000
  if (unlikely(payload->log_position % 10000000 == 0)) {
    printf(
        "Thread %lu: Got read response retcode %u log_position %lu val %lu for local_reqid %lu, idx %zu from node %zu latency %f\n",
        c->thread_id, static_cast<uint8_t>(payload->return_code),
        payload->log_position, *reinterpret_cast<int64_t *>(payload->entry_val),
        op->local_reqid, i, op->cur_px_conn,
        erpc::to_usec(tsc_now - c->req_tsc[i], c->rpc->get_freq_ghz()));
  }


  // Create the new op
  if (likely(!force_quit && !experiment_over)) {
    // Record latency stuff
    if (likely(c->state == State::experiment)) {
      size_t req_tsc = c->req_tsc[i];
      double
          req_lat_us = erpc::to_usec(tsc_now - req_tsc, c->rpc->get_freq_ghz());
      c->latency.update(static_cast<size_t>(req_lat_us * kAppLatFac));
      c->stat_resp_rx_tot++;
    }

    c->ops[i]->reset(c->reqid_counter++);

    c->tag_pool.free(tag);
    send_read_request(c, op, true);
  } else if (unlikely(experiment_over)) {
    c->tag_pool.free(tag);
    c->completed_slots++;
  }
}

void
op_cont_func(void *_context, void *_tag) {
  auto tsc_now = erpc::rdtsc();

  fflush(stdout);
  auto tag = reinterpret_cast<Tag *>(_tag);
  Operation *op = tag->op;
  ClientContext *c = static_cast<ClientContext *>(_context);

  size_t i = op->local_idx;

  // this is a failure-with-continuation
  if (unlikely(tag->resp_msgbuf.get_data_size() == 0)) {
    erpc::rt_assert(false, "Client Failure with continuation!\n");
  }

  // this is for proxy dailure detection which doesn't happen anymore
  op->received_response = true;

  // This is a good response! was 10000
  if (unlikely(op->seqnum % 10000 == 0)) {
    LOG_INFO(
        "Thread %zu: Got seqnum %zu for local_reqid %zu, idx %zu from node %zu latency %f\n",
        c->thread_id, op->seqnum, op->local_reqid, i, op->cur_px_conn,
        erpc::to_usec(tsc_now - c->req_tsc[i], c->rpc->get_freq_ghz()));
  }

  // Create the new op
  if (likely(!force_quit && !experiment_over)) {
    // Record latency stuff
    if (likely(c->state == State::experiment)) {
      size_t req_tsc = c->req_tsc[i];
      double req_lat_us =
          erpc::to_usec(tsc_now - req_tsc, c->rpc->get_freq_ghz());
      c->latency.update(static_cast<size_t>(req_lat_us * kAppLatFac));
      c->stat_resp_rx_tot++;
    }

    c->ops[i]->reset(c->reqid_counter++);

    c->tag_pool.free(tag);
    send_request(c, op, true);
  } else if (unlikely(experiment_over)) {
    c->tag_pool.free(tag);
    c->completed_slots++;
  }
}

// Initialize state needed for client operation
void
create_client(ClientContext *c) {
  (void) c;
}

void
establish_proxy_connection(ClientContext *c,
                           int remote_tid,
                           std::string proxy_ip) {
  std::string uri;
  std::string port = ":31850";
  uri = proxy_ip + port;

  LOG_INFO("Thread %zu: Connecting with uri %s, remote_tid %d\n",
           c->thread_id, uri.c_str(), remote_tid);

  auto sn = c->rpc->create_session(uri, remote_tid);
  erpc::rt_assert(sn >= 0, "Failed to create session");

  c->sessions.push_back(sn);

  while (!c->rpc->is_connected(sn) && !force_quit) {
    c->rpc->run_event_loop_once();
  }
  LOG_INFO("Thread %zu: connected!\n", c->thread_id);
}

void
establish_sequencer_connection(ClientContext *c, int remote_tid,
                               std::string sequencer_ip) {
  std::string uri;
  std::string port = ":31850";
  uri = sequencer_ip + port;

  LOG_INFO("Thread %zu: Connecting with uri %s, remote_tid %d\n",
           c->thread_id, uri.c_str(), remote_tid);

  auto sn = c->rpc->create_session(uri, remote_tid);
  erpc::rt_assert(sn >= 0, "Failed to create session");

  c->seq_session_num = sn;

  while (!c->rpc->is_connected(sn) && !force_quit) {
    c->rpc->run_event_loop_once();
  }
  LOG_INFO("Thread %zu: Connected to the sequencer with session num %d\n",
           c->thread_id, c->seq_session_num);
}

// Connect to Rpc endpoints and push session numbers onto session vector
// for future use
int
connect_to_corfu_machine(ClientContext *c,
                         size_t idx, size_t rep_num, size_t sv_i) {
  std::string uri;
  std::string port = ":31850";
  int session_num;

  uri = c->corfu_ips[idx] + port;

  for (uint8_t thread = 0; thread < N_CORFUTHREADS; thread++) {
    printf("[%zu] Connecting CorfuServer with uri %s, remote_tid %d\n",
           c->thread_id, uri.c_str(), thread);

    session_num = c->rpc->create_session(uri, thread);

    erpc::rt_assert(session_num >= 0, "Failed to create session");

    while (!c->rpc->is_connected(session_num) && !force_quit) {
      c->rpc->run_event_loop_once();
    }

    c->corfu_session_nums[sv_i + thread][rep_num] = session_num;
    printf("[%zu] Connected to sv_i %zu thread %d  rep_num %zu with"
           " session num: %d\n",
           c->thread_id, sv_i, thread, rep_num, session_num);
  }
  fflush(stdout);

  return 0;
}

// n_corfu_servers is the number of logical servers (threads)
void
establish_corfu_connections(ClientContext *c) {
  printf("Establishing corfuips size %lu NCORFUTHREADS %d repfac %zu %lu "
         "Corfu connections... \n",
         c->corfu_ips.size(), N_CORFUTHREADS, kCorfuReplicationFactor,
         (c->corfu_ips.size()*N_CORFUTHREADS)/kCorfuReplicationFactor);
  c->corfu_session_nums.resize((c->corfu_ips.size()*N_CORFUTHREADS)/kCorfuReplicationFactor);

  for (size_t i = 0; i < c->corfu_session_nums.size(); i++)
    c->corfu_session_nums[i].resize(kCorfuReplicationFactor);

  size_t rep_num = 0;
  size_t j = 0;

  for (size_t i = 0; i < c->corfu_ips.size(); i++) {
    connect_to_corfu_machine(c, i, rep_num, j);

    rep_num = (rep_num + 1) % kCorfuReplicationFactor;
    if (rep_num == 0) j += N_CORFUTHREADS;
  }

  printf("...done establishing Corfu connections.\n");
}

uint16_t
get_client_id(size_t thread_id) {
  std::istringstream ss(my_ip);
  std::string last_octet;
  getline(ss, last_octet, '.');
  getline(ss, last_octet, '.');
  getline(ss, last_octet, '.');
  getline(ss, last_octet, '.');

  auto client_id = static_cast<uint16_t>(std::stoul(last_octet));
  client_id <<= 4;
  client_id |= thread_id;
  return client_id;
}

/***
 * tid: this clients thread id
 * nexus: pointer to machine's nexus
 * app_stats: pointer to machine's app_stats
 * proxy_id: the logical proxy (replica group) id this client thread should submit requests to
 *
 * TODO: rebalance clients based on physical threads
 *
 ***/
void
client_thread_func(size_t tid, erpc::Nexus *nexus, app_stats_t *app_stats,
                   uint16_t proxy_id_start)//, size_t raft_node_id)
{
  ClientContext c;
  c.app_stats = app_stats;
  c.thread_id = tid;
  c.nconnections = 0;
  c.proxy_id = proxy_id_start + (tid % FLAGS_nproxy_threads);
  c.alive_reps[0] = true;
  c.alive_reps[1] = true;
  c.alive_reps[2] = true;

  c.client_id = get_client_id(tid);

  // make a list of all the corfu ips
  boost::split(c.corfu_ips, FLAGS_corfu_ips, boost::is_any_of(","));

  LOG_INFO("Thread %zu: In client_thread_func\n", c.thread_id);
  printf("got to thread_func\n"); fflush(stdout);

  c.max_log_position = FLAGS_max_log_position;

  erpc::rt_assert(RAND_MAX > c.max_log_position);
  
  LOG_INFO("Thread %zu client_id is %u mlp %zu sizeof client_payload_t %zu\n",
           c.thread_id, c.client_id, c.max_log_position,
           sizeof(client_payload_t));
  LOG_INFO(
      "Thread ID: %zuproxy_id start %d tid %zu nproxy_threads %zu c.proxy_id %d\n",
      tid, proxy_id_start, tid, FLAGS_nproxy_threads, c.proxy_id);

  if (DEBUG_SEQNUMS) {
    char fname[100];
    snprintf(fname, 100, "seqnums-%s-%zu.txt", FLAGS_my_ip.c_str(), tid);
    c.fp = fopen(fname, "w+");
    erpc::rt_assert(c.fp != NULL, "Couldn't open fd for seqnums!");
  } else if (PLOT_RECOVERY || CHECK_FIFO) {
    char fname[100];
    snprintf(fname,
             100,
             "seqnums-%s-%zu.receivedSequenceNumbers",
             FLAGS_my_ip.c_str(),
             tid);
    c.fp = fopen(fname, "w+");
    erpc::rt_assert(c.fp != NULL, "Couldn't open fd for recovery!");
  }

  size_t remote_tid = tid % FLAGS_nproxy_threads;

  // Create RPC endpoint
  LOG_INFO("Creating RPC endpoint for tid %zu, r_tid %zu proxy_id %d...\n",
           tid, remote_tid, c.proxy_id);

  erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c),
                                  static_cast<uint8_t>(tid),
                                  sm_handler, 0);
  rpc.retry_connect_on_invalid_rpc_id = true;
  c.rpc = &rpc;
  LOG_INFO("Created RPC endpoint for tid %zu, proxy_id %d...\n",
           tid, c.proxy_id);

  establish_sequencer_connection(&c, tid % N_SEQTHREADS, FLAGS_seq_ip);
  establish_corfu_connections(&c);

  c.allocate_ops();
  LOG_INFO("Thread %zu: Finished allocating mbufs\n", tid);

  // Enqueue FLAGS_concurrency requests in parallel...
  for (size_t i = 0; i < FLAGS_concurrency; i++) {
    c.ops[i]->reset(c.reqid_counter++);

    if (FLAGS_max_log_position) {//unlikely(i==0 && c.thread_id == 0))
      LOG_INFO("starting sending read request %lu\n", i);
      send_read_request(&c, c.ops[i], true);
    } else {
      LOG_INFO("starting sending append request %lu\n", i);
      send_request(&c, c.ops[i], true);
    }
  }

  // The main loop!
  LOG_INFO("\nStarting main loop in thread %zu...\n", tid);

  Timer timer;
  timer.init(kWarmup*SEC_TIMER_US, nullptr, nullptr);
  c.state = State::warmup;
  timer.start();

  Timer ps_timer; ps_timer.init(SEC_TIMER_US, nullptr, nullptr);

  while (true) {
    // change state if necessary
    if (timer.expired_reset()) {
      if (c.state == State::warmup) {
        LOG_INFO("Thread %zu done warmup starting main experiment\n",
                 c.thread_id);
        // change state to main body
        c.state = State::experiment;
        ps_timer.start();
        timer.change_period(FLAGS_expduration * SEC_TIMER_US);
        timer.start();
        // stats start time
        clock_gettime(CLOCK_REALTIME, &c.tput_t0);
      } else if (c.state == State::experiment) {
        LOG_INFO("Thread %zu done main experiment starting cooldown\n",
                 c.thread_id);
        // change state to cooldown
        c.state = State::cooldown;
        timer.change_period(kWarmup * SEC_TIMER_US);
        timer.start();
      } else {
        break;
      }
    }

    if (c.state == State::experiment) {
      if (ps_timer.expired_reset()) {
        c.print_stats();
        // wait until after warmup to detect death...
        LOG_INFO("Thread ID: %zu setting received responses to false...\n", tid);
        for (size_t k = 0; k < FLAGS_concurrency; k++) {
          c.ops[k]->received_response = false;
        }
      }
    }
    run_event_loop_us(c.rpc, 1000);
  }

  LOG_INFO(
      "[%s] Thread %zu experiment is over, no longer submitting requests...\n",
      erpc::get_formatted_time().c_str(),
      c.thread_id);

  experiment_over = true;

  if (tid == 0) {
    LOG_INFO("Thread 0: Cleaning up...\n");

    char hostname[256];
    int result;
    result = gethostname(hostname, 256);
    erpc::rt_assert(!result, "Couldn't resolve hostname");
    std::string hname(hostname, hostname + strlen(hostname));

    char fname[100];
    std::vector<std::string> strs;
    boost::split(strs, hname, boost::is_any_of("."));

    std::vector<std::string> tmp;
    boost::split(tmp, strs[0], boost::is_any_of("-"));
    sprintf(fname, "%s/%s", FLAGS_out_dir.c_str(), FLAGS_results_file.c_str());

    LOG_INFO("Thread 0 writing results to file: %s\n", fname);

    FILE *fp;
    fp = fopen(fname, "w+");
    erpc::rt_assert(fp != NULL, "fp was NULL\n");

    fwrite(c.stats, sizeof(char), strlen(c.stats), fp);

    fprintf(fp, "\n");
    fclose(fp);

    LOG_INFO("Thread 0 is done dumping results to file!\n");
  }

  if (DEBUG_SEQNUMS || PLOT_RECOVERY || CHECK_FIFO) {
    fclose(c.fp);
  }

  LOG_INFO("Exiting thread %zu\n", tid);
}

// Launch proxy worker threads with configured numbers of leaders/followers
void
client_launch_threads(size_t nthreads, erpc::Nexus *nexus) {
  // Spin up the requisite number of sequencer threads
  auto *app_stats = new app_stats_t[nthreads];
  std::vector<std::thread> threads(nthreads);

  // each client thread needs the ips of 3 proxy machine

  for (size_t i = 0; i < nthreads; i++) {
    LOG_INFO("Setting up thread %zu...\n", i);

    LOG_INFO("threads size: %zu\n", threads.size());

    // FLAGS_proxy_id is the machine raft id of the proxy it is connecting to
    threads[i] =
        std::thread(client_thread_func, i, nexus, app_stats, FLAGS_proxy_id);
    erpc::bind_to_core(threads[i],
                       nthreads > 8 ? i % 2 : numa_node,
                       nthreads > 8 ? i / 2 : i);
  }

  for (size_t i = 0; i < nthreads; i++) {
    threads[i].join();
    LOG_INFO("Joined thread %zu\n", i);
  }
  delete[] app_stats;
}

static void
signal_handler(int signum) {
  int ret __attribute__((unused));
  if (signum == SIGINT || signum == SIGTERM) {
    char msg[] = "Signal received, preparing to exit...\n";
    ret = write(STDERR_FILENO, msg, sizeof(msg));
    fflush(stdout);
  }
  if (signum == SIGSEGV || signum == SIGBUS) {
    void *array[10];
    int size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    // print out all the frames to stderr
    char msg[] = "SEGFAULT\n";
    ret = write(STDERR_FILENO, msg, sizeof(msg));
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    fflush(stdout);
  }
  if (signum == SIGABRT) {
    void *array[10];
    int size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    // print out all the frames to stderr
    char msg[] = "SEGABRT\n";
    ret = write(STDERR_FILENO, msg, sizeof(msg));
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    fflush(stdout);
  }
  erpc::rt_assert(false, "");
}

int
main(int argc, char **argv) {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  offset = t.tv_sec * 1000000 + t.tv_nsec / 1000;

  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGSEGV, signal_handler);
  signal(SIGBUS, signal_handler);
  signal(SIGABRT, signal_handler);

  LOG_INFO("Parsing command line args...\n");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  erpc::rt_assert(FLAGS_concurrency <= MAX_CONCURRENCY,
                  "Concurrency is too large!");
  erpc::rt_assert(FLAGS_nproxy_leaders > 0,
                  "Must specify nproxy_leaders");

  erpc::rt_assert(FLAGS_nproxy_threads > 0,
                  "Must specify nproxy_threads");

  my_ip = FLAGS_my_ip;

  // Make sure we have enough ring buffers; shouldn't really be a
  // problem for the clients
//    size_t num_sessions = FLAGS_nthreads;
//    erpc::rt_assert(num_sessions * erpc::kSessionCredits <=
//           erpc::Transport::kNumRxRingEntries,
//          "Too few ring buffers");

  LOG_INFO("Passed check for Too few ring buffers.\n");

  if (FLAGS_results_file.empty())
    erpc::rt_assert(false, "Must specify a results file\n");

  std::string uri = my_ip + ":31850";
  LOG_INFO("Creating nexus object for URI %s...\n", uri.c_str());
  erpc::Nexus nexus(uri, numa_node, 0);

  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kRecordNoopSeqnum),
      noop_handler);

  // Connections are per nexus (machine).
  LOG_INFO("kNumRxRingEntries %zu kSessionCredits %zu max connections %zu\n",
           erpc::Transport::kNumRxRingEntries, erpc::kSessionCredits,
           erpc::Transport::kNumRxRingEntries/erpc::kSessionCredits);

  LOG_INFO("Launching threads...\n");
  client_launch_threads(FLAGS_nthreads, &nexus);

  LOG_INFO("Exiting!\n");
  return 0;
}
