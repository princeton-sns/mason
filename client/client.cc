#include "client.h"

std::string my_ip;
std::string proxy_ip;
uint32_t offset;  // For recovery

static constexpr uint8_t kWarmup = 4;
static constexpr double kAppLatFac = 3.0;        // Precision factor for latency
volatile bool experiment_over = false;
size_t numa_node = 0;

static void op_cont_func(void *, void *);


uint64_t nsequence_spaces;


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

  fmt_rt_assert(req_handle->get_req_msgbuf()->get_data_size() ==
                client_payload_size(nsequence_spaces),
                "noop message not the right size should be %zu is %zu\n",
                client_payload_size(nsequence_spaces),
                req_handle->get_req_msgbuf()->get_data_size());

  auto *payload = reinterpret_cast<client_payload_t *>(
      req_msgbuf->buf);
  erpc::rt_assert(payload->reqtype == ReqType::kNoop,
                  "Got a non-noop request type in the handler!\n");

  if (DEBUG_SEQNUMS) {
    size_t batch_size = 0;
    for (size_t j = 0; j < nsequence_spaces; j++) {
      batch_size = payload->seq_reqs[0].batch_size;
      // they should all have the same batch size for now...
      erpc::rt_assert(batch_size == payload->seq_reqs[j].batch_size);
    }

    std::string lines;
    for (size_t j = 0; j < batch_size; j++) {
      lines += get_formatted_time_from_offset(offset);
      for (size_t k = 0; k < nsequence_spaces; k++) {
        lines += " " + std::to_string(payload->seq_reqs[k].seqnum);
      }
      lines += " \n";
    }
    fprintf(c->fp, "%s", lines.c_str());
  } else if (PLOT_RECOVERY) {
    print_seqreqs(payload->seq_reqs, nsequence_spaces);
    std::string time = get_formatted_time_from_offset(offset);
    std::string line;
    seq_req_t *sr = payload->seq_reqs;
    size_t max_bs = 0;
    for (size_t i = 0; i < nsequence_spaces; i++) {
      max_bs = max_bs < sr[i].batch_size ? sr[i].batch_size : max_bs;
    }

    while (max_bs > 0) {
      line += time;
      for (size_t i = 0; i < nsequence_spaces; i++) {
        line += " ";
        if (sr[i].batch_size > 0) {
          line += std::to_string(sr[i].seqnum);
          sr[i].seqnum--;
          sr[i].batch_size--;
        } else {
          line += "0";
        }
      }
      line += "\n";
      max_bs--;
    }
    LOG_ERROR("cid %d got noops: %s\n", c->client_id, line.c_str());
    fprintf(c->fp, "%s", line.c_str());
  }

  c->rpc->resize_msg_buffer(
      &req_handle->pre_resp_msgbuf, 1);
  c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}


void
send_request(ClientContext *c, Operation *op, bool new_request) {
  int i = op->local_idx;

  // only update this if it is a new request
  if (likely(new_request)) c->req_tsc[i] = erpc::rdtsc();
  c->last_retx[i] = erpc::rdtsc();

  Tag *tag = c->tag_pool.alloc();
  tag->alloc_msgbufs(c, op);

  auto *payload =
      reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  payload->client_id = c->client_id;
  payload->client_reqid = op->local_reqid;
  payload->proxy_id = c->proxy_id;
  payload->highest_recvd_reqid = c->highest_cons_reqid;

  // for now just ask for one number from each sequence space
  for (size_t j = 0; j < nsequence_spaces; j++) {
    payload->seq_reqs[j].seqnum = 0;
    payload->seq_reqs[j].batch_size = 1;
  }

  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(ReqType::kExecuteOpA),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}


void
op_cont_func(void *_context, void *_tag) {
  auto tsc_now = erpc::rdtsc();

  auto tag = reinterpret_cast<Tag *>(_tag);
  Operation *op = tag->op;
  auto c = static_cast<ClientContext *>(_context);

  size_t i = op->local_idx;

  // this is a failure-with-continuation
  if (unlikely(tag->resp_msgbuf.get_data_size() == 0)) {
    erpc::rt_assert(false, "Client Failure with continuation!\n");
  }

  auto *payload =
      reinterpret_cast<client_payload_t *>(tag->resp_msgbuf.buf);

  op->received_response = true;

  // If the proxy we sent this request to is no longer the leader:
  //   increment cur_px_conn (%3) if not already done so. And try the next proxy.
  if (unlikely(payload->not_leader)) {
    LOG_ERROR("[%zu] op: lid %zu, cid %zu response not leader from %zu\n",
                 c->thread_id, op->local_reqid,
                 op->local_idx, op->cur_px_conn);

    c->next_proxy(op);

    if (!force_quit) {
      send_request(c, op, false);
    }
    c->tag_pool.free(tag);
    return;
  }

  // If mismatch with client_reqid > local_reqid, then we die!
  // It's OK if client_reqid < local_reqid, because this could be an app-level retransmit from a
  // proxy. We are potentially repeatedly sending requests to proxies for the same reqid, so
  // it is possible they can be satisfied twice.
  fmt_rt_assert(payload->client_reqid <= op->local_reqid,
                "[%s] Thread %zu: Mismatch: op: %zu received reqid %lu, "
                "expected %lu op %p tag %p\n",
                erpc::get_formatted_time().c_str(),
                c->thread_id, op->local_idx,
                payload->client_reqid, op->local_reqid, tag->op, tag);

  // ...but we should ignore the retransmit if we've seen this seqnum already
  if (payload->client_reqid < op->local_reqid) {
    return;
  }

  if (DEBUG_SEQNUMS) {
    size_t batch_size = 0;
    for (size_t j = 0; j < nsequence_spaces; j++) {
      batch_size = payload->seq_reqs[0].batch_size;
      // they should all have the same batch size for now...
      erpc::rt_assert(batch_size == payload->seq_reqs[j].batch_size);
    }

    std::string lines;
    for (size_t j = 0; j < batch_size; j++) {
      lines += get_formatted_time_from_offset(offset);
      for (size_t k = 0; k < nsequence_spaces; k++) {
        lines += " " + std::to_string(payload->seq_reqs[k].seqnum);
      }
      lines += " \n";
    }
    fprintf(c->fp, "%s", lines.c_str());
  } else if (PLOT_RECOVERY) {
    std::string time = get_formatted_time_from_offset(offset);
    std::string line;
    seq_req_t *sr = payload->seq_reqs;
    size_t max_bs = 0;
    for (size_t j = 0; j < nsequence_spaces; j++) {
      max_bs = max_bs < sr[j].batch_size ? sr[j].batch_size : max_bs;
    }

    while (max_bs > 0) {
      line += time;
      for (size_t j = 0; j < nsequence_spaces; j++) {
        line += " ";
        if (sr[j].batch_size > 0) {
          line += std::to_string(sr[j].seqnum);
          sr[j].seqnum--;
          sr[j].batch_size--;
        } else {
          line += "0";
        }
      }
      line += "\n";
      max_bs--;
    }
    fprintf(c->fp, "%s", line.c_str());
  }

  // good response, record req_id and update highest_cons_reqid
  c->push_and_update_highest_cons_req_id(payload->client_reqid);

  if (unlikely(payload->seq_reqs[0].seqnum % 1000000 == 0)) {
    LOG_INFO("Thread %zu: Got seqnums for local_reqid %zu, "
             "idx %zu from node %zu latency %f\n",
        c->thread_id, payload->client_reqid, i, op->cur_px_conn,
        erpc::to_usec(tsc_now - c->req_tsc[i], c->rpc->get_freq_ghz()));
    print_seqreqs(payload->seq_reqs, nsequence_spaces);
    LOG_INFO("Thread %zu: highest_cons_req_id %zu\n", c->thread_id,
             c->highest_cons_reqid);
  }

  // Create the new op
  if (likely(!force_quit && !experiment_over)) {
    // Record latency info
    if (likely(c->state == State::experiment)) {
      size_t req_tsc = c->req_tsc[i];
      double req_lat_us = erpc::to_usec(tsc_now - req_tsc,
                                        c->rpc->get_freq_ghz());
      c->latency.update(static_cast<size_t>(req_lat_us * kAppLatFac));
      c->stat_resp_rx_tot++;
    }

    c->ops[i]->reset(c->reqid_counter++);
    send_request(c, op, true);
  } else if (unlikely(experiment_over)) {
    c->completed_slots++;
  }
  c->tag_pool.free(tag);
}


// Initialize state needed for client operation
void
create_client(ClientContext *c) {
  (void) c;
}


void
establish_proxy_connection(ClientContext *c, int remote_tid,
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
 * proxy_id: the logical proxy (replica group) id this client thread
 *   should submit requests to
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

  LOG_INFO("Thread %zu client_id is %u\n", c.thread_id, c.client_id);

  LOG_INFO(
      "Thread ID: %zu proxy_id start %d "
      "tid %zu "
      "nproxy_threads %zu "
      "c.proxy_id %d\n",
      tid, proxy_id_start, tid, FLAGS_nproxy_threads, c.proxy_id);
  LOG_INFO("Thread ID: %zu starting with %zu sequence spaces\n",
           c.thread_id, nsequence_spaces);

  if (DEBUG_SEQNUMS) {
    char fname[100];
    snprintf(fname, 100, "seqnums-%s-%zu.txt", FLAGS_my_ip.c_str(), tid);
    c.fp = fopen(fname, "w+");
    erpc::rt_assert(c.fp != NULL, "Couldn't open fd for seqnums!");
  } else if (PLOT_RECOVERY || CHECK_FIFO) {
    char fname[100];
    snprintf(fname, 100, "seqnums-%s-%zu.receivedSequenceNumbers",
             FLAGS_my_ip.c_str(), tid);
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

  // Establish connections to the proxies in our group.
  // Assumes they are all alive when this thread starts.
  establish_proxy_connection(&c, remote_tid, FLAGS_proxy_ip_0);
  establish_proxy_connection(&c, remote_tid, FLAGS_proxy_ip_1);
  establish_proxy_connection(&c, remote_tid, FLAGS_proxy_ip_2);

  c.allocate_ops();
  LOG_INFO("Thread %zu: Finished allocating mbufs\n", tid);

  // Enqueue a bunch of requests...
  for (size_t i = 0; i < FLAGS_concurrency; i++) {
    c.ops[i]->reset(c.reqid_counter++);
    LOG_INFO("starting sending request %lu\n", i);
    send_request(&c, c.ops[i], true);
  }

  LOG_INFO("Starting main loop in thread %zu...\n", tid);

  // Add some time for warmup/cooldown
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
        // sync to disk ~once per second, prevent clients pausing for a long time
        // to sync to disk.
        if (PLOT_RECOVERY) {
          fsync(fileno(c.fp));
        }
      }
    }

    run_event_loop_us(c.rpc, 1000);

    if (c.state == State::experiment) {
      size_t k = 0;
      double now = erpc::to_sec(erpc::rdtsc(), c.rpc->get_freq_ghz());
      for (k = 0; k < FLAGS_concurrency; k++) {
        if (!c.ops[k]->received_response &&
            (now - erpc::to_sec(c.last_retx[k], c.rpc->get_freq_ghz())) >
            failover_to) {
          if (k == 0) {
            LOG_INFO("Thread ID: %zu DECLARING PROXY %zu DEAD! "
                     "op %zu did not receive a response in the "
                     "last %lf seconds, changing proxies from %zu to %zu\n",
                     c.thread_id, c.ops[k]->cur_px_conn, c.ops[k]->local_reqid,
                     failover_to,
                     c.ops[k]->cur_px_conn,
                     (c.ops[k]->cur_px_conn + 1) % 3);
          }

          // use the next proxy and then retransmit requests with the same req_id
          c.next_proxy(c.ops[k]);
          send_request(&c, c.ops[k], false);
        }
      }
    } else {
      // in warmup/cooldown
      // LOG_INFO("Thread ID: %zu in warmup or cooldown\n", c.thread_id);
    }
  }

  LOG_INFO(
      "[%s] Thread %zu experiment is over, no longer submitting requests...\n",
      erpc::get_formatted_time().c_str(), c.thread_id);

  experiment_over = true;

  if (tid == 0) {
    LOG_INFO("Thread 0: Cleaning up...\n");

    char fname[100];
    sprintf(fname, "%s/%s", FLAGS_out_dir.c_str(), FLAGS_results_file.c_str());

    FILE *fp;
    fp = fopen(fname, "w+");
    erpc::rt_assert(fp != nullptr, "fp was NULL\n");

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
    threads[i] = std::thread(client_thread_func, i, nexus, app_stats,
                             FLAGS_proxy_id);
    erpc::bind_to_core(threads[i],
                       nthreads > 8 ? i % 2 : numa_node,
                       nthreads > 8 ? i / 2 : i);
  }

  // for (auto &thread : threads) thread.join();
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
  fmt_rt_assert(false, "Received a signal %d!", signum);
}

int
main(int argc, char **argv) {
  struct timespec t{};
  clock_gettime(CLOCK_REALTIME, &t);
  offset = t.tv_sec * 1000000 + t.tv_nsec / 1000;

  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGSEGV, signal_handler);
  signal(SIGBUS, signal_handler);
  signal(SIGABRT, signal_handler);

  LOG_INFO("Parsing command line args...\n");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  nsequence_spaces = FLAGS_nsequence_spaces;

  erpc::rt_assert(FLAGS_nsequence_spaces > 0,
                  "Can not have 0 sequence spaces\n");
  erpc::rt_assert(FLAGS_concurrency <= MAX_CONCURRENCY,
                  "Concurrency is too large!");
  erpc::rt_assert(FLAGS_nproxy_leaders > 0,
                  "Must specify nproxy_leaders");
  erpc::rt_assert(FLAGS_nproxy_threads > 0,
                  "Must specify nproxy_threads");

  my_ip = FLAGS_my_ip;

  LOG_INFO("Passed check for Too few ring buffers.\n");

  std::string uri = my_ip + ":31850";
  LOG_INFO("Creating nexus object for URI %s...\n", uri.c_str());
  erpc::Nexus nexus(uri, numa_node, 0);

  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kRecordNoopSeqnum),
      noop_handler);

  LOG_INFO("Launching threads...\n");
  client_launch_threads(FLAGS_nthreads, &nexus);

  LOG_INFO("Exiting!\n");
  return 0;
}