#include "client.h"

std::string my_ip;
std::string proxy_ip;
uint32_t offset;  // For recovery

static constexpr uint8_t kWarmup = 4;
// Precision factor for latency
static constexpr double kAppLatFac = 2.0;
volatile bool experiment_over = false;
size_t numa_node = 0;

static void op_cont_func(void *, void *);
std::set<uint8_t> get_shards(ClientContext *);
void set_shards(std::set<uint8_t> &, uint8_t*);

uint64_t nsequence_spaces;
double create_percent;
double rename_percent;
double write_percent;
double read_percent;
double delete_percent;
double exists_percent;
double get_children_percent;

// Handle connections initiated by us
void
sm_handler(int session_num __attribute__((unused)),
           erpc::SmEventType sm_event_type,
           erpc::SmErrType sm_err_type, void *_context) {
  auto *c = static_cast<ClientContext *>(_context);

  LOG_INFO("Thread_id %zu: ", c->thread_id);

  erpc::rt_assert(
      sm_err_type == erpc::SmErrType::kNoError,
      "Got a SM error: " + erpc::sm_err_type_str(sm_err_type));

  if (!(sm_event_type == erpc::SmEventType::kConnected ||
      sm_event_type == erpc::SmEventType::kDisconnected)) {
    throw std::runtime_error("Unexpected SM event!");
  }

  if (sm_event_type == erpc::SmEventType::kConnected) {
    c->nconnections++;
    LOG_INFO("Got a connection, session %d! nconnections %zu\n",
             session_num, c->nconnections);
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
noop_handler(erpc::ReqHandle *, void *) {
  erpc::rt_assert(false, "should not receive noops\n");
}

// A way to record sequence numbers + timestamps for noops
void
watch_notification_handler(erpc::ReqHandle *req_handle, void *_c) {
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(watch_notification_t),
                  "Watch notification wrong size\n");
  auto *c = reinterpret_cast<ClientContext *>(_c);
  c->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf, 1);
  c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}

void send_delete_client_connection(ClientContext *, Operation *, Tag *);
void send_create_node(ClientContext *c, Operation *op, Tag *tag,
                      CreateNodeOptions options = CreateNodeOptions::kPersistent) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  // choose a random path_name
  size_t pidx = static_cast<size_t>(random()) % c->path_names.size();
  std::string path(c->path_names[pidx]);
  for (size_t j = 9; j < 16; j++) {
    path[j] = random() % 26 + 'A' + random() % 2 * 32;
  }
  // changing path name was here
  op->reqtype = ReqType::kCreateZNode;

  // set up payload
  payload->reqtype = ReqType::kCreateZNode;
  payload->zk_payload.create_node.client_id = c->client_id;
  strcpy(payload->zk_payload.create_node.name, path.c_str());
  payload->zk_payload.create_node.client_connection_id =
      c->client_connection_counter;

  // limit the number of concurrent ephemeral nodes
  if (false){//c->current_ephemeral_nodes.size() < 7) {
    LOG_CLIENT("Creating ephemeral node %s\n", path.c_str());
    c->current_ephemeral_nodes.push_back(path);
    options = CreateNodeOptions::kEphemeral; // todo remove and figure out final

    // active connection needs to tell every shard we have an eph node with
    auto shards = get_shards(c);
    payload->zk_payload.create_node.nshards = shards.size();
    set_shards(shards, payload->zk_payload.create_node.shards);
  } else {
    // only change it if it is not ephemeral, ephemeral store in curr_eph_nodes
    // this loses the old node? add the path to path_names instead?
    c->path_names[pidx] = path; // store new path locally
    options = CreateNodeOptions::kPersistent;
  }

  payload->zk_payload.create_node.options = options;
  *reinterpret_cast<uint64_t *>(payload->zk_payload.create_node.data) = 1337;

  // add as ephemeral node. this should probably be in the response?

  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}

void send_delete_client_connection(ClientContext *c, Operation *op, Tag *tag) {
  // client metadata is already setup
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  LOG_CLIENT("%s: sending delete_client_connection %zu\n", c->str().c_str(),
             c->client_connection_counter);

  op->reqtype = ReqType::kDeleteClientConnection;

  payload->reqtype = ReqType::kDeleteClientConnection;
  payload->zk_payload.delete_client_connection.client_connection_id = c->client_connection_counter;
  payload->zk_payload.delete_client_connection.client_id = c->client_id;

  std::set<uint8_t> shards = get_shards(c);
  payload->zk_payload.delete_client_connection.nshards = shards.size();
  set_shards(shards, payload->zk_payload.delete_client_connection.shards);

  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));

}

void send_write_node(ClientContext *c, Operation *op, Tag *tag) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  // choose the path to send the write to
  size_t pidx = static_cast<size_t>(random()) % c->path_names.size();
  std::string path(c->path_names[pidx]);

  // setup local state
  op->reqtype = ReqType::kWriteZNode;

  // setup payload
  payload->reqtype = ReqType::kWriteZNode;
  payload->zk_payload.write_node.version = -1;
  strcpy(payload->zk_payload.write_node.name, path.c_str());
  *reinterpret_cast<uint64_t *>(payload->zk_payload.write_node.data) = 1338;
  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}

void send_read_node(ClientContext *c, Operation *op, Tag *tag) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  // choose the path to send the read to
  std::string &path(c->get_random_path());

  // setup local state
  op->reqtype = ReqType::kReadZNode;

  // setup payload
  payload->reqtype = ReqType::kReadZNode;
  strcpy(payload->zk_payload.read_node.name, path.c_str());
  payload->zk_payload.read_node.watch = false;
  payload->zk_payload.read_node.client_id = c->client_id;
  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}

void send_rename_node(ClientContext *c, Operation *op, Tag *tag) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  static bool first = true;
  std::string path;
  size_t pidx = static_cast<size_t>(random()) % c->path_names.size();
  if (first) {
    path = c->path_names[0];
    first = false;
  } else {
    // choose the path to send the rename to
    path = c->path_names[pidx];
    for (size_t j = 9; j < 16; j++) {
      path[j] = random() % 26 + 'A' + random() % 2 * 32;
    }
  }
  // setup local state
  op->reqtype = ReqType::kRenameZNode;

  // setup payload
  payload->reqtype = ReqType::kRenameZNode;
  strcpy(payload->zk_payload.rename_node.from, c->path_names[pidx].c_str());
  c->path_names[pidx] = path; // store new path locally
  strcpy(payload->zk_payload.rename_node.to, c->path_names[pidx].c_str());

  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}

void send_delete_node(ClientContext *c, Operation *op, Tag *tag) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  // choose the path to send the delete to
  std::string &path(c->get_random_path());

  // setup local state
  op->reqtype = ReqType::kDeleteZNode;

  // setup payload
  payload->reqtype = ReqType::kDeleteZNode;
  payload->zk_payload.delete_node.version = -1;
  strcpy(payload->zk_payload.delete_node.name, path.c_str());
  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}

void send_exists(ClientContext *c, Operation *op, Tag *tag) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  // choose the path to send the read to
  std::string &path(c->get_random_path());

  // setup local state
  op->reqtype = ReqType::kExists;

  // setup payload
  payload->reqtype = ReqType::kExists;
  strcpy(payload->zk_payload.exists.name, path.c_str());
  payload->zk_payload.exists.watch = false;
  payload->zk_payload.exists.client_id = c->client_id;
  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}

void send_get_children(ClientContext *c, Operation *op, Tag *tag) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  // choose the path to send the read to
  size_t pidx = static_cast<size_t>(random()) % c->path_names.size();
  std::string path(c->path_names[pidx]);
  path.resize(path.find_first_of('/', 1));

  // setup local state
  op->reqtype = ReqType::kGetChildren;

  // setup payload
  payload->reqtype = ReqType::kGetChildren;
  strcpy(payload->zk_payload.get_children.name, path.c_str());
  payload->zk_payload.get_children.watch = false;
  payload->zk_payload.get_children.client_id = c->client_id;

  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          op_cont_func, reinterpret_cast<void *>(tag));
}

void
resend_request(ClientContext *c, Operation *op, Tag *tag) {
  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);
  LOG_CLIENT("Resending type %u\n", static_cast<uint8_t>(payload->reqtype));
  c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                          static_cast<uint8_t>(payload->reqtype), &tag->req_msgbuf,
                          &tag->resp_msgbuf, op_cont_func, tag);
}

void
send_request(ClientContext *c, Operation *op, bool new_request) {
  int i = op->local_idx;

  // only update this if it is a new request
  if (likely(new_request)) c->req_tsc[i] = erpc::rdtsc();
  c->last_retx[i] = erpc::rdtsc();

  Tag *tag = c->tag_pool.alloc();
  tag->alloc_msgbufs(c, op);

  auto *payload = reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  payload->client_id = c->client_id;
  payload->client_reqid = op->local_reqid;
  payload->proxy_id = c->proxy_id;
  payload->highest_recvd_reqid = c->highest_cons_reqid;

  // the first request should establish root
  // todo should just do this in the system
  if (payload->client_reqid == 0) {
    LOG_INFO("Creating root.\n");
    op->reqtype = ReqType::kCreateZNode;
    payload->reqtype = ReqType::kCreateZNode;
    sprintf(payload->zk_payload.create_node.name, "/");
    payload->zk_payload.create_node.version = 0;
    payload->zk_payload.create_node.options = CreateNodeOptions::kPersistent;
    payload->zk_payload.create_node.client_id = c->client_id;
    *reinterpret_cast<uint64_t *>(payload->zk_payload.create_node.data) = 1337;

    c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                            static_cast<uint8_t>(payload->reqtype),
                            &tag->req_msgbuf, &tag->resp_msgbuf,
                            op_cont_func, reinterpret_cast<void *>(tag));
    return;
  }

  // make sure we create each path in path_names
  if (c->ncreated_pathnames < c->path_names.size()) {
    op->reqtype = ReqType::kCreateZNode;
    payload->reqtype = ReqType::kCreateZNode;
    strcpy(payload->zk_payload.create_node.name,
           c->path_names[c->ncreated_pathnames].c_str());
    payload->zk_payload.create_node.version = 0;
    *reinterpret_cast<uint64_t *>(payload->zk_payload.create_node.data) = 1337;

    LOG_INFO("Initializing znode with path %s\n",
             payload->zk_payload.create_node.name);
    c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                            static_cast<uint8_t>(ReqType::kCreateZNode),
                            &tag->req_msgbuf, &tag->resp_msgbuf,
                            op_cont_func, reinterpret_cast<void *>(tag));
    c->ncreated_pathnames++;
    return;
  }

  double x = random() / static_cast<double>(RAND_MAX);
  double sum = 0;
  if (sum < x && x <= (sum += create_percent) && create_percent) {
    send_create_node(c, op, tag);
  } else if (sum < x && x <= (sum += rename_percent) && rename_percent) {
    send_rename_node(c, op, tag);
  } else if (sum < x && x <= (sum += write_percent) && write_percent) {
    send_write_node(c, op, tag);
  } else if (sum < x && x <= (sum += read_percent) && read_percent) {
    send_read_node(c, op, tag);
  } else if (sum < x && x <= (sum += delete_percent) && delete_percent) {
    send_delete_node(c, op, tag);
  } else if (sum < x && x <= (sum += exists_percent) && exists_percent) {
    send_exists(c, op, tag);
  } else if (sum < x && x <= (sum += get_children_percent) && get_children_percent) {
    send_get_children(c, op, tag);
  }
  return;

  send_create_node(c, op, tag);
  return;
  erpc::rt_assert(false, "impossible");

  if (x > rename_percent) {
    payload->reqtype = ReqType::kCreateZNode;
    // choose a random path_name
    size_t pidx = static_cast<size_t>(random()) % c->path_names.size();
    std::string path(c->path_names[pidx]);
    // choose new name
//    for (size_t j = 9; j < 16; j++) {
//      path[j] = random() % 26 + 'A' + random() % 2 * 32;
//    }
//    c->path_names[pidx] = path;
//    strcpy(payload->zk_payload.create_node.name, path.c_str());
//    *reinterpret_cast<uint64_t *>(payload->zk_payload.create_node.data) =
//        1337;
//
//    c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
//                            static_cast<uint8_t>(ReqType::kCreateZNode),
//                            &tag->req_msgbuf, &tag->resp_msgbuf,
//                            op_cont_func, reinterpret_cast<void *>(tag));

//    // DELETE PATH
//      payload->reqtype = ReqType::kDeleteZNode;
//      op->reqtype = ReqType::kDeleteZNode;
////      payload->zk_payload.write_node.data = 1338;
//
//      strcpy(payload->zk_payload.delete_node.name, path.c_str());
////      *reinterpret_cast<uint64_t *>(payload->zk_payload.write_node.data) = 1338;
////      LOG_INFO("About to submit delete for path %s data\n",
////               payload->zk_payload.delete_node.name);
//      c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
//                              static_cast<uint8_t>(ReqType::kDeleteZNode),
//                              &tag->req_msgbuf, &tag->resp_msgbuf,
//                              op_cont_func, reinterpret_cast<void *>(tag));

    // WRITE PATH
    payload->reqtype = ReqType::kWriteZNode;

    payload->zk_payload.write_node.version = -1;
    op->reqtype = ReqType::kWriteZNode;
    strcpy(payload->zk_payload.write_node.name, path.c_str());
    *reinterpret_cast<uint64_t *>(payload->zk_payload.write_node.data) = 1338;
    c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                            static_cast<uint8_t>(ReqType::kWriteZNode),
                            &tag->req_msgbuf, &tag->resp_msgbuf,
                            op_cont_func, reinterpret_cast<void *>(tag));
    return;
  } else { // rename path
    payload->reqtype = ReqType::kRenameZNode;

    // pick path
    size_t pidx = static_cast<size_t>(random()) % c->path_names.size();
    std::string path(c->path_names[pidx]);
    printf("for rename path is %s\n", path.c_str());
    if (1) { // todo improve testing
      // READ PATH
      payload->reqtype = ReqType::kReadZNode;
      op->reqtype = ReqType::kReadZNode;

      strcpy(payload->zk_payload.read_node.name, path.c_str());
      c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                              static_cast<uint8_t>(ReqType::kReadZNode),
                              &tag->req_msgbuf, &tag->resp_msgbuf,
                              op_cont_func, reinterpret_cast<void *>(tag));

      // DELETE PATH
//      payload->reqtype = ReqType::kDeleteZNode;
//      op->reqtype = ReqType::kDeleteZNode;
////      payload->zk_payload.write_node.data = 1338;
//
//      strcpy(payload->zk_payload.delete_node.name, path.c_str());
////      *reinterpret_cast<uint64_t *>(payload->zk_payload.write_node.data) = 1338;
////      LOG_INFO("About to submit delete for path %s data\n",
////               payload->zk_payload.delete_node.name);
//      c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
//                              static_cast<uint8_t>(ReqType::kDeleteZNode),
//                              &tag->req_msgbuf, &tag->resp_msgbuf,
//                              op_cont_func, reinterpret_cast<void *>(tag));
    } else {
      // randomize next name
      for (size_t j = 9; j < 16; j++) {
        path[j] = random() % 26 + 'A' + random() % 2 * 32;
      }
      strcpy(payload->zk_payload.rename_node.from, c->path_names[pidx].c_str());
      printf("Trying to rename from %s to %s\n",
             payload->zk_payload.rename_node.from, path.c_str());
      c->path_names[pidx] = path;
      strcpy(payload->zk_payload.rename_node.to, c->path_names[pidx].c_str());

      c->rpc->enqueue_request(c->sessions[op->cur_px_conn],
                              static_cast<uint8_t>(ReqType::kRenameZNode),
                              &tag->req_msgbuf, &tag->resp_msgbuf,
                              op_cont_func, reinterpret_cast<void *>(tag));
    }
    return;
  }
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
      resend_request(c, op, tag);
    }
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

  // good response, record req_id and update highest_cons_reqid
  c->push_and_update_highest_cons_req_id(payload->client_reqid);

  if (unlikely(payload->client_reqid % 10000== 0)) {
    LOG_INFO(
        "Thread %zu: Got seqnums for local_reqid %zu, idx %zu "
        "from node %zu latency %f\n",
        c->thread_id, payload->client_reqid, i, op->cur_px_conn,
        erpc::to_usec(tsc_now - c->req_tsc[i], c->rpc->get_freq_ghz()));
    LOG_INFO("Thread %zu: highest_cons_req_id %zu\n",
             c->thread_id, c->highest_cons_reqid);
  }

  // Create the new op
  if (likely(!force_quit && !experiment_over)) {
    // Record latency
    if (likely(c->state == State::experiment)) {
      size_t req_tsc = c->req_tsc[i];
      double req_lat_us =
          erpc::to_usec(tsc_now - req_tsc, c->rpc->get_freq_ghz());
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

void ping_cont_func(void *_c, void *_t) {
  auto *c = reinterpret_cast<ClientContext *>(_c);
  auto *tag = reinterpret_cast<client_heartbeat_tag *>(_t);
  client_heartbeat_resp_t *chresp = reinterpret_cast<client_heartbeat_resp_t *>(
      tag->resp_mbuf.buf);
  LOG_CLIENT("Got client_heartbeat_resp deleted %d connection num %zu\n",
             chresp->deleted, chresp->client_connection_number);

  if (chresp->deleted &&
      chresp->client_connection_number == c->client_connection_counter) {
    LOG_CLIENT("Actually deleting connection: deleted %d connection num %zu\n",
               chresp->deleted, chresp->client_connection_number);
    c->delete_connection();
  }

  c->rpc->free_msg_buffer(tag->req_mbuf);
  c->rpc->free_msg_buffer(tag->resp_mbuf);
}

std::set<uint8_t> get_shards(ClientContext *c) {
  std::set<uint8_t> shards;
  for (std::string path : c->current_ephemeral_nodes) {
    shards.insert(znode_to_shard_idx(path, 3));
  }
  return shards;
}
// set_shards defined in common.h
void ClientContext::ping_connections() {
  if (current_ephemeral_nodes.empty()) return;

  // can probably just maintain this
  std::set<uint8_t> shards = get_shards(this);

  auto *tag = new client_heartbeat_tag_t;
  tag->req_mbuf = rpc->alloc_msg_buffer_or_die(
      sizeof_client_heartbeat(shards.size()));
  tag->resp_mbuf = rpc->alloc_msg_buffer_or_die(sizeof(client_heartbeat_resp_t));

  auto *ch = reinterpret_cast<client_heartbeat_t *>(tag->req_mbuf.buf);
  ch->client_id = client_id;
  ch->proxy_id = proxy_id;
  ch->nshards = shards.size();
  ch->client_connection_number = client_connection_counter;
  set_shards(shards, ch->shards);
  LOG_CLIENT("sending heartbeat to proxy_id %u\n", ch->proxy_id);

  rpc->enqueue_request(ops[0]->cur_px_conn,
                       static_cast<uint8_t>(ReqType::kClientHeartbeat),
                       &tag->req_mbuf, &tag->resp_mbuf, ping_cont_func,
                       tag);
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

  // creates 128 random A-Z paths of length 16 (including /)
  for (size_t i = 0; i < 8; i++) {
    std::string path;
    path.push_back('/');
    for (size_t j = 0; j < 7; j++) {
      path.push_back(random() % 26 + 'A' + random() % 2 * 32);
    }
    path.push_back('/');
    for (size_t j = 0; j < 7; j++) {
      path.push_back(random() % 26 + 'A' + random() % 2 * 32);
    }
    c.path_names.push_back(path);
  }

  LOG_INFO("Thread %zu client_id is %u\n", c.thread_id, c.client_id);

  LOG_INFO(
      "Thread ID: %zu proxy_id start %d tid %zu nproxy_threads %zu c.proxy_id %d\n",
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

  // The main loop!
  LOG_INFO("Starting main loop in thread %zu...\n", tid);
  clock_gettime(CLOCK_REALTIME, &c.tput_t0);

  Timer timer;
  timer.init(kWarmup*SEC_TIMER_US, nullptr, nullptr);
  c.state = State::warmup;
  timer.start();
  Timer ps_timer; ps_timer.init(SEC_TIMER_US, nullptr, nullptr);

  Timer ping_timer;
  ping_timer.init(SEC_TIMER_US, nullptr, nullptr);
  ping_timer.start();

  while (true) {
    // change state if necessary todo make function
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
    // check print stats timer and react accordingly
    if (c.state == State::experiment) {
      if (ps_timer.expired_reset()) {
        c.print_stats();
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

    if (ping_timer.expired_reset()) {
      c.ping_connections();
    }

    run_event_loop_us(c.rpc, 1000);

    // if this op didn't receive a response in the last second
    // the proxy is probably dead... we want a higher granularity
    if (c.state == State::experiment){
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
//      LOG_INFO("Thread ID: %zu in warmup or cooldown\n", c.thread_id);
    }
  }

  LOG_INFO(
      "[%s] Thread %zu experiment is over, no longer submitting requests...\n",
      erpc::get_formatted_time().c_str(), c.thread_id);

  experiment_over = true;

  if (tid == 0) {
    LOG_INFO("Thread 0: Cleaning up...\n");

    char hostname[256];
    int result;
    result = gethostname(hostname, 256);
    erpc::rt_assert(!result, "Couldn't resolve hostname");
    std::string hname(hostname, hostname + strlen(hostname));

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

  srandom(time(NULL));

  // Op percentages
  create_percent = FLAGS_create_percent;
  rename_percent = FLAGS_rename_percent;
  write_percent = FLAGS_write_percent;
  read_percent = FLAGS_read_percent;
  delete_percent = FLAGS_delete_percent;
  exists_percent = FLAGS_exists_percent;
  get_children_percent = FLAGS_get_children_percent;

  nzk_shards = FLAGS_nzk_servers/kZKReplicationFactor;
  LOG_INFO("cp %f renp %f wp %f reap %f dp %f ep %f gcp %f \n",
           create_percent, rename_percent, write_percent, read_percent,
           delete_percent, exists_percent, get_children_percent);
  LOG_INFO("Client connection timeout is %zu\n", kClientConnectionTimeout);

  double sum = create_percent + rename_percent + write_percent + read_percent
      + delete_percent + exists_percent + get_children_percent;
  fmt_rt_assert(std::abs(sum - 1.0) < .0000000001,
                "Op percents don't add to 1 got %f\n", sum);

  erpc::rt_assert(FLAGS_nsequence_spaces > 0,
                  "Can not have 0 sequence spaces\n");
  erpc::rt_assert(FLAGS_concurrency <= MAX_CONCURRENCY,
                  "Concurrency is too large!");
  erpc::rt_assert(FLAGS_nproxy_leaders > 0,
                  "Must specify nproxy_leaders");
  erpc::rt_assert(FLAGS_nproxy_threads > 0,
                  "Must specify nproxy_threads");
  erpc::rt_assert(FLAGS_rename_percent >= 0 &&
                      FLAGS_rename_percent <= 1,
                  "rename percent must be between 0 and 1");

  my_ip = FLAGS_my_ip;

  std::string uri = my_ip + ":31850";
  LOG_INFO("Creating nexus object for URI %s...\n", uri.c_str());
  erpc::Nexus nexus(uri, numa_node, 0);

  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kRecordNoopSeqnum),
      noop_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kWatchNotification),
      watch_notification_handler);

  // Connections are per nexus (machine).
  LOG_INFO("kNumRxRingEntries %zu kSessionCredits %zu max connections %zu\n",
           erpc::Transport::kNumRxRingEntries, erpc::kSessionCredits,
           erpc::Transport::kNumRxRingEntries/erpc::kSessionCredits);

  LOG_INFO("Launching threads...\n");
  client_launch_threads(FLAGS_nthreads, &nexus);

  LOG_INFO("Exiting!\n");
  return 0;
}