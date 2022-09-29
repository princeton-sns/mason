#include "server.h"

size_t numa_node = 0;
volatile bool force_quit = false;

void
sm_handler(int session_num, erpc::SmEventType sm_event_type,
           erpc::SmErrType sm_err_type, void *_context) {
  auto *c = static_cast<ZKContext *>(_context);

  LOG_INFO("session_num %d, thread_id %zu: ", session_num, c->thread_id);

  erpc::rt_assert(
      sm_err_type == erpc::SmErrType::kNoError,
      "Got a SM error: " + erpc::sm_err_type_str(sm_err_type));

  if (!(sm_event_type == erpc::SmEventType::kConnected ||
      sm_event_type == erpc::SmEventType::kDisconnected)) {
    throw std::runtime_error("Unexpected SM event!");
  }

  if (sm_event_type == erpc::SmEventType::kConnected) {
    printf("Got a connection!\n");
    c->nconnections++;
  } else {
    printf("Lost a connection!\n");
    c->nconnections--;
  }
}


// Heavily borrowed from erpc code:
// https://github.com/erpc-io/eRPC/blob/master/apps/small_rpc_tput/small_rpc_tput.cc
void
ZKContext::print_stats() {
  double seconds = erpc::sec_since(tput_t0);

  double tput_mrps = stat_resp_tx_tot / (seconds * 1000000);
  app_stats[thread_id].mrps = tput_mrps;
  app_stats[thread_id].num_re_tx = rpc->pkt_loss_stats.num_re_tx;

  if (max_received_seqnum < cur_seqnum) {
    erpc::rt_assert(max_received_seqnum+1 == cur_seqnum,
                    "cur_seqnum was more than 1 ahead of max received\n");
  }
  uint64_t lag = max_received_seqnum - cur_seqnum + 1;
  printf("[%s] Thread %zu: %.3f Mrps, re_tx = %zu, still_in_wheel = %zu. "
         "RX: %luK resps. nZNodes %lu. Size (B) %lu \n\tcurrent seq_num %zu "
         "max_received_seqnum %zu lag %zu\n",
         erpc::get_formatted_time().c_str(),
         thread_id, tput_mrps, app_stats[thread_id].num_re_tx,
         rpc->pkt_loss_stats.still_in_wheel_during_retx,
         stat_resp_tx_tot / 1000,
         znodes.size(),
         znodes.size() * sizeof(ZNode), cur_seqnum, max_received_seqnum,
         lag);

  if (thread_id == 0) {
    app_stats_t accum;
    for (size_t i = 0; i < N_SEQTHREADS; i++) {
      accum += app_stats[i];
    }

    std::string totals = accum.to_string();
  }

  stat_resp_tx_tot = 0;
  rpc->pkt_loss_stats.num_re_tx = 0;

  clock_gettime(CLOCK_REALTIME, &tput_t0);
  fflush(stdout);
}

void
create_node_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(create_node_t),
                  "create_node args wrong size");

  auto *op = reinterpret_cast<create_node_t *>(req_msgbuf->buf);

  c->set_max_received_seqnum(op->seqnum);
  auto *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op);
  respond_to_request(c, req_handle, nullptr, 0);
  c->push_and_pull_from_queue(mop);
}

void
write_node_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(write_node_t),
                  "write_node args wrong size");

  auto *op = reinterpret_cast<write_node_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  Operation *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op);

  // an ACK, ops that don't return execute asynchronously
  respond_to_request(c, req_handle, nullptr, 0);
  c->push_and_pull_from_queue(mop);
}

void
create_node_with_children_req_handler(erpc::ReqHandle *req_handle,
                                      void *_context) {
  erpc::rt_assert(false, "should not be creating node with children now\n");
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  auto *op = reinterpret_cast<create_node_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  auto *mop = c->op_pool.alloc();
  size_t bufsize = req_msgbuf->get_data_size() - sizeof(create_node_t);
  char *buf = new char[bufsize];
  memcpy(buf, req_msgbuf->buf + sizeof(create_node_t), bufsize);

  mop->set(op->seqnum, op, buf);

  respond_to_request(c, req_handle, nullptr, 0);

  c->push_and_pull_from_queue(mop);
}

void
rename_node_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(rename_node_t),
                  "rename_node args wrong size");

  auto *op = reinterpret_cast<rename_node_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  auto *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op);
  respond_to_request(c, req_handle, nullptr, 0);
  c->push_and_pull_from_queue(mop);
}

void
delete_and_read_node_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(delete_node_t),
                  "delete_node args wrong size");

  auto *op = reinterpret_cast<delete_node_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  auto *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op, req_handle);

  c->push_and_pull_from_queue(mop);
}

void
delete_node_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(delete_node_t),
                  "delete_node args wrong size");

  auto *op = reinterpret_cast<delete_node_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  Operation *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op);
  respond_to_request(c, req_handle, nullptr, 0);
  c->push_and_pull_from_queue(mop);
}

void
read_node_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(read_node_t),
                  "read_node args wrong size");

  auto *op = reinterpret_cast<read_node_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  Operation *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op, req_handle);
  c->push_and_pull_from_queue(mop);
}

void
add_child_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(add_child_t),
                  "add_child args wrong size");

  auto *op = reinterpret_cast<add_child_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  auto *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op);
  respond_to_request(c, req_handle, nullptr, 0);

  c->push_and_pull_from_queue(mop);
}

void
remove_child_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(remove_child_t),
                  "add_child args wrong size");

  auto *op = reinterpret_cast<remove_child_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  auto *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op);
  respond_to_request(c, req_handle, nullptr, 0);

  c->push_and_pull_from_queue(mop);
}

void
rename_child_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(rename_child_t),
                  "rename_child args wrong size");

  auto *op = reinterpret_cast<rename_child_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);
  auto *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op);

  respond_to_request(c, req_handle, nullptr, 0);

  c->push_and_pull_from_queue(mop);
}

void
exists_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(exists_t),
                  "exists args wrong size");

  auto *op = reinterpret_cast<exists_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  Operation *mop = c->op_pool.alloc();
  // this function can not ack immediately
  mop->set(op->seqnum, op, req_handle);

  c->push_and_pull_from_queue(mop);
}

void
get_children_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(get_children_t),
                  "get_children args wrong size");

  auto *op = reinterpret_cast<get_children_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  Operation *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op, req_handle);

  c->push_and_pull_from_queue(mop);
}

void
client_heartbeat_req_handler(erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(client_heartbeat_req_t),
                  "client_heartbeat args wrong size");

  auto *chreq = reinterpret_cast<client_heartbeat_req_t *>(req_msgbuf->buf);

  // setup response
  client_heartbeat_resp_t chresp;
  try {
    // this client connection number is old
    if (chreq->client_connection_number < c->client_connection_numbers.at(
        chreq->client_id)) {
      chresp.deleted = false;
      chresp.client_connection_number = c->client_connection_numbers.at(
          chreq->client_id);
    }
    // this server is behind the "current" client connection, reply, but do nothing
    else if (chreq->client_connection_number > c->client_connection_numbers.at(
        chreq->client_id)) {
      chresp.deleted = false;
      chresp.client_connection_number = c->client_connection_numbers.at(
          chreq->client_id);
    }
    // this is not the first connection and it's the current connection
    // try to reset it (it might still have been deleted)
    else {
      chresp.deleted = c->reset_client_timer(chreq->client_id);
      chresp.client_connection_number = c->client_connection_numbers.at(
          chreq->client_id);
    }
  } catch (std::out_of_range &) {
    // didn't have connection number,
    // this is for a connection we don't know about yet
    chresp.deleted = false;
    chresp.client_connection_number = 0;
  }

  respond_to_request(c, req_handle, &chresp, sizeof(chresp));
}

void
delete_client_connection_req_handler(
    erpc::ReqHandle *req_handle, void *_context) {
  // Get message buffer and check messagesize
  auto *c = static_cast<ZKContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();

  auto *op = reinterpret_cast<delete_client_connection_t *>(req_msgbuf->buf);
  c->set_max_received_seqnum(op->seqnum);

  auto *mop = c->op_pool.alloc();
  mop->set(op->seqnum, op, req_handle);
  c->push_and_pull_from_queue(mop);
}

void
zk_thread_func(size_t thread_id, erpc::Nexus *nexus, app_stats_t *app_stats) {
  ZKContext c;
  c.thread_id = thread_id;
  c.nconnections = 0;
  c.app_stats = app_stats;

  LOG_INFO("[%zu] creating rpc\n", thread_id);
  erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c),
                                  static_cast<uint8_t>(thread_id),
                                  sm_handler, 0);
  c.rpc = &rpc;
  // it will use dyn_resp_msgbuf if not a static sized response
  rpc.set_pre_resp_msgbuf_size(kMaxStaticMsgSize);
  // store client_ips
  boost::split(c.client_ips, FLAGS_client_ips, boost::is_any_of(","));
  c.nclient_threads = FLAGS_nclient_threads;

  c.my_client_id = get_client_id(FLAGS_my_ip, thread_id);


  clock_gettime(CLOCK_REALTIME, &c.tput_t0);
  printf("Thread %lu beginning main loop...\n", thread_id);
  fflush(stdout);
  while (!force_quit) {
    rpc.run_event_loop(100);
    c.check_notification_queue();
    c.check_client_connections();
    fflush(stdout);
    c.print_stats();
  }
}

void
launch_threads(size_t nthreads, erpc::Nexus *nexus) {
  // Spin up the requisite number of threads
  std::vector<std::thread> threads(nthreads);
  auto *app_stats = new app_stats_t[nthreads];

  for (size_t i = 0; i < nthreads; i++) {
    printf("Launching thread %zu\n", i);
    threads[i] = std::thread(zk_thread_func, i, nexus, app_stats);
    erpc::bind_to_core(threads[i],
                       N_ZKTHREADS > 8 ? i % 2 : numa_node,
                       nthreads > 8 ? i / 2 : i);
  }

  for (auto &thread : threads) thread.join();
  delete[] app_stats;
}

static void
signal_handler(int signum) {
  if (signum == SIGINT || signum == SIGTERM) {
    printf("\n\nSignal %d received, preparing to exit...\n",
           signum);
    force_quit = true;
  }
  if (signum == SIGSEGV) {
    void *array[10];
    int size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    // print out all the frames to stderr
    fprintf(stderr, "SEGFAULT: signal %d:\n", signum);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
  }
}

int
main(int argc, char **argv) {
  printf("Starting Corfu server...\n");
  fflush(stdout);
  int ret = 0;

  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGSEGV, signal_handler);

  // Parse command line args
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Create a Nexus object (1 per NUMA node)
  std::string uri = FLAGS_my_ip + ":31850";
  printf("Creating nexus at %s\n", uri.c_str());
  fflush(stdout);
  erpc::Nexus nexus(uri, numa_node, 0);

  nexus.register_req_func(static_cast<uint8_t>(OpType::kCreateNode),
                          create_node_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kCreateNodeWithChildren),
                          create_node_with_children_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kWriteNode),
                          write_node_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kRenameNode),
                          rename_node_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kDeleteAndReadNode),
                          delete_and_read_node_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kDeleteNode),
                          delete_node_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kReadNode),
                          read_node_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kAddChild),
                          add_child_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kRemoveChild),
                          remove_child_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kRenameChild),
                          rename_child_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kExists),
                          exists_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kGetChildren),
                          get_children_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kClientHeartbeat),
                          client_heartbeat_req_handler);
  nexus.register_req_func(static_cast<uint8_t>(OpType::kDeleteClientConnection),
                          delete_client_connection_req_handler);

  // Connections are per nexus (machine).
  LOG_INFO("kNumRxRingEntries %zu kSessionCredits %zu max connections %zu\n",
           erpc::Transport::kNumRxRingEntries, erpc::kSessionCredits,
           erpc::Transport::kNumRxRingEntries/erpc::kSessionCredits);

  // ...and launch threads.
  size_t nthreads = N_ZKTHREADS;
  LOG_INFO("Launching %zu threads\n", nthreads);
  launch_threads(nthreads, &nexus);

  printf("Bye...\n");
  return ret;
}