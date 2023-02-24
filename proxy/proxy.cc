#include "proxy.h"
#include "raft_callbacks.h"
#include "testing.h"
#include <rte_ethdev.h>
#include <sys/stat.h>
#include "gc.h"
#include "recovery.h"

#define INDUCE_HOLES 0

double freq_ghz;
long int update_ackd_time = 0;

// IP addresses for the other endpoints. All are global except for
// nextproxy_ip; this is only for the "last" thread on this machine,
// which will be passing its garbage along to another machine for collection.
std::string my_ip;
std::string nextproxy0_ip;
std::string nextproxy1_ip;
std::string nextproxy2_ip;
std::string seq_ip;
std::string backupseq_ip;
std::string client_ip;

// these are the two machines that hold the other two replicas
// for all of our proxy groups
std::string replica_1_ip;
std::string replica_2_ip;

std::vector<WorkerContext *> *context_vector;

size_t nsequence_spaces;

// Forward declarations
static void request_seqnum_cont_func(void *, void *);
static void submit_operation_cont_func(void *, void *);
static void get_and_persist_deps_cont_func(void *, void *);
static void persist_deps_cont_func(void *, void *);
static void heartbeat_cont_func(void *, void *);

// Recovery handlers
static void request_bitmap_handler(erpc::ReqHandle *, void *);
static void request_dependencies_handler(erpc::ReqHandle *, void *);

// For testing/mocking
uint64_t current_seqnum;

// Handle connections initiated by us
inline void
sm_handler(int session_num, erpc::SmEventType sm_event_type,
           erpc::SmErrType sm_err_type, void *_context) {
  auto *c = static_cast<WorkerContext *>(_context);

  LOG_ERROR("[%zu] In sm_handler\n", c->thread_id);

  erpc::rt_assert(
      sm_err_type == erpc::SmErrType::kNoError ||
          session_num ==
              c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)] ||
          sm_event_type == erpc::SmEventType::kDisconnected,
      "Got a SM error: " + erpc::sm_err_type_str(sm_err_type));

  if (!(sm_event_type == erpc::SmEventType::kConnected ||
      sm_event_type == erpc::SmEventType::kDisconnected)) {
    throw std::runtime_error("Unexpected SM event!");
  }

  if (sm_event_type == erpc::SmEventType::kConnected) {
    LOG_ERROR("\tGot a connection, session %d!\n", session_num);
    c->nconnections++;
  } else {
    LOG_ERROR("\tLost a connection, session %d!\n", session_num);
    c->nconnections--;
  }
}

void print_waiting_batch(Proxy *p) {
  auto bid = p->highest_cons_batch_id + 1;
  if (!p->in_done_batch_map(bid)) return;

  Batch *b = p->done_batch_map[bid];

  fmt_rt_assert(b->batch_size() != 0,
                "PID: %d waiting for bid %zu with size 0?",
                p->proxy_id, b->batch_id);

  char line[1024];
  memset(line, '\0', 1024);
  sprintf(line, "bid %zu waiting for:", b->batch_id);
  bool found = false;
  for (auto first : b->acked_ops) {
    if (!b->acked_ops[first.first]) {
      found = true;
      sprintf(line + strlen(line), "\n\tcid %u crid %zu",
              first.first.first, first.first.second);
    }
  }

  printf("hcbid+1: %s\n", line);
  fmt_rt_assert(found, "PID: %d all ops in waiting batch were acked!?\n\t"
                       "bid %d batch size %zu acked_ops size %zu",
                p->proxy_id, b->batch_id, b->batch_size(), b->acked_ops.size());
}

// Lifted (almost) verbatim from erpc code:
// https://github.com/erpc-io/eRPC/blob/master/apps/small_rpc_tput/small_rpc_tput.cc
static inline void
print_stats(void *_c) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  auto *c = reinterpret_cast<WorkerContext *>(_c);

  double seconds = erpc::sec_since(c->tput_t0);

  // this is packets per second not including appendentries
  double tput_mrps = c->stat_resp_tx_tot / (seconds * 1000000);
  double ae_pkts_per_sec = c->ae_pkts / (seconds * 1000000);
  double ae_bytes_per_sec = c->ae_bytes / (seconds);// * 1000000);
  (void) ae_bytes_per_sec;
  (void) ae_pkts_per_sec;
  c->app_stats[c->thread_id].mrps = tput_mrps;
  if (c->stat_batch_count > 0) {
    c->app_stats[c->thread_id].avg_batch_size = (
        c->stat_resp_tx_tot / c->stat_batch_count);
  } else {
    c->app_stats[c->thread_id].avg_batch_size = 0;
  }
#if !NO_ERPC
  c->app_stats[c->thread_id].num_re_tx = c->rpc->pkt_loss_stats.num_re_tx;
#else
  c->app_stats[c->thread_id].num_re_tx = 0;
#endif

  LOG_INFO("stat_resp_tx_tot %lu\n", c->stat_resp_tx_tot);

  c->app_stats[c->thread_id].lat_us_50 = c->latency.perc(0.50) / kAppLatFac;
  c->app_stats[c->thread_id].lat_us_99 = c->latency.perc(0.99) / kAppLatFac;
  c->app_stats[c->thread_id].lat_us_999 = c->latency.perc(0.999) / kAppLatFac;
  c->app_stats[c->thread_id].lat_us_9999 = c->latency.perc(0.9999) / kAppLatFac;

  char lat_stat[100];
  sprintf(lat_stat, "[%.2f, %.2f us]", c->latency.perc(.50) / kAppLatFac,
          c->latency.perc(.99) / kAppLatFac);

  if (likely(!NO_ERPC)) {
    LOG_INFO(
        "Thread %zu: %.3f Mrps, re_tx = %zu, still_in_wheel = %zu. "
        "RX: %zu resps. Latency: %s. Avg. Batch Size: %f AE pkts_s: %f AE bytes_s: %f\n",
        c->thread_id, tput_mrps, c->app_stats[c->thread_id].num_re_tx,
        c->rpc->pkt_loss_stats.still_in_wheel_during_retx,
        c->stat_resp_tx_tot, lat_stat, c->avg_batch_size, ae_pkts_per_sec,
        ae_bytes_per_sec);
  } else {
    LOG_INFO(
        "Thread %zu: %.3f Mrps, re_tx = %zu, still_in_wheel = %zu. "
        "RX: %zu resps. Latency: %s. Avg. Batch Size: %f AE pkts_s: %f AE bytes_s: %f\n",
        c->thread_id, tput_mrps, c->app_stats[c->thread_id].num_re_tx,
        static_cast<size_t>(0),
        c->stat_resp_tx_tot, lat_stat, c->avg_batch_size, ae_pkts_per_sec,
        ae_bytes_per_sec);
  }

  if (c->thread_id == 0) {
    app_stats_t accum;
    for (size_t i = 0; i < FLAGS_nthreads; i++) {
      accum += c->app_stats[i];
    }
    accum.avg_batch_size /= FLAGS_nthreads;
    accum.lat_us_50 /= FLAGS_nthreads;
    accum.lat_us_99 /= FLAGS_nthreads;
    accum.lat_us_999 /= FLAGS_nthreads;
    accum.lat_us_9999 /= FLAGS_nthreads;

    LOG_INFO("TOTAL: %s\n\n", accum.to_string().c_str());
  }

  std::string line("base seqnums ");
  for (size_t i = 0; i < nsequence_spaces; i++) {
    line += std::to_string(c->received_ms_seqnums[i]->base_seqnum) + " ";
  }
  LOG_INFO("%s\n", line.c_str());

  for (auto &el : c->proxies) {
    Proxy *proxy = el.second;
    LOG_INFO("Logical Proxy %u\n", proxy->proxy_id);
    LOG_INFO("\tis proxy_id: %d node_id %d the leader?: %d the leader is: %d\n",
             proxy->proxy_id, proxy->replica_data[my_raft_id].node_id,
             raft_is_leader(proxy->raft), raft_get_current_leader(proxy->raft));
    LOG_INFO("\tmax_received_seqnums: %lu\n", proxy->max_received_seqnum);

    auto *raft_p = reinterpret_cast<raft_server_private_t *>(proxy->raft);

    auto *log = reinterpret_cast<my_log_private_t *>(raft_p->log);
    (void) log;
    LOG_INFO("\tlog size: %ld, commit idx %ld "
             "\n\t\treplica 1 next idx: %ld "
             "\n\t\treplica 2 next idx: %ld "
             "\n\thighest_cons_batch_id: %zu\n",
             log->count, raft_p->commit_idx,
             raft_node_get_next_idx(raft_get_node(proxy->raft, 1)),
             raft_node_get_next_idx(raft_get_node(proxy->raft, 2)),
             proxy->highest_cons_batch_id);
    LOG_INFO("\tdone batch map size n: %zu in rough B %zu\n",
             proxy->done_batch_map.size(),
             proxy->done_batch_map.size() * sizeof(Batch));

    LOG_INFO("sizes: "
             "\n\t lacrid %ld"
             "\n\t apbm %ld"
             "\n\t nsbm %ld"
             "\n\t dbm %ld"
             "\n\t crdm %ld"
             "\n\t cripm %ld"
             "\n\t owh %ld"
             //                "\n\t cops %ld"
             "\n\t dbids %ld"
             "\n\t deleted_seq_req_ids %ld"
             "\n",
             proxy->last_ackd_crid.size(),
             proxy->appended_batch_map.size(),
             proxy->need_seqnum_batch_map.size(),
             proxy->done_batch_map.size(),
             proxy->client_retx_done_map.size(),
             proxy->client_retx_in_progress_map.size(),
             proxy->ops_with_handles.size(),
//                proxy->client_ops.size(),
             proxy->done_batch_ids.size(),
             proxy->deleted_seq_req_ids.size());
    LOG_INFO("highest_del_seq_req_id %zu\n", proxy->highest_del_seq_req_id);
    LOG_INFO("bitmaps\n");
    for (size_t i = 0; i < nsequence_spaces; i++) {
      LOG_INFO("\t sequence space %zu cap: %zu\n", i,
               proxy->c->received_ms_seqnums[i]->capacity());
    }
    line.clear();
    line += "client_retx_done_map ";
    for (auto const &pair : proxy->client_retx_done_map) {
      line += " count " + std::to_string(pair.second.size()) + " (b) " +
          std::to_string(pair.second.size() * sizeof(ClientOp));
    }
    LOG_INFO("%s\n", line.c_str());

    line.clear();
    line += "client_retx_in_progress_map ";
    for (auto const &pair : proxy->client_retx_in_progress_map) {
      line += " " + std::to_string(pair.second.size());
    }
    LOG_INFO("%s\n", line.c_str());

    line.clear();
    line += "ops_with_handles ";
    for (auto const &pair : proxy->ops_with_handles) {
      line += " " + std::to_string(pair.second.size());
    }
    LOG_INFO("%s\n", line.c_str());
  }

  LOG_INFO("total update_ackd time %ld\n", update_ackd_time);
  LOG_INFO("bitmap: avgtts %Lf avgts %Lf avgrd %Lf\n",
           cycles_to_usec(c->avg_time_bw_trying_to_send),
           cycles_to_usec(c->avg_time_bw_sending),
           cycles_to_usec(c->avg_ring_duration));

  c->stat_resp_tx_tot = 0;
  c->ae_pkts = 0;
  c->ae_bytes = 0;
  c->stat_batch_count = 0;
#if !NO_ERPC
  c->rpc->pkt_loss_stats.num_re_tx = 0;
#endif
  c->latency.reset();

  clock_gettime(CLOCK_REALTIME, &c->tput_t0);
  c->util_timers[STAT_TIMER_IDX].start();

#if PRINT_TIMING
  printf("[%s] print_stats [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

inline void
send_heartbeat(void *_c) {
  auto *c = reinterpret_cast<WorkerContext *>(_c);

  if (c->in_recovery) {
    c->stop_heartbeat_timer();
    return;
  }

  Proxy *proxy = c->proxies[FLAGS_proxy_id_start + c->thread_id];
  Batch *batch = proxy->batch_pool.alloc();
  batch->reset(c, proxy, 0, 0);

  Tag *tag = proxy->tag_pool.alloc();
  tag->alloc_msgbufs(c, batch, false);

  LOG_SEQ("[%zu] Sending heartbeat to sequencer, tag %p...\n",
          c->thread_id,
          static_cast<void *>(tag));

  uint8_t
      session_num = c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)];

  c->rpc->enqueue_request(session_num,
                          static_cast<uint8_t>(ReqType::kHeartbeat),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          heartbeat_cont_func,
                          reinterpret_cast<void *>(tag));

  // Set timer callback to recovery; declare sequencer dead if we don't
  // hear back before timeout
  if (c->heartbeat_last_call == false) {
    c->heartbeat_last_call = true;
    c->reset_heartbeat_timer(send_heartbeat);
  } else {
    LOG_SEQ("\tResetting heartbeat timer...\n");
    c->reset_heartbeat_timer(initiate_recovery);
    LOG_RECOVERY("t%zu: Seq packet dropped? Recovery next...\n",
                 c->thread_id);
  }
}

inline void
heartbeat_cont_func(void *_c, void *_tag) {
  // Reset the timer
  auto *c = reinterpret_cast<WorkerContext *>(_c);
  Tag *tag = reinterpret_cast<Tag *>(_tag);
  Batch *b = tag->batch;

  if (tag->resp_msgbuf.get_data_size() == 0) {
    b->proxy->tag_pool.free(tag);
    b->proxy->batch_pool.free(b);
    return;
  }

  b->proxy->tag_pool.free(tag);
  b->proxy->batch_pool.free(b);
  LOG_SEQ("[%zu] Freed heartbeat tag/batch\n", c->thread_id);

  c->reset_heartbeat_timer(send_heartbeat);
  LOG_RECOVERY("[%zu] Reset timer\n", c->thread_id);

  c->heartbeat_last_call = false;
  LOG_RECOVERY(
      "[%zu] Heartbeat cont func done!\n", c->thread_id);
}

inline void
reply_not_leader(erpc::ReqHandle *req_handle,
                 uint16_t pid, uint16_t cid, client_reqid_t crid, Proxy *p) {
  p->c->rpc->resize_msg_buffer(
      &req_handle->pre_resp_msgbuf, client_payload_size(nsequence_spaces));

  auto *payload = reinterpret_cast<client_payload_t *>(
      req_handle->pre_resp_msgbuf.buf);
  payload->proxy_id = pid;
  payload->client_id = cid;
  payload->client_reqid = crid;
  payload->not_leader = true;

  p->c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}

void
Proxy::update_ackd_reqids(uint16_t cid, int64_t ackd_crid) {
  if (unlikely(!in_last_acked_map(cid))) {
    LOG_ERROR("client was not in map\n");
    last_ackd_crid[cid] = -1;
  }
  // if ackd_crid is -1, last_ackd_crid is -1, so shouldn't execute
  for (int64_t i = last_ackd_crid[cid] + 1; i <= ackd_crid; i++) {
    ClientOp *cop = nullptr;
    if (!(cop = in_client_retx_done_map(cid, i))) {
      if (!(cop = in_in_progress_map(cid, i))) {
        continue;
      }
    }
    erpc::rt_assert(cop != nullptr, "We got a ClientOp nullptr???\n");

    auto *b = cop->batch;
    // every client request has been ackd
    // returns true if this batch is now completely ackd
    if (b->ack_op(cop)) {
      push_and_update_highest_cons_batch_id(b->batch_id);
    }
  }

  LOG_COMPACTION("got ack for cid %u rid %ld, updated highest_cons_bid %zu\n",
                 cid, ackd_crid, highest_cons_batch_id);

  // there could be reordering
  if (likely(ackd_crid > last_ackd_crid[cid])) {
    last_ackd_crid[cid] = ackd_crid;
  }
}

inline void
client_op_handler(erpc::ReqHandle *req_handle, void *_context) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif
  auto *c = static_cast<WorkerContext *>(_context);

  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  fmt_rt_assert(req_msgbuf->get_data_size()
                    == CLIENT_PAYLOAD_SIZE(nsequence_spaces),//sizeof(client_payload_t),
                "Payload from client is the wrong size! Is %zu need %zu\n",
                req_msgbuf->get_data_size(),
                CLIENT_PAYLOAD_SIZE(nsequence_spaces));

  auto *request = reinterpret_cast<client_payload_t *>(req_msgbuf->buf);
  LOG_FAILOVER("Got a request for proxy %d\n", request->proxy_id);

  fmt_rt_assert(c->in_proxies_map(request->proxy_id),
                "I do not manage the proxy_id this client (id: %u)"
                " sent to! proxy_sent: %d\n",
                request->client_id, request->proxy_id);

  Proxy *proxy = c->proxies[request->proxy_id];

  // if I'm not the leader, but I should be the leader, I shouldn't fail?
  // if I shouldn't be the leader and I'm receiving a request,
  // this is buggy right now and should fail
  // only happens during failover
  if (unlikely(!raft_is_leader(proxy->raft))) {
    // we are not the leader for this proxy_id, we should not process this request
    LOG_FAILOVER("I am not the leader for the proxy_id "
                 "this client sent to! proxy_sent: %d\n", request->proxy_id);
    reply_not_leader(req_handle, request->proxy_id,
                     request->client_id, request->client_reqid, proxy);
    return;
  }

  // it always safe to act on ack'd client requests if I'm the leader
  // always update, could maintain a var that maintains last ack'd to see if need to update
  //  but we are unlikely to repeat very often due to eRPC guarantees
  proxy->update_ackd_reqids(request->client_id, request->highest_recvd_reqid);

  // check if we actually already have a request for this req_id
  // this is possible if the previous leader failed, and we need
  // to finish the request.
  // If it is in the map then the request is not complete, though it has
  // received a sequence number.
  // Only happens during failover.
  if (unlikely(proxy->in_in_progress_map(
      request->client_id, request->client_reqid))) {
    // this is a retransmit the follower (now leader) has and is working on. ignore
    // add resp_handler to the op in the batch so that we can reply when it finishes
    ClientOp *op = proxy->client_retx_in_progress_map
    [request->client_id][request->client_reqid];

    LOG_FAILOVER("pid: %u client_id %d op %lu "
                 "is in progress in batch_id %lu\n",
                 proxy->proxy_id, op->client_id, op->client_reqid,
                 op->batch->batch_id);

    if (op->has_handle) {
      // shouldn't ever happen =(
      LOG_FAILOVER("in in progress map got a retx with an op "
                   "that already has a handle: client_id %d op %ld\n",
                   request->client_id, request->client_reqid);
      // already have a handle, ignore
      return;
    }

    op->req_handle = req_handle;
    op->has_handle = true;

    if (op->committed) {
      op->submit_operation();
    }

    proxy->ops_with_handles[op->client_id][op->client_reqid] = op;
    return;
  }

  if (unlikely(proxy->in_client_retx_done_map(request->client_id,
                                              request->client_reqid))) {
    // this is a retransmit the follower (now leader) has completed
    // put the handler into the existing op and reply

    ClientOp *op =
        proxy->client_retx_done_map[request->client_id][request->client_reqid];

    fmt_rt_assert(!op->has_handle, "in done map got a retx with an op that "
                                   "already has a handle: client_id %d op %d\n",
                  request->client_id, request->client_reqid);

    proxy->client_retx_done_map[request->client_id].erase(
        request->client_reqid);

    // we don't add to has_handle map since it will be removed in respond_to_client
    op->has_handle = true;
    op->req_handle = req_handle;
    op->respond_to_client();
    return;
  }

  if (unlikely(proxy->in_client_retx_done_map(request->client_id,
                                              request->client_reqid))) {
    // this is a retransmit the follower (now leader) has completed
    // put the handler into the existing op and reply

    ClientOp *op =
        proxy->client_retx_done_map[request->client_id][request->client_reqid];

    fmt_rt_assert(!op->has_handle, "in done map got a retx with an op that "
                                   "already has a handle: client_id %d op %d\n",
                  request->client_id, request->client_reqid);

    proxy->client_retx_done_map[request->client_id].erase(request->client_reqid);

    // we don't add to has_handle map since it will be removed in respond_to_client
    op->has_handle = true;
    op->req_handle = req_handle;
    op->respond_to_client();
    return;
  }

  /* If here we are the leader and haven't seen this op before.
   * For FIFO ordering, we need to put each client request into a per-client queue
   * ordered by client_reqid. Each time we hear back from the sequencer, we can
   * put into a batch the next set of consecutive operations for each client.
   */
  /* If we're the leader, we've either inherited the client_reqid history
   * from the previous leader or we've never seen anything from this
   * client before.
   */
  ClientOp *op = proxy->client_op_pool.alloc();
  op->populate(request->reqtype, request->client_id, request->client_reqid,
               request->proxy_id, req_handle, proxy->op_counter++, proxy, c,
               request->zk_payload);
  proxy->enqueue_or_add_to_batch(op);

  // add to map we need to maintain for acking requests...
  // This can be done now even if the op isn't added to a batch; it would
  // be equivalent to the batch getting delayed en route to sequencer.
  proxy->client_retx_in_progress_map[request->client_id][request->client_reqid]
    = op;

#if PRINT_TIMING
  printf("[%s] client_op_handler [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

// Fill in request to sequencer for particular batch size
inline void
Batch::populate_seqnum_request_buffer(Tag *tag) {
#if !NO_ERPC
// could try freeing then reallocating...
  c->rpc->free_msg_buffer(tag->req_msgbuf);
  c->rpc->free_msg_buffer(tag->resp_msgbuf);
  tag->req_msgbuf =
      c->rpc->alloc_msg_buffer_or_die(SEQUENCE_PAYLOAD_SIZE(nsequence_spaces));
  tag->resp_msgbuf =
      c->rpc->alloc_msg_buffer_or_die(SEQUENCE_PAYLOAD_SIZE(nsequence_spaces));
  c->rpc->resize_msg_buffer(
      &tag->req_msgbuf, SEQUENCE_PAYLOAD_SIZE(nsequence_spaces));
  auto *payload = reinterpret_cast<payload_t *>(tag->req_msgbuf.buf);
  memset(payload, 0, SEQUENCE_PAYLOAD_SIZE(nsequence_spaces));
#else
  payload_t *payload = reinterpret_cast<payload_t *>(tag->req_msgbuf);
#endif

  payload->proxy_id = proxy_id;
  payload->batch_id = batch_id;
  payload->seq_req_id = seq_req_id;
  payload->retx = false;

  copy_seq_reqs(payload->seq_reqs, seq_reqs);
}

// Contact sequencer for a batch of seqnums
inline void
Batch::request_seqnum(bool failover) {
  erpc::rt_assert(false, "Should not be requesting seqnum\n");
  if (c->in_recovery) {
    return;
  }

  LOG_SEQ("Requesting seqnum for batch_id %lu, size %zu\n",
          batch_id, batch_size());

#if MOCK_SEQ
  mock_request_seqnum(failover);
#else

  // Enqueue the request; use the batch object as the tag
  Tag *tag = proxy->tag_pool.alloc();
  tag->alloc_msgbufs(c, this, failover);
  populate_seqnum_request_buffer(tag);

  erpc::rt_assert(!failover || batch_size() == 0,
                  "there are client ops in a failover batch\n");

  uint8_t
      session_num = c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)];

  recovery_tag = tag;

  LOG_SEQ("Requesting seq num batch_id %zu srid %zu batch addr %p tag %p\n",
          batch_id,
          seq_req_id,
          reinterpret_cast<void *>(tag->batch),
          reinterpret_cast<void *>(tag));

  erpc::rt_assert(tag->req_msgbuf.get_data_size()
                      == SEQUENCE_PAYLOAD_SIZE(nsequence_spaces),
                  "seq request payload wrong size\n");

  c->rpc->enqueue_request(session_num,
                          static_cast<uint8_t>(ReqType::kGetSeqNum),
                          &(tag->req_msgbuf), &tag->resp_msgbuf,
                          request_seqnum_cont_func,
                          reinterpret_cast<void *>(tag));

  proxy->c->stat_resp_tx_tot++;
#endif
}

// Get the seqnum and update the proxy's max_received_seqnum. Start piggyback
// timer in case no new requests come in.
inline void
request_seqnum_cont_func(void *_context, void *_tag) {

#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  auto *c = reinterpret_cast<WorkerContext *>(_context);
  auto tag = static_cast<Tag *>(_tag);
  Batch *b = tag->batch;
  Proxy *proxy = b->proxy;

  if (tag->resp_msgbuf.get_data_size() == 0) {
    // Failed continuation! The batch will have been saved in the outstanding
    // batch map. Free tag and return.
    return;
  }

  // We just heard from the sequencer; restart the heartbeat timer
  c->reset_heartbeat_timer(send_heartbeat);
  c->heartbeat_last_call = false;

#if !NO_ERPC
  auto *payload = reinterpret_cast<payload_t *>(tag->resp_msgbuf.buf);
#else
  payload_t *payload = reinterpret_cast<payload_t *>(tag->resp_msgbuf);
#endif

  if (!raft_is_leader(b->proxy->raft)) {
    // I am no longer the leader for a seqnum that I requested, don't do anything
    debug_print(1, "[%zu] Not the leader!\n", c->thread_id);
    return;
  }

  // This proxy is the leader for its group.
  LOG_SEQ("[%zu] received_seqnums\n", c->thread_id);
  if (LOG_DEBUG_SEQ) print_seqreqs(payload->seq_reqs, nsequence_spaces);
  // Make sure we got the right response...
  if (payload->batch_id != b->batch_id) {
    // LOG_DT("Expected %zu, got %zu. Batch addr %p, tag %p\n",
    //             b->batch_id, payload->batch_id, b, tag);
  }

  LOG_SEQ("Thread %lu: Got seqnums for batch_id %lu "
          "seq_req_id %lu reqtype of [0] %u retx %d\n",
          b->proxy->c->thread_id,
          b->batch_id, b->seq_req_id,
          static_cast<uint8_t>(b->batch_client_ops[0]->reqtype), payload->retx);
  // debug_seq
  if (DEBUG_SEQ) print_seqreqs(payload->seq_reqs, nsequence_spaces);

  // Make sure we got the right response...
  fmt_rt_assert(payload->batch_id == b->batch_id,
                "Got a seqnum response for another batch! "
                "Expected %zu, got %zu. Batch addr %p, tag %p\n",
                b->batch_id, payload->batch_id, b, tag);

  if (unlikely(tag->failover)) {
    erpc::rt_assert(false, "no failover yet for zookeeper, bad");
    debug_print(1, "We've failed over!\n");
    printf("received a response for a previous srid nops (should be 0) == %zu\n",
           tag->batch->batch_size());
    fflush(stdout);
    erpc::rt_assert(tag->batch->batch_size() == 0,
                    "failover batch size not 0 in noop batch\n");

    ClientOp *op = proxy->client_op_pool.alloc();
    op->populate(ReqType::kNoop, UINT16_MAX, INT64_MAX,
                 proxy->proxy_id, nullptr, proxy->op_counter++, proxy);
    // make it so that it is like 1 client getting all the numbers...
    // this is so the followers add the seqnums to their bitmaps
    copy_seq_reqs(op->seq_reqs, payload->seq_reqs);
    // all the seq_reqs should be 0 at this point in the batch before we
    // add to the batch
    for (size_t j = 0; j < nsequence_spaces; j++) {
      erpc::rt_assert(b->seq_reqs[j].seqnum == 0,
                      "seqnum not zero in empty noop batch\n");
      erpc::rt_assert(b->seq_reqs[j].batch_size == 0,
                      "batch_size not zero in empty noop batch\n");
    }
    proxy->add_op_to_batch(op, b);
  } else if (unlikely(payload->retx)) {
    // a retx for a "future" srid
    LOG_ERROR("[%u] It's a retx, batch_id %zu, "
              "seq_req_id %zu!\n", proxy->proxy_id, b->batch_id, b->seq_req_id);
    print_seqreqs(payload->seq_reqs, nsequence_spaces);
    // This seqnum must go to a noop; a different leader requested this!
    // We must re-request a sequence number for ops in this
    // batch. Put these ops in the current batch, replace with noops.
    for (auto op : b->batch_client_ops) {
      proxy->add_op_to_batch(op);
    }
    b->batch_client_ops.clear();

    ClientOp *op = proxy->client_op_pool.alloc();
    erpc::rt_assert(false, "no noop yet for zookeeper, bad");
    op->populate(ReqType::kNoop, UINT16_MAX, INT64_MAX,
                 proxy->proxy_id, nullptr, proxy->op_counter++, proxy);

    // make it so that it is like 1 client getting all the numbers...
    // this is so the followers add the seqnums to their bitmaps
    copy_seq_reqs(op->seq_reqs, payload->seq_reqs);
    // all the seq_reqs should be 0 at this point in the batch before we
    // add to the batch
    for (size_t j = 0; j < nsequence_spaces; j++) {
      b->seq_reqs[j].seqnum = 0;
      b->seq_reqs[j].batch_size = 0;
      erpc::rt_assert(b->seq_reqs[j].seqnum == 0,
                      "seqnum not zero in empty noop batch\n");
      erpc::rt_assert(b->seq_reqs[j].batch_size == 0,
                      "batch_size not zero in empty noop batch\n");
    }
    proxy->add_op_to_batch(op, b);
    erpc::rt_assert(b->batch_client_ops.size() == 1, "retx batch size not 1\n");
  }

  // Update seqnum for this batch and for proxy
#if INDUCE_HOLES
  if (payload->seqnum == 500) {
      // Pretend it got dropped...
      printf("DROPPING SEQNUM 500!!!\n");
      return;
  }
#endif

  // we replicate even noop batches
  process_received_seqnums(b, payload->seq_reqs);

  b->proxy->tag_pool.free(tag);

#if PRINT_TIMING
  printf("[%s] seq_num_cont_func [%s]\n",
         start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

// todo remove arg seqnum
inline void
process_received_seqnums(Batch *b, seq_req_t seq_reqs[]) {
  // if noops now both the batch and the cli op have it
  copy_seq_reqs(b->seq_reqs, seq_reqs);

  b->assign_seqnums_to_ops_ms_test();

  b->replicate_seqnums();

  // This has to be done after the call to replicate. We want to make sure
  // that if this next batch of ops is sequenced and persisted, then the
  // ones before have also been sequenced and persisted.
  b->proxy->release_queued_ops();
}

/* Add op to batch if all ops before it for this client have been sequenced
 * or if the op before it (for this client) is in this batch.
 */
inline bool
Proxy::enqueue_or_add_to_batch(ClientOp *op) {
  uint64_t cid = op->client_id;

  if (!in_highest_sequenced_crid_map(cid)) {
    highest_sequenced_crid[cid] = -1;
  }

  // Check if any other ops for this client are in the batch;
  // if not, get the last sequenced client_reqid
  if (!current_batch->in_highest_crid_this_batch_map(cid)) {
    current_batch->highest_crid_this_batch[cid] =
        highest_sequenced_crid[cid];
  }
  current_batch->highest_crid_this_batch[cid] = std::max(
      highest_sequenced_crid[cid],
      current_batch->highest_crid_this_batch[cid]);

  // Put the op into the batch if it can be sequenced in this batch; else
  // enqueue it for the next batch. If ops get reordered en route from client,
  // they will get added to this batch before the batch closes.
  LOG_FIFO("batch_id %zu, cid %zu: "
           "highest_sequenced_crid %ld, "
           "highest_crid_this_batch %ld, "
           "checking op %zu\n",
           current_batch->batch_id, cid, highest_sequenced_crid[cid],
           current_batch->highest_crid_this_batch[cid], op->client_reqid);
  if (op->client_reqid == (current_batch->highest_crid_this_batch[cid] + 1)) {
    LOG_FIFO("Adding op %zu to batch\n", op->client_reqid);
    add_op_to_batch(op);
    current_batch->highest_crid_this_batch[cid]++;
    return false;
  } else {
    LOG_FIFO("Enqueueing op %zu\n", op->client_reqid);
    enqueue_op(op);
    return true;
  }
}

/* Push op onto client-specific queue to be sequenced later.
 */
inline void
Proxy::enqueue_op(ClientOp *op) {
  op_queues[op->client_id].push(op);
}

/* Check each client's queued ops against the highest client_reqid.
 * If the queued op's ID is equal to the icreqid, put it into the current batch;
 * repeat until there are no more requests for any client.
 */
inline void
Proxy::release_queued_ops() {
  LOG_FIFO("About to check for queued ops\n");
  for (auto &it : op_queues) {
    uint64_t cid = it.first;
    while (!op_queues[cid].empty()) {
      LOG_FIFO("op_queues for cid %zu has %zu elements.\n",
               cid, op_queues.size());
      ClientOp *op = op_queues[cid].top();
      op_queues[cid].pop();
      bool enqueued = enqueue_or_add_to_batch(op);
      if (enqueued) {
        break;
      }
    }
  }
}

// Record sequence numbers received from the sequencer
inline void
Batch::record_ms_seqnums() {
  LOG_RAFT("[%zu] Recording seqnums\n", proxy->c->thread_id);
  for (auto op : batch_client_ops) {
    LOG_RAFT("[%zu] op->seqreqs\n", proxy->c->thread_id);
    for (size_t i = 0; i < nsequence_spaces; i++) {
      if (op->seq_reqs[i].batch_size == 0) continue;

      uint64_t
          cur_seqnum = op->seq_reqs[i].seqnum - op->seq_reqs[i].batch_size + 1;
      // only insert if we didn't already truncate.
      // this can happen on followers if they receive notification
      // they can truncate (and do so) before applying the seqnum
      while (cur_seqnum <= op->seq_reqs[i].seqnum) {
        LOG_RAFT("[%zu] About to insert ss %zu sn %zu\n",
                 proxy->c->thread_id, i, cur_seqnum);
        if (cur_seqnum >= c->received_ms_seqnums[i]->base_seqnum)
          c->received_ms_seqnums[i]->insert_seqnum(cur_seqnum);
        cur_seqnum++;
      }
    }
  }
  LOG_RAFT("[%zu] Done Recording seqnums\n", proxy->c->thread_id);
}

inline void
Batch::assign_seqnums_to_ops_ms_test() {
  auto *current_seq_reqs = new seq_req_t[nsequence_spaces];
  for (size_t i = 0; i < nsequence_spaces; i++) {
    current_seq_reqs[i].seqnum = seq_reqs[i].seqnum - seq_reqs[i].batch_size;
    current_seq_reqs[i].batch_size = seq_reqs[i].batch_size;
  }

  for (auto op : batch_client_ops) {
    // noops already have there reqs and batch sizes assigned
    if (op->reqtype == ReqType::kNoop) {
      fmt_rt_assert(batch_client_ops.size() == 1,
                    "not only 1 client ops in noop batch %zu\n",
                    batch_client_ops.size());
      return;
    }
    if (op->reqtype == ReqType::kNoop) {
      fmt_rt_assert(batch_client_ops.size() == 1,
                    "not only 1 client ops in noop batch %zu\n",
                    batch_client_ops.size());
    }

    for (size_t i = 0; i < nsequence_spaces; i++) {
      if (op->seq_reqs[i].batch_size == 0) continue;

      current_seq_reqs[i].seqnum += op->seq_reqs[i].batch_size;
      op->seq_reqs[i].seqnum = current_seq_reqs[i].seqnum;
      fmt_rt_assert(current_seq_reqs[i].seqnum <= seq_reqs[i].seqnum,
                    "assign_seqnums_to_ops assigned more seqnums than we were allocated! "
                    "assigned %zu allocated %zu",
                    current_seq_reqs[i].seqnum,
                    seq_reqs[i].seqnum);

    }
    LOG_SEQ("Assigning batch_id %lu op %lu client %d with seqnum %lu\n",
            this->batch_id, op->client_reqid, op->client_id, current_seqnum);

    LOG_FIFO("highest_sequenced_crid %ld, client_reqid %zu\n",//, sn %zu\n",
             proxy->highest_sequenced_crid[op->client_id],
             op->client_reqid);

    proxy->highest_sequenced_crid[op->client_id] = std::max(
        proxy->highest_sequenced_crid[op->client_id],
        op->client_reqid);
  }
}

inline void
Batch::replicate(EntryType type) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif
  entry_t *entry = init_entry(batch_client_ops.size());
  size_t entry_size = ENTRY_SIZE(batch_client_ops.size());

  entry->batch = this;

  // initialize raft entry
  entry->highest_cons_batch_id = proxy->highest_cons_batch_id;
  entry->type = type;
  entry->seq_req_id = seq_req_id;
  entry->batch_id = batch_id;
  entry->batch_size = batch_client_ops.size();

  auto *cmdata = entry->cmdata_buf;
  for (size_t i = 0; i < batch_client_ops.size(); i++) {
    cmdata[i].reqtype = batch_client_ops[i]->reqtype;
    cmdata[i].client_id = batch_client_ops[i]->client_id;
    cmdata[i].client_reqid = batch_client_ops[i]->client_reqid;
    cmdata[i].zk_payload = batch_client_ops[i]->zk_payload;
    copy_seq_reqs(cmdata[i].seq_reqs, batch_client_ops[i]->seq_reqs);
  }

  // here we've created the entry
  auto *buf = static_cast<uint8_t *>(malloc(entry_size));
  serialize_entry(buf, entry);

  delete entry;

  // create raft entry
  msg_entry_t raft_entry;
  raft_entry.type = RAFT_LOGTYPE_NORMAL;
  raft_entry.data.buf = buf;
  raft_entry.data.len = entry_size;
  raft_entry.id = my_raft_id;

  raft_recv_entry(proxy->raft, &raft_entry, &raft_entry_response);

#if PRINT_TIMING
  printf("[%s] replicate [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

void
Proxy::replicate_recovery() {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  LOG_RECOVERY("[%zu] Proxy %d in replicate recovery.\n",
               c->thread_id, proxy_id);

  entry_t *entry;
  size_t n = 0;

  entry = reinterpret_cast<entry_t *>(malloc(ENTRY_SIZE(n)));

  // initialize raft entry
  entry->batch = nullptr;
  entry->type = EntryType::kSwitchToBackupSeq;
  entry->batch_id = 0;
  entry->seq_req_id = 0;
  entry->batch_size = 0;

  // create raft entry
  msg_entry_t raft_entry;
  raft_entry.type = RAFT_LOGTYPE_NORMAL;
  raft_entry.data.buf = entry;
  raft_entry.data.len = ENTRY_SIZE(n);
  raft_entry.id =
      my_raft_id;

  raft_recv_entry(raft, &raft_entry, &dummy_entry_response);

#if PRINT_TIMING
  printf("[%s] replicate [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

// Replicate sequence number among Raft peers
inline void
Batch::replicate_seqnums() {
  this->replicate(EntryType::kSequenceNumberNoncontig);
}

// Send each operation in the batch to the overlying system
inline void
Batch::submit_batch_to_system() {
  for (auto op : batch_client_ops) {
    static int i = 0;
    LOG_RAFT("[%zu] Submitting op with type %d to system\n",
             c->thread_id, static_cast<uint8_t>(op->reqtype));
    if (op->reqtype == ReqType::kNoop) { // todo figure out noop
      erpc::rt_assert(i == 0,
                      "got a noop where the first op was not noop\n");
      // one of the ops in the batch is a kNoop, they all should be!
      for (auto cop : batch_client_ops) {
        fmt_rt_assert(cop->reqtype == ReqType::kNoop,
                      "not all ops in a noop batch were actually noops\n");
      }
      send_noop_to_client(this, op);
    } else {
      op->submit_operation();
    }
  }
}

// this would make assigning seqnums easier
// but I don't know what seq_reqs are used for later
// metadata * in tag would be useful
inline uint64_t consume_seqnum(seq_req_t *seq_req) {
  erpc::rt_assert(seq_req->batch_size > 0, "consuming with batch size 0");
  uint64_t seqnum = seq_req->seqnum - seq_req->batch_size + 1;
  seq_req->batch_size--;
  return seqnum;
}

// should include a "done" function to call when all responses received
// tag should include some metadata in order to respond to clients
// or execute further rpcs
void one_shot_cont_func(void *, void *_t) {
  auto *tag = reinterpret_cast<Tag *>(_t);

  // replies is a struct that has one element per "shard" request
  // one physical shard may be multiple elements if it has > 1 request
  auto &replies = *(tag->replies);
  replies[tag->my_shard_idx]++;

  // check if have quorum from all, if so can return to client
  if (!*tag->returned_to_client) {
    bool got_quorum = true;
    // if we received a quorum from each shard
    for (size_t shard_reps : replies) {
      if (shard_reps < 2)
        got_quorum = false;
    }
    // can return to client
    if (got_quorum) {
      submit_operation_cont_func(nullptr, reinterpret_cast<void *>(tag->op));
      *tag->returned_to_client = true;
    }
  }

  // if we received a response from each shard we can delete things
  for (size_t reps : replies) {
    if (reps != kZKReplicationFactor) {
      // we always free this tag
      tag->op->proxy->tag_pool.free(tag);
      return;
    }
  }
  delete tag->replies;
  delete tag->returned_to_client;
  if (tag->seq_reqs) {
    delete[] tag->seq_reqs;
  }
  // should this delete tag->seq_reqs?
  tag->op->proxy->tag_pool.free(tag);
}

// this seems to be the same as create_node_cont_func?
// and seems very similar to other _cont_funcs, maybe can make a more general
// function
void read_node_cont_func(void *, void *_t) {
  auto *tag = reinterpret_cast<Tag *>(_t);

  auto &replies = *(tag->replies);
  replies[tag->my_shard_idx]++;
  erpc::rt_assert(replies.size() == 1, "Not 1 shard for write request?");
  // check if have quorum from all, if so can return to client
  if (!*tag->returned_to_client) {
    bool got_quorum = true;
    // loop through each shard in replies, if any are < 2 got_quorum is false
    for (size_t shard_reps : replies) {
      if (shard_reps < 2)
        got_quorum = false;
    }
    if (got_quorum) {
      tag->op->zk_payload.read_node.version =
          *reinterpret_cast<int32_t *>(tag->resp_msgbuf.buf);
      memcpy(tag->op->zk_payload.read_node.data,
             tag->resp_msgbuf.buf + sizeof(int32_t), MAX_ZNODE_DATA);
      submit_operation_cont_func(nullptr, reinterpret_cast<void *>(tag->op));
      *tag->returned_to_client = true;
    }
  }

  // check each shard and then reply to client or whatever if response from each
  for (size_t reps : replies) {
    if (reps != kZKReplicationFactor) {
      tag->op->proxy->tag_pool.free(tag);
      return;
    }
  }
  delete tag->replies;
  delete tag->returned_to_client;
  tag->op->proxy->tag_pool.free(tag);
}

void send_req_to_shard(ClientOp *op, size_t shard, size_t shard_idx,
                       OpType op_type, std::vector<size_t> *replies,
                       bool *returned_to_client, seq_req_t *seq_reqs, void *buf,
                       size_t buf_size, erpc::erpc_cont_func_t cont_func) {
  auto *c = op->c;
  // for each node in the shard
  for (size_t i = 0; i < kZKReplicationFactor; i++) {
    Tag *tag = op->proxy->tag_pool.alloc();
    tag->alloc_msgbufs(c);
    c->rpc->resize_msg_buffer(&tag->req_msgbuf, buf_size);
    tag->my_shard_idx = shard_idx;
    tag->replies = replies;
    tag->returned_to_client = returned_to_client;
    tag->op = op;
    tag->seq_reqs = seq_reqs;

    memcpy(tag->req_msgbuf.buf, buf, buf_size);

    int session_num = c->zk_session_nums[shard][i];
    // to handle zk failures we just don't send if not connected?
    erpc::rt_assert(c->rpc->is_connected(session_num),
                    "not connected to zk server trying to send to\n");
    c->rpc->enqueue_request(session_num, op_type, &tag->req_msgbuf,
                            &tag->resp_msgbuf, cont_func, tag);
  }
}

// One shot
// right now. recursive creation is not supported. but it is also not checked.
// clients need to submit create_node for each parent dir it is unsure exists?
// there can be a flag that says "no data" to not modify data
//
void submit_create_node(ClientOp *op) {
  Proxy *proxy = op->proxy;
  // creates node with version specified in create_node
  create_node_t *create_node = &op->zk_payload.create_node;
  proxy->execute_create_node(create_node);

  // create root only goes to one shard.
  if (std::string(create_node->name) != std::string("/")) {
    // then send the add_child to its parent
    add_child_t add_child;
    std::string parent(get_parent(create_node->name));

    strcpy(add_child.name, parent.c_str());
    strcpy(add_child.child, create_node->name);
    // it's possible they go to the same shard...
    add_child.seqnum = 0;
    proxy->execute_add_child(&add_child);
  }
  submit_operation_cont_func(nullptr, op);
}

void submit_server_delete_client_connection(ClientOp *op) {
  Proxy *proxy = op->proxy;
  LOG_SERVER("In submit server delete client connection\n");
  proxy->execute_delete_client_connection(
      &op->zk_payload.delete_client_connection);
}

// One shot
// delete a client connection
void submit_delete_client_connection(ClientOp *op) {
  Proxy *proxy = op->proxy;
  proxy->execute_delete_client_connection(
      &op->zk_payload.delete_client_connection);
  // this might be deleted via a timer, so no reply to client is necesary
  if (true) {
    submit_operation_cont_func(nullptr, op);
  }
}

inline void
client_heartbeat_handler(erpc::ReqHandle *req_handle, void *_context) {
  auto *c = static_cast<WorkerContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  auto *client_heartbeat = reinterpret_cast<client_heartbeat_t *>(
      req_msgbuf->buf);
  Proxy *p = c->proxies[client_heartbeat->proxy_id];
  erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(client_heartbeat_t),
                  "client_heartbeat args wrong size");

  auto *chreq = reinterpret_cast<client_heartbeat_t *>(req_msgbuf->buf);
  LOG_SERVER("Thread %zu: received client heartbeat from %d\n",
           c->thread_id, chreq->client_id);

  // setup response
  client_heartbeat_resp_t chresp;
  try {
    // this client connection number is older than the current number
    if (chreq->client_connection_number < p->client_connection_numbers.at(
        chreq->client_id)) {
      chresp.deleted = false;
      chresp.client_connection_number = p->client_connection_numbers.at(
          chreq->client_id);
    }
      // this server is behind the "current" client connection, reply, but do nothing
    else if (chreq->client_connection_number > p->client_connection_numbers.at(
        chreq->client_id)) {
      chresp.deleted = false;
      chresp.client_connection_number = p->client_connection_numbers.at(
          chreq->client_id);
    }
      // this is not the first connection and it's the current connection
      // try to reset it (it might still have been deleted if server timed out)
    else {
      chresp.deleted = p->reset_client_timer(chreq->client_id);
      chresp.client_connection_number = p->client_connection_numbers.at(
          chreq->client_id);
    }
  } catch (std::out_of_range &) {
    // didn't have connection number,
    // this is for a connection we don't know about yet
    chresp.deleted = false;
    chresp.client_connection_number = 0;
  }

  LOG_SERVER("Returning client_heartbeat_resp deleted %d connection num %zu\n",
           chresp.deleted, chresp.client_connection_number);
  p->c->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf, sizeof(chresp));
  *reinterpret_cast<client_heartbeat_resp_t *>(
      req_handle->pre_resp_msgbuf.buf) = chresp;
  p->c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}

// One shot
void submit_write_node(ClientOp *op) {
  write_node_t *write_node = &op->zk_payload.write_node;
  // actual write node request
  op->proxy->execute_write_node(write_node);
  submit_operation_cont_func(nullptr, op);
  return;
}

void submit_read_node(ClientOp *op) {
  op->proxy->execute_read_node(
      reinterpret_cast<read_node_t *>(&op->zk_payload));
  submit_operation_cont_func(nullptr, op);
  return;
}

void submit_exists(ClientOp *op) {
  op->proxy->execute_exists(&op->zk_payload.exists);
  submit_operation_cont_func(nullptr, op);
  return;
}

void submit_get_children(ClientOp *op) {
  op->proxy->execute_get_children(op, &op->zk_payload.get_children);
  submit_operation_cont_func(nullptr, op);
  return;
}

// data is the data we received during the delete round.
void submit_create_and_rename_child(ClientOp *op,
                                    uint8_t *data,
                                    seq_req_t *seq_reqs) {
  // send the create node to the name's node
  auto *replies = new std::vector<size_t>;
  auto *returned_to_client = new bool;
  *returned_to_client = false;
  replies->resize(2); // two shards for this round

  create_node_t create_node;
  strcpy(create_node.name, op->zk_payload.rename_node.to);
  create_node.version = *reinterpret_cast<int32_t *>(data);
  memcpy(create_node.data, data+sizeof(int32_t), MAX_ZNODE_DATA);
  auto cshard = op->c->znode_to_shard_idx(create_node.name);
  create_node.seqnum = consume_seqnum(&seq_reqs[cshard]);

  send_req_to_shard(op, cshard, 0, OpType::kCreateNode, replies,
                    returned_to_client, seq_reqs, &create_node,
                    sizeof(create_node), one_shot_cont_func);

  rename_child_t rename_child;
  auto rshard =
      op->c->znode_to_shard_idx(get_parent(op->zk_payload.rename_node.from));

  rename_child.seqnum = consume_seqnum(&seq_reqs[rshard]);

  strcpy(rename_child.from, op->zk_payload.rename_node.from);
  strcpy(rename_child.to, op->zk_payload.rename_node.to);

  send_req_to_shard(op, rshard, 1, OpType::kRenameChild, replies,
                    returned_to_client, seq_reqs, &rename_child,
                    sizeof(rename_child), one_shot_cont_func);
}

// delete seems to be working
// need to now send the create and rename child
void delete_and_rename_node_cont_func(void *, void *_t) {
  auto *tag = reinterpret_cast<Tag *>(_t);

  auto &replies = *(tag->replies);
  replies[tag->my_shard_idx]++;

  if (!*tag->returned_to_client) {
    // make function
    bool got_quorum = true;
    for (size_t shard_reps : replies) {
      if (shard_reps < 2)
        got_quorum = false;
    }
    if (got_quorum) {
      fmt_rt_assert(tag->resp_msgbuf.get_data_size() == MAX_ZNODE_DATA,
                    "delete reply not right size got %zu wanted %zu\n",
                    tag->resp_msgbuf.get_data_size(), MAX_ZNODE_DATA);
      submit_create_and_rename_child(tag->op, tag->resp_msgbuf.buf,
                                     tag->seq_reqs);

      *tag->returned_to_client = true;
    }
  }

  for (size_t reps : replies) {
    if (reps != kZKReplicationFactor) {
      tag->op->proxy->tag_pool.free(tag);
      return;
    }
  }
  delete tag->replies;
  delete tag->returned_to_client;
  tag->op->proxy->tag_pool.free(tag);
}

void submit_rename_node(ClientOp *op) {
  op->proxy->execute_rename_node(
      reinterpret_cast<rename_node_t *>(&op->zk_payload));
  submit_operation_cont_func(nullptr, op);
  return;
}

void submit_delete_node(ClientOp *op) {
  op->proxy->execute_delete_node(
      reinterpret_cast<delete_node_t *>(&op->zk_payload));
  submit_operation_cont_func(nullptr, op);
  return;
}

// Currently a dummy implementation: just call continuations
// for each operation in batch
inline void
ClientOp::submit_operation(void) {
  switch (reqtype) {
    case ReqType::kCreateZNode: { submit_create_node(this); break; }
    case ReqType::kRenameZNode: { submit_rename_node(this); break; }
    case ReqType::kDeleteZNode: { submit_delete_node(this); break; }
    case ReqType::kWriteZNode:  { submit_write_node(this); break; }
    case ReqType::kReadZNode:   { submit_read_node(this); break; }
    case ReqType::kExists:      { submit_exists(this); break; }
    case ReqType::kGetChildren: { submit_get_children(this); break; }
    case ReqType::kDeleteClientConnection: {
      submit_delete_client_connection(this); break; }
    case ReqType::kServerDeleteClientConnection: {
      submit_server_delete_client_connection(this); break; }
    default: {
      erpc::rt_assert(false, "Unknown client operation\n");
      break;
    }
  }
}

void Proxy::complete_and_send_current_batch(void) {
  c->update_avg_batch_size(current_batch->batch_size());
  c->stat_batch_count++;
  current_batch->seq_req_id = seq_req_id++;

  appended_batch_map[current_batch->batch_id] =
      current_batch;

  for (size_t i = 0; i < current_batch->batch_size(); i++) {
    auto *op = current_batch->batch_client_ops[i];
    client_retx_in_progress_map[op->client_id][op->client_reqid] = op;
  }
  // ZooKeeper (RSMKeeper): Instead of requesting a seqnum, we just replicate the batch
  current_batch->replicate(EntryType::kSequenceNumberNoncontig);

}

// Callback for batch timeouts
// Should close current batch and open a new one
static inline void
batch_to_cb(void *_proxy) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  auto *proxy = reinterpret_cast<Proxy *>(_proxy);
  // Do nothing if the batch hasn't received any requests; else,
  // process batch.
  LOG_TIMERS("Thread %lu: Batch %p expired with size %zu!\n",
             proxy->c->thread_id,
             reinterpret_cast<void *>(proxy->current_batch),
             proxy->current_batch->batch_size());

  if (proxy->current_batch->batch_size() == 0) {
    LOG_TIMERS("Thread %lu: Batch size was 0 resetting.\n",
               proxy->c->thread_id);
    proxy->release_queued_ops();
    proxy->reset_batch_timer();

  } else {
    LOG_TIMERS("Thread %lu: Non-zero batch checking for releasable ops.\n",
               proxy->c->thread_id);
    // Check for ops that may have been enqueued from reordering
    proxy->release_queued_ops();
    proxy->complete_and_send_current_batch();

    proxy->create_new_batch();

#if MOCK_CLI
    proxy->add_dummy_client_ops();
#endif
  }

#if PRINT_TIMING
  printf("[%s] batch_to_cb [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

// Respond to client
inline void
submit_operation_cont_func(void *_context, void *_tag) {
  (void) _context;
  auto op = reinterpret_cast<ClientOp *>(_tag);
  Proxy *proxy = op->proxy;

  // if I'm not the leader I shouldn't reply
  if (!raft_is_leader(proxy->raft)) {
    LOG_SERVER("I am not the current leader. Not responding to client\n");
    return;
  }

  if (DEBUG_SEQ) print_seqreqs(op->seq_reqs, nsequence_spaces);

  if (!raft_is_leader(proxy->raft)) {
    // I am no longer the leader for a seqnum that I requested, don't do anything
    return;
  }

  op->respond_to_client();
}
// Respond to the client
inline erpc::MsgBuffer *
ClientOp::populate_client_response() {
  client_payload_t *payload = nullptr;
  if (reqtype != ReqType::kGetChildren) {
    proxy->c->rpc->resize_msg_buffer(
        &req_handle->pre_resp_msgbuf, client_payload_size(nsequence_spaces));
    payload = reinterpret_cast<client_payload_t *>(
        req_handle->pre_resp_msgbuf.buf);
    payload->proxy_id = proxy_id;
    payload->client_id = client_id;
    payload->client_reqid = client_reqid;

    erpc::rt_assert(raft_is_leader(batch->proxy->raft),
                    "was not the leader when responding to client\n");
    payload->not_leader = !raft_is_leader(batch->proxy->raft);
    erpc::rt_assert(payload->not_leader == 0,
                    "claiming I'm not the leader while responding to an op\n");
    payload->zk_payload = zk_payload;
    return &req_handle->pre_resp_msgbuf;
  } else {
    // this is a get_children response which can be large, need dyn_resp
    erpc::MsgBuffer &resp_msgbuf = req_handle->dyn_resp_msgbuf;
    // op_buf_size set at service response time
    size_t msg_size =
        client_payload_size(nsequence_spaces) + op_buf_size - 1;
    if (op_buf_size > sizeof(zk_payload_t)) {
      msg_size += op_buf_size - sizeof(zk_payload_t);
    }

    fmt_rt_assert(msg_size < kMaxMsgSize,
                  "Size of get children response too large %zu. "
                  "Max eRPC msg size is %zu.\n", msg_size, kMaxMsgSize);
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(msg_size);

    payload = reinterpret_cast<client_payload_t *>(resp_msgbuf.buf);
    payload->proxy_id = proxy_id;
    payload->client_id = client_id;
    payload->client_reqid = client_reqid;

    erpc::rt_assert(raft_is_leader(batch->proxy->raft),
                    "was not the leader when responding to client\n");
    payload->not_leader = !raft_is_leader(batch->proxy->raft);
    erpc::rt_assert(payload->not_leader == 0,
                    "claiming I'm not the leader while responding to an op\n");
    memcpy(static_cast<void *>(&payload->zk_payload), op_buf, op_buf_size);
    return &resp_msgbuf;
  }
}

inline void
ClientOp::respond_to_client() {
#if MOCK_CLI
  mock_respond_to_client();
#else

  proxy->client_retx_in_progress_map[client_id].erase(client_reqid);

  proxy->client_retx_done_map[client_id][client_reqid] = this;

  if (!has_handle) {
    // should this be in below check?
    // add to done map and remove from the in_progress map
    LOG_THREAD("no handle adding client_id %d op %lu from to done_map\n",
               client_id, client_reqid);

    proxy->client_retx_done_map[client_id][client_reqid] = this;
    return;
  }

  proxy->ops_with_handles[client_id].erase(client_reqid);

  erpc::MsgBuffer *resp_msgbuf = populate_client_response();
  erpc::rt_assert(resp_msgbuf->get_data_size() > 0,
                  "get_children Data size was 0 in client response\n");
  batch->c->rpc->enqueue_response(req_handle, resp_msgbuf);

  batch->completed_ops++;

  req_handle = nullptr;
  has_handle = false;

  batch->c->stat_resp_tx_tot++;
#endif
}

/**
 * This function creates a create_session and connect request, and waits
 * for the connection to be established.
 *
 * @param c
 * @param ip the ip to connect to
 * @param remote_tid the remote thread's id
 * @param machine_idx the index into the c->session_num_vec
 * @return 0 on success, fails otherwise.
 */
inline int
connect_and_store_session(WorkerContext *c, std::string ip, int remote_tid,
                          MachineIdx machine_idx) {
  auto idx = static_cast<uint8_t>(machine_idx);
  std::string uri;
  std::string port = ":31850";
  int session_num;

  uri = ip + port;

  session_num = c->rpc->create_session(uri, remote_tid);

  erpc::rt_assert(session_num >= 0, "Failed to create session");

  if (machine_idx != MachineIdx::CLIENT) {
    while (!c->rpc->is_connected(session_num) && !force_quit) {
      c->rpc->run_event_loop_once();
    }
  }

  c->session_num_vec.at(idx) = session_num;
  LOG_INFO("Connected to %s with session num: %d\n", ip.c_str(), session_num);

  return 0;
}

/**
 * This function creates a create_session and connect request, and waits
 * for the connection to be established, while continue normal operation
 * by calling raft_periodic
 *
 * @param c
 * @param ip the ip to connect to
 * @param remote_tid the remote thread's id
 * @param machine_idx the index into the c->session_num_vec
 * @return 0 on success, fails otherwise.
 */
inline int
connect_and_store_session(WorkerContext *c, std::string ip, int remote_tid,
                          MachineIdx machine_idx, Proxy *p) {
  auto idx = static_cast<uint8_t>(machine_idx);
  std::string uri;
  std::string port = ":31850";
  int session_num;

  uri = ip + port;

  session_num = c->rpc->create_session(uri, remote_tid);

  erpc::rt_assert(session_num >= 0, "Failed to create session");

  // why don't we wait for CLIENT?
  if (machine_idx != MachineIdx::CLIENT) {
    while (!c->rpc->is_connected(session_num) && !force_quit) {
      p->call_raft_periodic(1);
      c->rpc->run_event_loop_once();
    }
  }

  c->session_num_vec.at(idx) = session_num;
  LOG_INFO("Connected to %s with session num: %d\n", ip.c_str(), session_num);

  return 0;
}

// Connect to Rpc endpoints and push session numbers onto session vector
// for future use
int
connect_to_zk_machine(WorkerContext *c,
                      size_t idx,
                      size_t rep_num,
                      size_t sv_i) {
  std::string uri;
  std::string port = ":31850";
  int session_num;

  uri = c->zk_ips[idx] + port;

  for (uint8_t thread = 0; thread < N_ZKTHREADS; thread++) {
    printf("Connecting with uri %s, remote_tid %d\n", uri.c_str(), thread);

    session_num = c->rpc->create_session(uri, thread);

    erpc::rt_assert(session_num >= 0, "Failed to create session");

    while (!c->rpc->is_connected(session_num) && !force_quit) {
      c->rpc->run_event_loop_once();
    }

    c->zk_session_nums[sv_i + thread][rep_num] = session_num;
    printf("Connected to sv_i %zu thread %d  rep_num %zu with session num: %d\n",
           sv_i,
           thread,
           rep_num,
           session_num);
  }
  fflush(stdout);

  return 0;
}

// n_corfu_servers is the number of logical servers (threads)
void
establish_zk_connections(WorkerContext *c) {
  printf(
      "Establishing zk_ips size %lu N_ZKTHREADS %d repfac %d %lu ZooKeeper connections... \n",
      c->zk_ips.size(),
      N_ZKTHREADS,
      kZKReplicationFactor,
      (c->zk_ips.size() * N_ZKTHREADS));
  // one outer vector with nested vectors as shard connections
  c->zk_session_nums.resize(
      (c->zk_ips.size() * N_ZKTHREADS) / kZKReplicationFactor);

  for (size_t i = 0; i < c->zk_session_nums.size(); i++)
    c->zk_session_nums[i].resize(kZKReplicationFactor);

  // size_t rep_num = 0;
  // size_t j = 0;

  // for each shard
  // for (size_t i = 0; i < c->zk_ips.size(); i++) {
  //   // connect_to_zk_machine(c, i, rep_num, j);

  //   // fmt_rt_assert(c->zk_session_nums.size() == nsequence_spaces,
  //   //               "nshards %zu != %zu nsequence_spaces\n",
  //   //               c->zk_session_nums.size(), nsequence_spaces);
  //   rep_num = (rep_num + 1) % kZKReplicationFactor;
  //   if (rep_num == 0) j += N_ZKTHREADS;
  // }

  printf("...done establishing ZooKeeper connections.\n");
}

/**
 * This function creates a create_session and connect request, but does not
 * wait for it to be connected nor call run_event_loop.
 *
 * @param c
 * @param ip the ip to connect to
 * @param remote_tid the remote thread's id
 * @param machine_idx the index into the c->session_num_vec
 * @return 0 on success, fails otherwise.
 */
inline int
connect_and_store_session_async(WorkerContext *c, std::string ip,
                                int remote_tid,
                                MachineIdx machine_idx) {
  uint8_t idx = static_cast<uint8_t>(machine_idx);
  std::string uri;
  std::string port = ":31850";
  int session_num;

  uri = ip + port;
  LOG_INFO("Asynchronously connecting with uri %s, remote_tid %d\n",
           uri.c_str(), remote_tid);

  session_num = c->rpc->create_session(uri, remote_tid);
  erpc::rt_assert(session_num >= 0, "Failed to create session");

  c->session_num_vec.at(idx) = session_num;
  return 0;
}

// Connect to dependency trackers, sequencer. If thread 0, connect to your
// garbage collection buddy
inline void
establish_cats_connections(WorkerContext *c, int remote_tid) {
  printf("\nEstablishing CATS connections...\n");

  printf("Connecting to Rpcid %d on machines:", remote_tid);
#if !MOCK_SEQ
  printf("\nsequencer: ");
  // connect_and_store_session(c, seq_ip, remote_tid, MachineIdx::SEQ);
#endif

  // If this is the 0th thread, connect to the 0th thread on the neighbor
  if (c->thread_id == 0 && !FLAGS_no_gc) {
    LOG_INFO("next proxy0: \n");
    connect_and_store_session(c, nextproxy0_ip, 0, MachineIdx::NEXTPX0);
    LOG_INFO("next proxy1: \n");
    connect_and_store_session(c, nextproxy1_ip, 0, MachineIdx::NEXTPX1);
    LOG_INFO("next proxy2: \n");
    connect_and_store_session(c, nextproxy2_ip, 0, MachineIdx::NEXTPX2);
  }

  printf("... established CATS connections\n");
  fflush(stdout);
}

inline void
establish_raft_connections(WorkerContext *c, size_t remote_tid) {
  printf("Establishing Raft connections... Replica 1 then 2\n");
  connect_and_store_session(c, replica_1_ip, remote_tid, MachineIdx::REPLICA_1);
  connect_and_store_session(c, replica_2_ip, remote_tid, MachineIdx::REPLICA_2);
  printf("... established Raft connections\n");
  fflush(stdout);
}

// Create proxy object and place in vector
inline void
create_logical_proxies(WorkerContext *c) {
  printf("Creating logical proxies...\n");
  Proxy *p;

  if (my_raft_id == 0) { // leaders
    p = new Proxy(c, true, FLAGS_proxy_id_start + c->thread_id);
    printf("creating proxy leader with proxy id %lu\n",
           FLAGS_proxy_id_start + c->thread_id);
    fflush(stdout);
    c->proxies[FLAGS_proxy_id_start + c->thread_id] = p;
  } else { // followers
    p = new Proxy(c, false, FLAGS_proxy_id_start + c->thread_id);
    printf("creating proxy follower with proxy id %lu\n",
           FLAGS_proxy_id_start + c->thread_id);
    fflush(stdout);
    c->proxies[FLAGS_proxy_id_start + c->thread_id] = p;
  }

  fflush(stdout);
}

// Threads' main loop:
// - connect to seq, backends if applicable
// - create data structures for each logical proxy (leader or follower)
// - receive client connections and establish connections back
void
thread_func(size_t tid, erpc::Nexus *nexus, app_stats_t *app_stats) {
  WorkerContext c;
  printf("before vec at tid\n");
  fflush(stdout);
  context_vector->at(tid) = &c;

  c.app_stats = app_stats;
  c.thread_id = tid;
  for (size_t i = 0; i < nsequence_spaces; i++) {
    c.received_ms_seqnums[i]->thread_id = tid;
  }
  c.nconnections = 0;

  // make a list of all the corfu ips
  boost::split(c.zk_ips, FLAGS_zk_ips, boost::is_any_of(","));

#if !NO_ERPC
  int remote_tid = tid % N_SEQTHREADS;

  // Create RPC endpoint
  printf("Creating RPC endpoint for tid %zu, phyport %zu...\n",
         tid, tid % NPHY_PORTS);
  erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c),
                                  static_cast<uint8_t>(tid),
                                  sm_handler, tid % NPHY_PORTS);
  printf("... created RPC endpoint\n");

  rpc.set_pre_resp_msgbuf_size(MAX_PRE_RESP_MSGBUF);

  printf("max message size %zu\n", c.rpc->get_max_msg_size());

  rpc.retry_connect_on_invalid_rpc_id = true;
  printf("Done creating RPC endpoint\n");

  c.rpc = &rpc;
  c.allocate_gc_mbufs();

  create_logical_proxies(&c);

  establish_cats_connections(&c, remote_tid);

  // store client ips and nclient_threads
  boost::split(c.client_ips, FLAGS_client_ips, boost::is_any_of(","));
  c.nclient_threads = FLAGS_nclient_threads;

  // establish_zk_connections(&c);

  // establish Raft connections,
  // ASSUMPTION: *** our peers have the same thread_id ***
  establish_raft_connections(&c, c.thread_id);

  if (PLOT_RECOVERY) {
    printf("\nclient: ");
    connect_and_store_session(&c, client_ip, 0, MachineIdx::CLIENT);
  }

  // it seems number of proxy groups per thread == 1 is the best
  // need unique proxy_ids for the sequencer amo map
  // 1 proxy per thread simplifies things
  erpc::rt_assert(c.proxies.size() == 1, "proxies size not 1\n");

  // check which proxies are supposed to be the leader, then try to become the leader
  // was only change, did not test
  for (auto &el : c.proxies) {
    Proxy *proxy = el.second;
    raft_set_election_timeout(proxy->raft,
                              kRaftElectionTimeout);
    if (proxy->am_leader) {

      printf("first call to raft_periodic\n");
      // guarantees this one tries to become the leader
      raft_periodic(proxy->raft, kRaftElectionTimeout * 2 + 1);
      proxy->raft_periodic_tsc = erpc::rdtsc();
    }

  }

  // connect to client to be able to send watch notifications
  for (auto pair : c.proxies) {
    Proxy *p = pair.second;
    p->connect_to_clients();
  }

#else
  (void)nexus;
  c.rpc = nullptr;
#endif

#if MOCK_CLI
  c.proxies[0]->add_dummy_client_ops();

  struct timespec exp_t0;
  clock_gettime(CLOCK_REALTIME, &exp_t0);
#endif

  // The main loop!
  printf("\nStarting main loop in thread %zu...\n", tid);

  if (kUtilPrinting) {
    c.init_util_timers();
  }

  while (likely(!force_quit)) {
    if (unlikely(c.in_recovery)) {
      finish_recovery(&c);
    }
    c.check_timers();

#if MOCK_CLI
    if (unlikely(erpc::sec_since(exp_t0) > 20)) {
        break;
    }
#endif

    // we need to call raft periodic for every single Proxy (raft instance)
    // on this thread...
    // ASSUMPTION: proxy ids are consecutive
    std::string start;
    if (PRINT_TIMING) start = erpc::get_formatted_time();

    for (auto &el : c.proxies) {
      Proxy *proxy = el.second;
      proxy->call_raft_periodic(0);
      raft_apply_all(proxy->raft);

      if (unlikely(raft_get_log_count(proxy->raft) >=
          FLAGS_max_log_size)) {
        proxy->proxy_snapshot();
      }
      proxy->check_client_connections();
      proxy->check_notification_queue();
    }

    if (PRINT_TIMING)
      printf("[%s] raft_periodic for all proxies [%s]\n", start.c_str(),
             erpc::get_formatted_time().c_str());

    if (!NO_ERPC) {
      if (PRINT_TIMING)
        printf("about to call run_event_loop [%s]\n",
               erpc::get_formatted_time().c_str());
      rpc.run_event_loop_once();
      if (PRINT_TIMING)
        printf("after call to run_event_loop [%s]\n",
               erpc::get_formatted_time().c_str());
    }

    for (auto &el : c.proxies) {
      Proxy *p = el.second;
      if (!raft_is_leader(p->raft)) {

        for (auto &i : p->ops_with_handles) {
          auto map = i.second;

          for (auto &j : map) {
            ClientOp *op = j.second;

            if (!op->has_handle) {
              p->ops_with_handles[op->client_id].erase(op->client_reqid);
              op->has_handle = false;
              continue;
            }
            erpc::rt_assert(op->has_handle,
                            "op did not have handle in has handle map\n");

            printf("[%s] pid: %u lost leadership at some point "
                   "with outstanding requests, return not leader\n",
                   erpc::get_formatted_time().c_str(), p->proxy_id);
            fflush(stdout);

            reply_not_leader(op->req_handle, p->proxy_id,
                             op->client_id, op->client_reqid, p);

            p->ops_with_handles[op->client_id].erase(op->client_reqid);
            op->has_handle = false;
          }
        }
      }
    }
  }

  printf("Thread %zu sizeof batch pool: %zu\n",
         tid,
         c.proxies[FLAGS_proxy_id_start]->batch_pool.pool.size());
  printf("Thread %zu sizeof client_op_pool: %zu\n",
         tid,
         c.proxies[FLAGS_proxy_id_start]->client_op_pool.pool.size());
  printf("Thread %zu sizeof tag pool: %zu\n",
         tid,
         c.proxies[FLAGS_proxy_id_start]->tag_pool.pool.size());

  printf("exiting\n");
}

inline void Proxy::call_raft_periodic(size_t nms) {
  // raft_periodic() uses msec_elapsed for only request and election timeouts.
  // msec_elapsed is in integer milliseconds which does not work for us because
  // we invoke raft_periodic() much work frequently. Instead, we accumulate
  // cycles over calls to raft_periodic().
  size_t cur_tsc = erpc::rdtsc();
  size_t msec_elapsed =
      (cur_tsc - raft_periodic_tsc) / cycles_per_msec;

  size_t ms = 0;

  if (msec_elapsed > 0) {
    raft_periodic_tsc = cur_tsc;
    ms = msec_elapsed;
  }
  // only call raft_periodic if the leader that is supposed
  // to be the leader already became the leader
  if (likely(am_leader || got_leader)) {
    if (ms >= nms)
      raft_periodic(raft, ms);
  }
}

///**
//1. Begin snapshotting with raft_begin_snapshot.
//2. Save the current membership details to the snapshot.
//3. Save the finite state machine to the snapshot.
//4. End snapshotting with raft_end_snapshot.
//5. When the send_snapshot callback fires, the user must propogate the snapshot to the peer.
//6. Once the peer has the snapshot, they call raft_begin_load_snapshot.
//7. Peer calls raft_add_node to add nodes as per the snapshot's membership info.
//8. Peer calls raft_node_set_voting to nodes as per the snapshot's membership info.
//9. Peer calls raft_node_set_active to nodes as per the snapshot's membership info.
//10. Finally, peer calls raft_node_set_active to nodes as per the snapshot's membership info.
//**/
void
Proxy::proxy_snapshot() {
  // tell raft we are starting the snapshot
  erpc::rt_assert(raft_begin_snapshot(raft, 0) == 0,
                  "error trying to snapshot\n");

  // serialize and write proxy struct to file with boost
  std::string temp;
  temp = "/usr/local/snapshot" + c->my_ip + std::to_string(c->thread_id) +
      std::to_string(proxy_id);

  last_included_index = raft_get_commit_idx(raft);
  last_included_term = raft_get_entry_from_idx(raft, last_included_index)->term;

  uint64_t tf_start;
  (void) tf_start;
  uint64_t t_start __attribute__((unused));
  {
    // ofs must be closed before ifs, putting it in its own scope ensures that
    std::ofstream ofs(temp);

    boost::archive::text_oarchive oa(ofs);
    // writes the proxy to the oa archive which goes to the output filestream "filename"

    t_start = erpc::rdtsc();
    oa << this;
    tf_start = erpc::rdtsc();
  }
  LOG_INFO("%s Serialization took %Lf us flush time %Lf for %zu znodes\n",
           p_string().c_str(), cycles_to_usec(erpc::rdtsc() - t_start),
           cycles_to_usec(erpc::rdtsc() - tf_start), znodes.size());

  // tell raft we are done snapshotting
  raft_end_snapshot(raft);
}

// Launch proxy worker threads with configured numbers of leaders/followers
inline void
launch_threads(size_t nthreads, erpc::Nexus *nexus) {
  // Spin up the requisite number of sequencer threads
  std::vector<std::thread> threads(nthreads);
  context_vector = new std::vector<WorkerContext *>(nthreads);
  auto *app_stats = new app_stats_t[nthreads];

  printf("nthreads: %zu\n", nthreads);
  fflush(stdout);

  for (size_t i = 0; i < nthreads; i++) {
    printf("Setting up thread %zu on numa_node %zu...\n", i, numa_node);
    fflush(stdout);
    threads[i] = std::thread(thread_func, i, nexus,
                             app_stats);
    erpc::bind_to_core(
        threads[i],
        nthreads > 8 ? i % 2 : numa_node,
        i);
  }

  printf("joining threads\n");
  fflush(stdout);
  for (auto &thread : threads) thread.join();

  printf("deleting app_stats... ");
  fflush(stdout);
  printf("done deleting app_stats\n");
  fflush(stdout);
}

static inline void
signal_handler(int signum) {
  fflush(stdout);
  if (signum == SIGINT || signum == SIGTERM) {
    printf("\n\nSignal %d received, preparing to exit...\n",
           signum);
    erpc::rt_assert(false, "exit");
  }
  if (signum == SIGSEGV || signum == SIGABRT) {

    void *array[10];
    int size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    // print out all the frames to stderr
    if (signum == SIGSEGV) {
      fprintf(stderr, "SEGFAULT: signal %d:\n", signum);
    } else if (signum == SIGABRT) {
      fprintf(stderr, "SEGFAULT: signal %d:\n", signum);
    }

    backtrace_symbols_fd(array, size, STDERR_FILENO);
    erpc::rt_assert(false, "exit");
  }
  if (signum == SIGBUS) {
    fflush(stdout);

    void *array[10];
    int size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    // print out all the frames to stderr
    fprintf(stderr, "SIGBUS: signal %d:\n", signum);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    erpc::rt_assert(false, "exit");
  }
  fflush(stdout);
}

int
main(int argc, char **argv) {
  freq_ghz = erpc::measure_rdtsc_freq();

  printf("Size of proxy struct: %zu\n", sizeof(Proxy));
  printf("Size of client op struct: %zu\n", sizeof(ClientOp));
  printf("Size of batch struct: %zu\n", sizeof(Batch));
  printf("Size of tag struct: %zu\n", sizeof(Tag));

  printf("app_stats_t size: %zu\n", sizeof(app_stats_t));
  fflush(stdout);
  erpc::rt_assert(sizeof(app_stats_t) == 64, "app_stats_t is wrong size!");

  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGSEGV, signal_handler);

  printf("Parsing command line args...\n");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  my_ip = FLAGS_my_ip;
  nextproxy0_ip = FLAGS_nextproxy0_ip;
  nextproxy1_ip = FLAGS_nextproxy1_ip;
  nextproxy2_ip = FLAGS_nextproxy2_ip;

  seq_ip = FLAGS_seq_ip;
  backupseq_ip = FLAGS_backupseq_ip;
  replica_1_ip = FLAGS_replica_1_ip;
  replica_2_ip = FLAGS_replica_2_ip;

  client_ip = FLAGS_client_ip;
  erpc::rt_assert(client_ip != "" || !PLOT_RECOVERY,
                  "Need a client ip to plot recovery!");

  my_raft_id = FLAGS_my_raft_id;
  replica_1_raft_id = FLAGS_replica_1_raft_id;
  replica_2_raft_id = FLAGS_replica_2_raft_id;

  nsequence_spaces = FLAGS_nsequence_spaces;
  printf("FLAGS_nsequence_spaces is %zu nsequence_spaces %zu\n",
         FLAGS_nsequence_spaces, nsequence_spaces);

  erpc::rt_assert(FLAGS_nthreads == 1, "nthreads must be 1 for proxykeeper\n");

  if (nextproxy0_ip.empty()) {
    printf("nextproxy is empty setting no_gc to true.\n");
    FLAGS_no_gc = true;
  } else {
    printf("nextproxy is not empty.\n");
  }

#if !NO_ERPC
  std::string uri = my_ip + ":31850";
  printf("Creating nexus object for URI %s...\n", uri.c_str());
  erpc::Nexus nexus(uri, numa_node, 0);

  // Normal operation handlers
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kCreateZNode),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kRenameZNode),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kDeleteZNode),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kWriteZNode),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kReadZNode),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kExists),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kGetChildren),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kDeleteClientConnection),
      client_op_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kClientHeartbeat),
      client_heartbeat_handler);

  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kDoGarbageCollection),
      garbage_collection_handler);

  // raft handlers
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kRequestVote),
      requestvote_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kAppendEntries),
      appendentries_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kAppendEntriesResponse),
      appendentries_response_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kSendSnapshot),
      send_snapshot_handler);

  printf("kAppendEntries %d kAppendEntriesResponse %d\n",
         static_cast<uint8_t>(ReqType::kAppendEntries),
         static_cast<uint8_t>(ReqType::kAppendEntriesResponse));

  // Recovery handlers
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kBackupReady),
      backup_ready_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kGetBitmap),
      request_bitmap_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kGetDependencies),
      request_dependencies_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kAssignSeqnumToHole),
      fill_hole_handler);
  nexus.register_req_func(
      static_cast<uint8_t>(ReqType::kRecoveryComplete),
      recovery_complete_handler);

  printf("Launching %zu threads...\n", FLAGS_nthreads);
  launch_threads(FLAGS_nthreads, &nexus);

#else
  erpc::Nexus *nexus = nullptr;

  printf("Launching threads...\n");
  launch_threads(FLAGS_nthreads, nexus);
#endif

  free(context_vector);

  printf("Exiting!\n");
  return 0;
}
