extern "C" {
#include <raft/raft.h>
#include <raft/raft_private.h>
#include <raft/raft_log.h>
}

#include <assert.h>
#include <sys/stat.h>

#include "proxy.h"

#define FALLING_BEHIND_WARNING 200000
#define MAX_AE_SIZE 20000
#define N_ENTRIES_FAST_PATH 50

// for debugging, mimics an internal willemt/Raft struct...
// https://github.com/willemt/raft
typedef struct {
  /* size of array */
  raft_index_t size;

  /* the amount of elements in the array */
  raft_index_t count;

  /* position of the queue */
  raft_index_t front, back;

  /* we compact the log, and thus need to increment the Base Log Index */
  raft_index_t base;

  raft_entry_t *entries;

  /* callbacks */
  raft_cbs_t *cb;
  void *raft;
} my_log_private_t;

typedef struct {
  /* Persistent state: */

  /* the server's best guess of what the current term is
   * starts at zero */
  raft_term_t current_term;

  /* The candidate the server voted for in its current term,
   * or Nil if it hasn't voted for any.  */
  raft_node_id_t voted_for;

  /* the log which is replicated */
  void *log;

  /* Volatile state: */

  /* idx of highest log entry known to be committed */
  raft_index_t commit_idx;

  /* idx of highest log entry applied to state machine */
  raft_index_t last_applied_idx;

  /* follower/leader/candidate indicator */
  int state;

  /* amount of time left till timeout */
  int timeout_elapsed;

  raft_node_t *nodes;
  int num_nodes;

  int election_timeout;
  int election_timeout_rand;
  int request_timeout;

  /* what this node thinks is the node ID of the current leader, or NULL if
   * there isn't a known current leader. */
  raft_node_t *current_leader;

  /* callbacks */
  raft_cbs_t cb;
  void *udata;

  /* my node ID */
  raft_node_t *node;

  /* the log which has a voting cfg change, otherwise -1 */
  raft_index_t voting_cfg_change_log_idx;

  /* Our membership with the cluster is confirmed (ie. configuration log was
   * committed) */
  int connected;

  int snapshot_in_progress;
  int snapshot_flags;

  /* Last compacted snapshot */
  raft_index_t snapshot_last_idx;
  raft_term_t snapshot_last_term;

  /* Previous index/term values stored during snapshot,
   * which are restored if the operation is cancelled.
   */
  raft_index_t saved_snapshot_last_idx;
  raft_term_t saved_snapshot_last_term;
} my_raft_server_private_t;

typedef struct replica_data replica_data_t;

// With eRPC, there is currently no way for an RPC server to access connection
// data for a request, so the client's Raft node ID is included in the request.
struct app_requestvote_t {
  int node_id;
  uint16_t proxy_id; // which proxy group this is destined for
  msg_requestvote_t msg_rv;
};

typedef struct send_snapshot {
  int node_id;
  uint16_t proxy_id;
  size_t rpc_n;
  raft_index_t last_included_idx;
  raft_term_t last_included_term;
  size_t total_snapshot_size;
  size_t size_before_bm;
  size_t total_msg_size;
  size_t msg_n;
  size_t offset;
  size_t length;
  char *snapshot_block;
} send_snapshot_t;

typedef struct snapshot_response {
  raft_index_t next_index;
  size_t rpc_n;
  bool done = false;
  uint16_t node_id;
} snapshot_response_t;

Proxy *raft_get_proxy(void *udata) {
  auto *my_data = static_cast<replica_data_t *>(udata);
  auto *wc = my_data->wc;
  return wc->proxies[my_data->pid];

}

// Most serialization code for appendentries from eRPC
// https://github.com/erpc-io/eRPC/blob/master/apps/smr/appendentries.h

// With eRPC, there is currently no way for an RPC server to access connection
// data for a request, so the client's Raft node ID is included in the request.
struct app_appendentries_t {
  int node_id;  // Node ID of the sender
  uint16_t proxy_id;
  msg_appendentries_t msg_ae;
  // If ae.n_entries > 0, the msg_entry_t structs are serialized here. Each
  // msg_entry_t struct's buf is placed immediately after the struct.

  // Serialize the ingredients of an app_appendentries_t into a network buffer
  static inline void
  serialize(erpc::MsgBuffer &req_msgbuf, int node_id, int proxy_id,
            msg_appendentries_t *msg_ae) {
    uint8_t *buf = req_msgbuf.buf;
    auto *srlz = reinterpret_cast<app_appendentries_t *>(req_msgbuf.buf);

    // Copy the whole-message header
    srlz->node_id = node_id;
    srlz->proxy_id = proxy_id;
    srlz->msg_ae = *msg_ae;
    srlz->msg_ae.entries = nullptr;  // Was local pointer
    buf += sizeof(app_appendentries_t);

    // Serialize each entry in the message
    for (size_t i = 0; i < static_cast<size_t>(msg_ae->n_entries); i++) {
      // Copy the entry header
      *reinterpret_cast<msg_entry_t *>(buf) = msg_ae->entries[i];
      reinterpret_cast<msg_entry_t *>(buf)->data.buf = nullptr;  // Local ptr
      buf += sizeof(msg_entry_t);

      // Copy the entry data
      rte_memcpy(buf, msg_ae->entries[i].data.buf,
                 msg_ae->entries[i].data.len);
      buf += msg_ae->entries[i].data.len;
    }

    assert(buf == req_msgbuf.buf + req_msgbuf.get_data_size());
  }

  static constexpr size_t kStaticMsgEntryArrSize = 16;

  // Unpack an appendentries request message received at the server.
  //  * The buffers for entries the unpacked message come from the mempool.
  //  * The entries array for the unpacked message is dynamically allocated
  //    if there are too many entries. Caller must free if so.
  static inline void unpack(const erpc::MsgBuffer *req_msgbuf,
                            msg_entry_t *static_msg_entry_arr) {
    uint8_t *buf = req_msgbuf->buf;
    auto *ae_req = reinterpret_cast<app_appendentries_t *>(buf);
    msg_appendentries_t &msg_ae = ae_req->msg_ae;
    assert(msg_ae.entries == nullptr);

    auto n_entries = static_cast<size_t>(msg_ae.n_entries);
    bool is_keepalive = (n_entries == 0);

    if (!is_keepalive) {
      // Non-keepalive appendentries requests contain app-defined log entries
      buf += sizeof(app_appendentries_t);
      msg_ae.entries = n_entries <= kStaticMsgEntryArrSize
                       ? static_msg_entry_arr
                       : new msg_entry_t[n_entries];

      // Invariant: buf points to a msg_entry_t, followed by its buffer
      for (size_t i = 0; i < n_entries; i++) {
        msg_ae.entries[i] = *(reinterpret_cast<msg_entry_t *>(buf));
        buf += sizeof(msg_entry_t);

        assert(msg_ae.entries[i].data.buf == nullptr);

        msg_ae.entries[i].data.buf = malloc(msg_ae.entries[i].data.len);
        rte_memcpy(msg_ae.entries[i].data.buf, buf, msg_ae.entries[i].data.len);
        buf += msg_ae.entries[i].data.len;

      }

      assert(buf == req_msgbuf->buf + req_msgbuf->get_data_size());
    }
  }
};

void __raft_log(raft_server_t *raft, raft_node_t *node, void *udata,
                const char *buf);

inline void requestvote_cont(void *, void *); // fwd decl

// Return a string representation of a requestvote request message
static std::string msg_requestvote_string(msg_requestvote_t *msg_rv) {
  std::ostringstream ret;
  ret << "[candidate_id " << msg_rv->candidate_id << ", "
      << "last log idx " << std::to_string(msg_rv->last_log_idx) << ", "
      << "last log term " << std::to_string(msg_rv->last_log_term) << ", "
      << "term " << std::to_string(msg_rv->term) << "]";
  return ret.str();
}

// Return a string representation of a requestvote response request message
static std::string msg_requestvote_response_string(
    msg_requestvote_response_t *msg_rv_resp) {
  std::ostringstream ret;
  ret << "[term " << std::to_string(msg_rv_resp->term) << ", "
      << "vote granted " << std::to_string(msg_rv_resp->vote_granted) << "]";
  return ret.str();
}

// Call back comments and signatures mostly taken from some version of
// willemt/Raft https://github.com/willemt/raft/blob/master/include/raft.h

/** Callback for sending request vote messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node's ID that we are sending this message to
 * @param[in] msg The request vote message to be sent
 * @return 0 on success */
inline int __send_requestvote(
    raft_server_t *raft,
    void *user_data, // user data is MY data!
    raft_node_t *node, // the node we want to send to
    msg_requestvote_t *msg
) {
  // mark raft and user data unused
  (void) raft;
  (void) node;
  (void) user_data;

  // this proxy
  auto *my_data = static_cast<replica_data_t *>(user_data);
  auto *wc = my_data->wc;
  Proxy *proxy = wc->proxies[my_data->pid];

  LOG_RAFT("in __send_requestvote\n");

  // receiver data
  auto *data = static_cast<replica_data_t *>(raft_node_get_udata(node));

  // If not connected don't send
  if (!wc->rpc->is_connected(
      wc->session_num_vec[static_cast<int>(data->idx)])) {
    return 0;
  }

  // allocate a tag and buffers
  raft_tag_t *rv_tag = proxy->raft_tag_pool.alloc();

  rv_tag->req_msgbuf = proxy->c->rpc->alloc_msg_buffer_or_die(
      sizeof(app_requestvote_t));
  rv_tag->resp_msgbuf = proxy->c->rpc->alloc_msg_buffer_or_die(
      sizeof(app_requestvote_t));

  // fill in the tag
  rv_tag->node = node;
  rv_tag->proxy = proxy;

  // fill in request
  auto *rv_req = reinterpret_cast<app_requestvote_t *>(rv_tag->req_msgbuf.buf);
  rv_req->node_id = proxy->replica_data[my_raft_id].node_id;
  rv_req->msg_rv = *msg;

  rv_req->proxy_id = my_data->pid;

  LOG_ERROR("PID: %u sending requestvote to session number: %d \n\t%s\n",
         proxy->proxy_id,
         proxy->c->session_num_vec[static_cast<int>(data->idx)],
         msg_requestvote_string(&rv_req->msg_rv).c_str());
  fflush(stdout);

  // enqueue it to be sent
  proxy->c->rpc->enqueue_request(
      proxy->c->session_num_vec[static_cast<int>(data->idx)],
      static_cast<uint8_t>(ReqType::kRequestVote),
      &rv_tag->req_msgbuf, &rv_tag->resp_msgbuf,
      requestvote_cont, reinterpret_cast<void *>(rv_tag));
  proxy->c->stat_resp_tx_tot++;
  return 0;
}

inline void requestvote_handler(erpc::ReqHandle *req_handle, void *_context) {
  auto *wc = static_cast<WorkerContext *>(_context);

  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  assert(req_msgbuf->get_data_size() == sizeof(app_requestvote_t));

  auto *rv_req = reinterpret_cast<app_requestvote_t *>(req_msgbuf->buf);
  erpc::MsgBuffer &resp_msgbuf = req_handle->pre_resp_msgbuf;

  wc->rpc->resize_msg_buffer(&resp_msgbuf, sizeof(msg_requestvote_response_t));

  auto *rv_resp =
      reinterpret_cast<msg_requestvote_response_t *>(resp_msgbuf.buf);

  // rv_req->msg_rv is valid only for the duration of this handler, which is OK
  // as msg_requestvote_t does not contain any dynamically allocated members.
  Proxy *proxy = wc->proxies[rv_req->proxy_id];

  int was_leader = raft_is_leader(proxy->raft);

  LOG_ERROR("PID: %d received requestvote from %u, am I leader? %d\n",
           proxy->proxy_id, rv_req->node_id, was_leader);

  int e = raft_recv_requestvote(proxy->raft,
                                raft_get_node(proxy->raft, rv_req->node_id),
                                &rv_req->msg_rv, rv_resp);

  erpc::rt_assert(e == 0, "error receiving requestvote %s\n", strerror(e));

  LOG_ERROR("PID: %d sending requestvote response %s\n",
                 proxy->proxy_id,
                 msg_requestvote_response_string(rv_resp).c_str());

  wc->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);

  if (was_leader) {
    if (!raft_is_leader(proxy->raft)) {
      LOG_ERROR("I JUST LOST LEADERSHIP for %d\n", proxy->proxy_id);
      proxy->lose_leadership();
    }
  }
}

// request_vote continuation function
inline void requestvote_cont(void *, void *_tag) {
  auto *rv_tag = reinterpret_cast<raft_tag_t *>(_tag);
  Proxy *proxy =
      rv_tag->proxy;

  int was_leader = raft_is_leader(proxy->raft);

  auto *msg_rv_resp =
      reinterpret_cast<msg_requestvote_response_t *>(rv_tag->resp_msgbuf.buf);

  if (likely(rv_tag->resp_msgbuf.get_data_size() > 0)) {
    // The RPC was successful
    LOG_ERROR("PID: %u Received requestvote response: %s\n",
           proxy->proxy_id,
           msg_requestvote_response_string(msg_rv_resp).c_str());

    int e = raft_recv_requestvote_response(proxy->raft, rv_tag->node,
                                           msg_rv_resp);
    erpc::rt_assert(e == 0, "error receiving requestvote response %s\n",
                    strerror(e));  // XXX: Doc says: Shutdown if e != 0
  } else {
    // This is a continuation-with-failure
    printf("smr: Requestvote RPC failed to complete");
    fflush(stdout);
  }

  proxy->c->rpc->free_msg_buffer(rv_tag->req_msgbuf);
  proxy->c->rpc->free_msg_buffer(rv_tag->resp_msgbuf);
  proxy->raft_tag_pool.free(rv_tag);

  printf("received request vote response: was_leader: %d is_leader %d\n",
         was_leader, raft_is_leader(proxy->raft));
  fflush(stdout);

  if (!was_leader) {// if I am becoming the leader
    if (raft_is_leader(proxy->raft)) {
      LOG_ERROR("[%zu] I JUST GAINED LEADERSHIP for %d\n",
             proxy->c->thread_id, proxy->proxy_id);
      proxy->gain_leadership();
    }
  } else if (!raft_is_leader(proxy->raft)) {
    proxy->lose_leadership();
  }
}

void appendentries_cont(void *, void *);  // Fwd decl

/** Callback for sending append entries messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node's ID that we are sending this message to
 * @param[in] msg The appendentries message to be sent
 * @return 0 on success */
inline int __send_appendentries(
    raft_server_t *raft,
    void *user_data,
    raft_node_t *node,
    msg_appendentries_t *msg
) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  // mark raft and user data unused
  (void) raft;
  (void) node;
  (void) user_data;

  // this proxy
  auto *my_data = static_cast<replica_data_t *>(user_data);
  auto *wc = my_data->wc;
  Proxy *proxy = wc->proxies[my_data->pid];

  LOG_RAFT("in __send_appendentries\n");

  // receiver data
  auto *data = static_cast<replica_data_t *>(raft_node_get_udata(node));

  bool is_keepalive = (msg->n_entries == 0);
  // there has to be a better way to avoid "unused" when logging is off
  (void) is_keepalive;

  // might be nice to track this
  if (data->nOutstanding >= MAX_OUTSTANDING_AE) {
    LOG_WARN("[%zu] Too many outstanding append_entries (%d), not sending to"
             " node_id %d\n", wc->thread_id, data->nOutstanding, data->node_id);
    return 0;
  }

  LOG_RAFT_SMALL(
      "smr: Sending appendentries (%s) to node_id %d proxy_id %d n_entries %d"
      " [%s].\n",
      is_keepalive ? "keepalive" : "non-keepalive",
      data->node_id, data->pid, msg->n_entries,
      erpc::get_formatted_time().c_str());

  // If haven't received in a while and not connected don't send
  if (!wc->rpc->is_connected(
      wc->session_num_vec[static_cast<int>(data->idx)])) {
    LOG_RECOVERY("[%zu] __send_appendentries not connected to proxy sn %d\n",
                  wc->thread_id,
                  wc->session_num_vec[static_cast<int>(data->idx)]);
    return 0;
  }

  // Compute the request size. Keepalive appendentries requests do not have
  // a buffer, but they have an unused msg_entry_t.
  size_t req_size = sizeof(app_appendentries_t);
  for (size_t i = 0; i < static_cast<size_t>(msg->n_entries); i++) {
    // here we only want up to the i-1 entry
    // if the next entry makes this request too large, stop adding entries
    // was > proxy->c->rpc->get_max_msg_size() 2208 2.39M, 2207 49k
    if (req_size + sizeof(msg_entry_t) + msg->entries[i].data.len >
        MAX_AE_SIZE) {
      if (i != 0) {
        msg->n_entries = i;
        break;
      }
    }
    req_size += sizeof(msg_entry_t) + msg->entries[i].data.len;
  }

  auto *log = reinterpret_cast<my_log_private_t *>(
          (reinterpret_cast<raft_server_private_t *>(proxy->raft))->log);
  LOG_RAFT_SMALL("\t to node_id %d size of log %ld\n",
                 data->node_id, log->count);
  LOG_RAFT_SMALL("\t to node_id %d prev_log_idx %ld "
                 "n_entries %d appendentries size: %lu\n",
                 data->node_id, msg->prev_log_idx, msg->n_entries, req_size);

  if (unlikely(log->count - msg->prev_log_idx > FALLING_BEHIND_WARNING)) {
    LOG_WARN(
        "[%s] Thread %zu WARNING: REPLICA %d FELL OVER %d BEHIND! %ld BEHIND\n",
        erpc::get_formatted_time().c_str(), proxy->c->thread_id, data->node_id,
        FALLING_BEHIND_WARNING, log->count - msg->prev_log_idx);
  }


  raft_tag_t *rrt = proxy->raft_tag_pool.alloc();
  rrt->req_msgbuf = proxy->c->rpc->alloc_msg_buffer_or_die(req_size);
  rrt->resp_msgbuf =
      proxy->c->rpc->alloc_msg_buffer_or_die(
          sizeof(msg_appendentries_response_t));
  rrt->node = node;
  rrt->proxy = proxy;

  app_appendentries_t::serialize(rrt->req_msgbuf,
                                 proxy->replica_data[my_raft_id].node_id,
                                 proxy->proxy_id, msg);

  proxy->c->rpc->enqueue_request(
      proxy->c->session_num_vec[static_cast<int>(data->idx)],
      static_cast<uint8_t>(ReqType::kAppendEntries),
      &rrt->req_msgbuf, &rrt->resp_msgbuf,
      appendentries_cont, reinterpret_cast<void *>(rrt));

  proxy->c->ae_pkts += req_size / (1025);
  proxy->c->ae_bytes += req_size;
  data->nOutstanding++;

#if PRINT_TIMING
  printf("[%s] send_appendentries [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
  return 0;
}

inline void appendentries_response_cont(void *, void *_tag) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  auto *ae_tag = reinterpret_cast<raft_tag_t *>(_tag);
  Proxy *proxy =
      ae_tag->proxy;

  proxy->c->rpc->free_msg_buffer(ae_tag->req_msgbuf);
  proxy->c->rpc->free_msg_buffer(ae_tag->resp_msgbuf);
  proxy->raft_tag_pool.free(ae_tag);

#if PRINT_TIMING
  printf("[%s] appendentries_response_cont [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

// appendentries request format is like so:
// node ID, msg_appendentries_t, [{size, buf}]
inline void appendentries_handler(erpc::ReqHandle *req_handle, void *_context) {

#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif
  auto *wc = static_cast<WorkerContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();

  auto *ae_req = reinterpret_cast<app_appendentries_t *>(req_msgbuf->buf);
  Proxy *proxy = wc->proxies[ae_req->proxy_id];

  if (wc->in_recovery) {
    LOG_RECOVERY("[%zu] In appendentries_handler\n", wc->thread_id);
  }

  // I received an appendentries: this proxy got a leader
  if (unlikely(!proxy->got_leader)) {
    proxy->got_leader = true;
    LOG_ERROR("PID: %u just got the first leader\n", proxy->proxy_id);
  }

  auto node_id = ae_req->node_id;

  // Reconstruct an app_appendentries_t in req_msgbuf. The entry buffers the
  // unpacked message are long-lived (pool-allocated). The unpacker may choose
  // to not use static_msg_entry_arr for the unpacked entries, in which case
  // we free the dynamic memory later below.
  msg_entry_t static_msg_entry_arr[app_appendentries_t::kStaticMsgEntryArrSize];
  app_appendentries_t::unpack(req_msgbuf, static_msg_entry_arr);

  msg_appendentries_t msg_ae = ae_req->msg_ae;

  // send the blank response immediately
  erpc::MsgBuffer &resp_msgbuf = req_handle->pre_resp_msgbuf;
  proxy->c->rpc->resize_msg_buffer(&resp_msgbuf, 8);
  debug_print(DEBUG_RAFT, "responding to appendentries\n");
  proxy->c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);


  LOG_RAFT_SMALL(
      "smr: received appendentries from node_id %d for proxy_id %d "
      "prev_log_idx %ld n_entries %d  [%s]\n",
      node_id, proxy->proxy_id, msg_ae.prev_log_idx, msg_ae.n_entries,
      erpc::get_formatted_time().c_str());

  if (wc->in_recovery) {
    LOG_RECOVERY(
        "[%zu] smr: received appendentries from node_id %d for proxy_id %d "
        "prev_log_idx %ld n_entries %d  [%s]\n", wc->thread_id,
        node_id, proxy->proxy_id, msg_ae.prev_log_idx, msg_ae.n_entries,
        erpc::get_formatted_time().c_str());
  }

  raft_tag_t *rrt = proxy->raft_tag_pool.alloc();

  rrt->req_msgbuf = proxy->c->rpc->alloc_msg_buffer_or_die(
      AE_RESPONSE_SIZE(nsequence_spaces));
  rrt->resp_msgbuf = proxy->c->rpc->alloc_msg_buffer_or_die(8);
  rrt->node = raft_get_node(proxy->raft,
                            proxy->replica_data[my_raft_id].node_id);
  rrt->proxy = proxy;

  // make the response be of type ae_response_t which we will need when changing to rpc
  wc->rpc->resize_msg_buffer(&(rrt->req_msgbuf),
                             AE_RESPONSE_SIZE(nsequence_spaces));
  auto *response = reinterpret_cast<ae_response_t *>(rrt->req_msgbuf.buf);
  response->node_id = my_raft_id;
  response->proxy_id = proxy->proxy_id;
  for (size_t i = 0; i < nsequence_spaces; i++) {
    response->base_seqnums[i] = wc->received_ms_seqnums[i]->base_seqnum;
  }

  // Only the buffers for entries in the append
  int e = raft_recv_appendentries(
      proxy->raft, raft_get_node(proxy->raft, node_id), &msg_ae,
      &(response->ae_response));
  erpc::rt_assert(e == 0, "error receiving appendentries %s\n", strerror(e));

  if (msg_ae.entries != static_msg_entry_arr) delete[] msg_ae.entries;

  // if we have not yet connected to this proxy (which is possible) the session num will be 0!
  // this will then send it to the sequencer!
  // I think it safe to just return and never send the "response" rpc
  auto session_num =
      proxy->c->session_num_vec[static_cast<int>(proxy->replica_data[node_id].idx)];
  if (unlikely(session_num == 0)) {
    // we haven't connected to the proxy in this direction yet, for now we just drop
    return;
  }
  erpc::rt_assert(session_num != 0, "would have sent to session num 0!\n");

  proxy->c->rpc->enqueue_request(session_num,
                                 static_cast<uint8_t>(ReqType::kAppendEntriesResponse),
                                 &rrt->req_msgbuf, &rrt->resp_msgbuf,
                                 appendentries_response_cont,
                                 reinterpret_cast<void *>(rrt));
#if PRINT_TIMING
  printf("[%s] ae_handler [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

inline void
appendentries_response_handler(erpc::ReqHandle *req_handle, void *_context) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  auto start_time = erpc::get_formatted_time();
  auto *response =
      reinterpret_cast<ae_response_t *>(req_handle->get_req_msgbuf()->buf);
  auto *wc = reinterpret_cast<WorkerContext *>(_context);
  auto node_id = response->node_id;
  Proxy *proxy = wc->proxies[response->proxy_id];

  int was_leader = raft_is_leader(proxy->raft);

  LOG_RAFT("received appendentries response\n");

  for (size_t i = 0; i < nsequence_spaces; i++) {
    proxy->c->received_ms_seqnums[i]->mutex.lock();
    if (proxy->c->received_ms_seqnums[i]->base_seqnum <
        response->base_seqnums[i]) {
      proxy->c->received_ms_seqnums[i]->pending_truncates = 1;
    }
    proxy->c->received_ms_seqnums[i]->mutex.unlock();
  }

  if (!raft_is_leader(proxy->raft)) {
    printf(
        "I am not the leader receiving an appendentries response\n");
    fflush(stdout);
  }


  // ack the response
  erpc::MsgBuffer &resp_msgbuf = req_handle->pre_resp_msgbuf;
  proxy->c->rpc->resize_msg_buffer(&resp_msgbuf, 8);

  LOG_RAFT_SMALL(
      "proxy_id %u appendentries_response_handler about to queue response started [%s] now [%s]\n",
      proxy->proxy_id,
      start_time.c_str(),
      erpc::get_formatted_time().c_str());
  proxy->c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);

  if (likely(req_handle->get_req_msgbuf()->get_data_size() > 0)) {
    LOG_RAFT_SMALL(
        "smr: proxy_id %u received an appendentries response from node_id %d current idx %ld [%s]\n",
        proxy->proxy_id,
        node_id,
        response->ae_response.current_idx,
        erpc::get_formatted_time().c_str());

    int e = raft_recv_appendentries_response(
        proxy->raft, raft_get_node(proxy->raft, node_id),
        &response->ae_response);

    if (e == RAFT_ERR_NOT_LEADER) {
      printf(
          "[%s] proxy_id %u recv_ae_response not the leader, calling raft_periodic leader is: %d\n",
          erpc::get_formatted_time().c_str(),
          proxy->proxy_id,
          raft_get_current_leader(proxy->raft));

      // lose leadership if I thought I was the leader
      if (was_leader) proxy->lose_leadership();
    } else {
      // some other error
      erpc::rt_assert(e == 0, "error receiving appendentries response %s\n",
                      strerror(e));
    }

    LOG_RAFT(
        "commit idx after receiving response: %lu matchindex: %lu my matchindex: %lu\n",
        raft_get_commit_idx(proxy->raft),
        raft_node_get_match_idx(raft_get_node(proxy->raft, node_id)),
        raft_node_get_match_idx(raft_get_node(proxy->raft, node_id)));
    LOG_RAFT("Last applied: %lu\n",
             raft_get_last_applied_idx(proxy->raft));
  } else {
    printf("appendentries failed\n");
  }

  // TODO perf: calling periodic here may improve latency by applying entries that were just committed?

  proxy->replica_data[node_id].nOutstanding--;

#if PRINT_TIMING
  printf("[%s] ae_response_handler [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

inline void appendentries_cont(void *, void *_tag) {
#if PRINT_TIMING
  auto start = erpc::get_formatted_time();
#endif

  auto *ae_tag = reinterpret_cast<raft_tag_t *>(_tag);
  Proxy *proxy =
      ae_tag->proxy;

  proxy->c->rpc->free_msg_buffer(ae_tag->req_msgbuf);
  proxy->c->rpc->free_msg_buffer(ae_tag->resp_msgbuf);
  proxy->raft_tag_pool.free(ae_tag);

  proxy->c->rpc->run_event_loop_once();

#if PRINT_TIMING
  printf("[%s] appendentries_cont [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
}

void fix_client_pointers(Proxy *proxy,
                         std::unordered_map<uint16_t,
                         std::unordered_map<client_reqid_t,
                         ClientOp *> > *m) {
  for (auto p0 : *m) {
    for (auto p1 : p0.second) {
      ClientOp *client_op = p1.second;
      client_op->proxy = proxy;
      if (proxy->in_need_seqnum_batch_map(client_op->batch_id)) {
        client_op->batch = proxy->need_seqnum_batch_map[client_op->batch_id];
      }
      if (proxy->in_appended_batch_map(client_op->batch_id)) {
        client_op->batch = proxy->appended_batch_map[client_op->batch_id];
      }
      if (proxy->in_done_batch_map(client_op->batch_id)) {
        client_op->batch = proxy->done_batch_map[client_op->batch_id];
      }
    }
  }
}

void fix_batch_pointers(Proxy *proxy,
                        std::unordered_map<uint64_t, Batch *> *m) {
  for (auto pair : *m) {
    Batch *batch = pair.second;

    batch->proxy = proxy;
    batch->c = proxy->c;

    for (unsigned int i = 0; i < batch->batch_client_ops.size(); i++) {
      batch->batch_client_ops[i]->batch = batch;
      batch->batch_client_ops[i]->proxy = proxy;
    }
  }
}

inline Proxy *
process_snapshot(WorkerContext *wc, snapshot_request_t *sr, Proxy *proxy) {
  auto time = erpc::get_formatted_time();
  LOG_INFO(
      "Completed receiving the snapshot, updating proxy and deleting idx %ld term %ld\n",
      sr->last_included_idx,
      sr->last_included_term);

  // first we write the snapshot from memory to disk
  std::string filename =
      "/usr/local/backup_snapshot" + wc->my_ip + std::to_string(wc->thread_id) +
          std::to_string(proxy->proxy_id);

  FILE *ss_file = fopen(filename.c_str(), "w");
  fmt_rt_assert(fwrite(sr->snapshot, 1, sr->size_before_bm, ss_file) ==
                    sr->size_before_bm,
                "did not write all data?\n");
  fclose(ss_file);

  // setup the names of the files
  FILE **bm_files = new FILE *[nsequence_spaces];
  auto *bm_fnames = new std::string[nsequence_spaces];
  for (size_t i = 0; i < nsequence_spaces; i++) {
    bm_fnames[i] = "/usr/local/backup_bitmap_" +
        std::to_string(i) + wc->my_ip +
        std::to_string(wc->thread_id) +
        std::to_string(proxy->proxy_id);
  }

  // this just writes the same bitmap over and over again
  // write the files
  char *buf = sr->snapshot + sr->size_before_bm;
  for (size_t i = 0; i < nsequence_spaces; i++) {
    bm_files[i] = fopen(bm_fnames[i].c_str(), "w");
    size_t b_size = *(reinterpret_cast<size_t *>(buf));
    buf += sizeof(size_t);
    fmt_rt_assert(fwrite(buf, 1, b_size, bm_files[i])
                      == b_size,
                  "did not write all data?\n");
    buf += b_size;
    fclose(bm_files[i]);
  }

  // call raft_begin_load_snapshot which removes all other nodes
  int e = raft_begin_load_snapshot(proxy->raft,
                                   sr->last_included_term,
                                   sr->last_included_idx);
  if (e < 0) return nullptr;

  fmt_rt_assert(e == 0,
                "error beginning load snapshot idx %ld term %ld error %d\n",
                sr->last_included_idx, sr->last_included_term, e);

  // save necessary local pointers
  Batch *nbtp = proxy->next_batch_to_persist;
  Batch *cb = proxy->current_batch;
  Timer *bt = proxy->batch_timer;
  auto bp = proxy->batch_pool;
  auto cop = proxy->client_op_pool;
  auto rtp = proxy->raft_tag_pool;

  replica_data_t rd[3];
  rd[0] = proxy->replica_data[0];
  rd[1] = proxy->replica_data[1];
  rd[2] = proxy->replica_data[2];

  auto rpt = proxy->raft_periodic_tsc;
  auto cpm = proxy->cycles_per_msec;

  auto tag_pool = proxy->tag_pool;

  auto *raft = proxy->raft;

  // load new proxy
  {
    std::ifstream ifs(filename);
    boost::archive::text_iarchive ia(ifs);
    ia >> proxy;
  }

  // load local pointers
  proxy->c = wc;
  proxy->next_batch_to_persist = nbtp;
  proxy->current_batch = cb;
  proxy->batch_timer = bt;
  proxy->batch_pool = bp;
  proxy->client_op_pool = cop;
  proxy->raft_tag_pool = rtp;
  proxy->replica_data[0] = rd[0];
  proxy->replica_data[1] = rd[1];
  proxy->replica_data[2] = rd[2];
  proxy->raft_periodic_tsc = rpt;
  proxy->cycles_per_msec = cpm;
  proxy->tag_pool = tag_pool;
  proxy->raft = raft;

  // set the bitmaps
  for (size_t i = 0; i < nsequence_spaces; i++) {
    wc->received_ms_seqnums[i] = proxy->received_seqnums[i];
    wc->received_ms_seqnums[i]->assign_sequence_space_and_f_name(i);
    wc->received_ms_seqnums[i]->read_from_file(bm_fnames[i].c_str());
  }
  delete[] bm_files;
  delete[] bm_fnames;

  // pointers are local, so even though the objects are reconstructed,
  // the pointers in each object need to be mapped to the new objects.
  fix_batch_pointers(proxy, &(proxy->appended_batch_map));
  fix_batch_pointers(proxy, &(proxy->need_seqnum_batch_map));
  fix_batch_pointers(proxy, &(proxy->done_batch_map));

  fix_client_pointers(proxy, &(proxy->client_retx_done_map));
  fix_client_pointers(proxy, &(proxy->client_retx_in_progress_map));
  fix_client_pointers(proxy, &(proxy->ops_with_handles));

  // set WorkerContext->proxies[] to be the correct proxy
  wc->proxies[proxy->proxy_id] = proxy;

  // raft_add_nodes
  raft_add_node(raft, &proxy->replica_data[replica_1_raft_id],
                proxy->replica_data[replica_1_raft_id].node_id, false);
  raft_add_node(raft, &proxy->replica_data[replica_2_raft_id],
                proxy->replica_data[replica_2_raft_id].node_id, false);

  raft_end_load_snapshot(proxy->raft);

  LOG_INFO("%s Finished loading snapshot last included index %ld proxy %p"
           "took %Lf\n",
           proxy->p_string().c_str(), proxy->last_included_index,
           static_cast<void *>(proxy),
           cycles_to_usec(erpc::rdtsc() - sr->start_time));

  // cleanup snapshot request data
  // delete map entry
  wc->snapshot_requests.erase(sr->rpc);

  // delete allocated mem
  free(sr->snapshot);
  delete sr;

  return proxy;
}

/*
* we receive portions of the snapshot at a time
* we need to:
*  if this is a new request
*    allocate new memory for snapshot
*    put this portion of the snapshot
*    maintain metadata that tells us which portions we've received
*    finally receive the snapshot when we receive the last (not necessarily last in order) snapshot portion
*    ack this message
* all other messages can be processed as normally:
*   we behave as if we received the entirety of the snapshot when we receive the last portion
*/
inline void send_snapshot_handler(erpc::ReqHandle *req_handle, void *_context) {
  auto time = erpc::get_formatted_time();
  auto *wc = static_cast<WorkerContext *>(_context);
  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();

  auto *ss_req = reinterpret_cast<send_snapshot_t *>(req_msgbuf->buf);
  Proxy *proxy = wc->proxies[ss_req->proxy_id];

  LOG_COMPACTION(
      "received snapshot node_id, %d proxy_id, %d, rpc_n %zu msg_n %zu n_msgs %zu total size %zu"
      " offset %zu length %zu\n",
      ss_req->node_id,
      ss_req->proxy_id,
      ss_req->rpc_n,
      ss_req->msg_n,
      ss_req->total_msg_size,
      ss_req->total_snapshot_size,
      ss_req->offset,
      ss_req->length);

  if (raft_is_leader(proxy->raft)) {
    erpc::MsgBuffer &resp_msgbuf = req_handle->pre_resp_msgbuf;
    proxy->c->rpc->resize_msg_buffer(&resp_msgbuf, sizeof(snapshot_response_t));

    auto *sr = reinterpret_cast<snapshot_response_t *>(resp_msgbuf.buf);
    sr->rpc_n = ss_req->rpc_n;
    sr->next_index = 0;
    // we send 0 when we didn't receive the last message yet, or leader
    proxy->c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
    return;
  }

  // get this snapshot
  if (!wc->in_snapshot_requests_map(ss_req->rpc_n)) {
    wc->snapshot_requests[ss_req->rpc_n] = new snapshot_request_t;
  }
  snapshot_request_t *sr = wc->snapshot_requests[ss_req->rpc_n];

  // process snapshot messages
  if (sr->msgs_rcvd == 0) {
    // this is a new snapshot request, we need to alloc
    sr->rpc = ss_req->rpc_n;
    sr->last_included_idx = ss_req->last_included_idx;
    sr->last_included_term = ss_req->last_included_term;
    sr->msgs =
        ss_req->total_msg_size + 1;
    sr->size_before_bm = ss_req->size_before_bm;
    sr->size = ss_req->total_snapshot_size;
    sr->snapshot = static_cast<char *>(malloc(
        ss_req->total_snapshot_size));
    sr->start_time = erpc::rdtsc();
  }

  // eRPC guarantees exactly-once
  sr->msgs_rcvd++;
  rte_memcpy(sr->snapshot + ss_req->offset, &ss_req->snapshot_block,
             ss_req->length);

  // if this was the last message, process entire snapshot
  if (sr->msgs_rcvd == sr->msgs) {
    // we completed this snapshot, we are going to process it, but we need to ack so CC
    // doesn't get messed up. It is the leader's job to make sure it only sets next_index if it updates it.
    // send response
    erpc::MsgBuffer &resp_msgbuf = req_handle->pre_resp_msgbuf;
    proxy->c->rpc->resize_msg_buffer(&resp_msgbuf, sizeof(snapshot_response_t));

    // ACK immediately so we don't throw off congestion control
    auto *s_resp = reinterpret_cast<snapshot_response_t *>(resp_msgbuf.buf);
    s_resp->node_id = my_raft_id;
    s_resp->next_index = sr->last_included_idx + 1;
    s_resp->rpc_n = ss_req->rpc_n;
    s_resp->done = true;
    proxy->c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);

    Proxy *new_proxy = process_snapshot(wc, sr, proxy);
    if (new_proxy != nullptr) {
      wc->proxies[ss_req->proxy_id] = new_proxy;
    }
  } else {
    // return an ACK
    erpc::MsgBuffer &resp_msgbuf = req_handle->pre_resp_msgbuf;
    proxy->c->rpc->resize_msg_buffer(&resp_msgbuf, sizeof(snapshot_response_t));
    // we send 0 when we didn't receive the last message yet, or not leader
    auto *s_resp = reinterpret_cast<snapshot_response_t *>(resp_msgbuf.buf);
    s_resp->node_id = my_raft_id;
    s_resp->rpc_n = ss_req->rpc_n;
    s_resp->next_index = 0;
    s_resp->done = false;
    proxy->c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
  }
}

inline void send_snapshot_cont(void *, void *_tag) {
  auto *tag = reinterpret_cast<raft_tag_t *>(_tag);
  auto proxy = tag->proxy;

  auto *sr = reinterpret_cast<snapshot_response_t *>(tag->resp_msgbuf.buf);

  if (sr->next_index) {
    raft_index_t ni = raft_node_get_next_idx(tag->node);
    if (sr->next_index > ni)
      raft_node_set_next_idx(tag->node, sr->next_index);
  }

  if (sr->done) {
    proxy->snapshot_done_sending[sr->node_id] = true;
  }

  proxy->c->rpc->free_msg_buffer(tag->req_msgbuf);
  proxy->c->rpc->free_msg_buffer(tag->resp_msgbuf);
  proxy->raft_tag_pool.free(tag);
}

/**
 * Log compaction
 * Callback for telling the user to send a snapshot.
 *
 * @param[in] raft Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node Node's ID that needs a snapshot sent to
 **/
inline int __send_snapshot(
    raft_server_t *raft,
    void *user_data,
    raft_node_t *node
) {
  (void) raft;
  (void) node;
  (void) user_data;


  // this proxy
  auto *my_data = static_cast<replica_data_t *>(user_data);
  WorkerContext *wc = my_data->wc;
  Proxy *proxy = wc->proxies[my_data->pid];

  // receiver data
  auto *data = static_cast<replica_data_t *>(raft_node_get_udata(node));

  // If haven't received in a while and not connected don't send
  if (!wc->rpc->is_connected(
      wc->session_num_vec[static_cast<int>(data->idx)])) {
    return 0;
  }

  if (!proxy->snapshot_done_sending[data->node_id]) {
    return 0;
  }
  proxy->snapshot_done_sending[data->node_id] = false;

  // read in the snapshot file
  std::string ss_file_name =
      "/usr/local/snapshot" + wc->my_ip + std::to_string(wc->thread_id) +
          std::to_string(proxy->proxy_id);
  // write the snapshot file to memory
  // this is how to read to memory
  // open the files and get the sizes
  // main snapshot file and size
  FILE *ss_file = fopen(ss_file_name.c_str(), "rb");
  fseek(ss_file, 0, SEEK_END);
  auto ss_size = static_cast<size_t>(ftell(ss_file));
  fseek(ss_file, 0, SEEK_SET);

  size_t total_size = ss_size;

  FILE **bm_files = new FILE *[nsequence_spaces];
  auto *bm_sizes = new size_t[nsequence_spaces];
  for (size_t i = 0; i < nsequence_spaces; i++) {
    erpc::rt_assert(strlen(wc->received_ms_seqnums[i]->f_name) != 0,
                    "empty bitmap filename\n");

    bm_files[i] = fopen(wc->received_ms_seqnums[i]->f_name, "rb");
    // scan to the end to find the size and set back to beginning
    fseek(bm_files[i], 0, SEEK_END);
    bm_sizes[i] = static_cast<size_t>(ftell(bm_files[i]));
    total_size += bm_sizes[i] + sizeof(size_t); // need size of bitmap file

    fseek(bm_files[i], 0, SEEK_SET);
  }

  // malloc enough size for all files in the snapshot
  char *snapshot = reinterpret_cast<char *>(malloc(total_size));
  char *buf = snapshot;
  // read files into the malloc'd buffer
  if (fread(buf, 1, ss_size, ss_file)) printf("read file\n");
  buf += ss_size;

  // read each file into the buffer
  for (size_t i = 0; i < nsequence_spaces; i++) {
    // first write the size
    *(reinterpret_cast<size_t *>(buf)) = bm_sizes[i];
    buf += sizeof(size_t);

    if (fread(buf, 1, bm_sizes[i], bm_files[i])) printf("read a bitmap file\n");
    buf += bm_sizes[i];
    fclose(bm_files[i]);
  }
  delete[] bm_files;
  delete[] bm_sizes;

  fclose(ss_file);

  // ss metadata
  int nid = proxy->replica_data[my_raft_id].node_id;
  uint16_t pid = proxy->proxy_id;
  size_t rpc_n = proxy->c->snapshot_rpcs++;
  size_t total_snapshot_size = total_size;
  size_t total_msg_size = total_size / proxy->c->rpc->get_max_msg_size();
  size_t length = 0;
  size_t msg_n = 0;

  char *curr = snapshot;
  size_t size_left = total_size;
  fmt_rt_assert(size_left > 0, "No data to send in snapshot?\n");

  while (size_left > 0) {
    auto msg_size = std::min(proxy->c->rpc->get_max_msg_size(),
                             sizeof(send_snapshot_t) + size_left -
                                 sizeof(char *));
    length = msg_size - sizeof(send_snapshot_t) + sizeof(char *);
    raft_tag_t *ss_tag = proxy->raft_tag_pool.alloc();
    ss_tag->proxy = proxy;
    ss_tag->node = node;
    ss_tag->req_msgbuf = proxy->c->rpc->alloc_msg_buffer_or_die(msg_size);
    ss_tag->resp_msgbuf = proxy->c->rpc->alloc_msg_buffer_or_die(
        sizeof(snapshot_request_t));

    fmt_rt_assert(
        ss_tag->req_msgbuf.buf != nullptr && ss_tag->resp_msgbuf.buf != nullptr,
        "msgbuffer was null\n");

    // set up the request buffer
    auto *ss_req = reinterpret_cast<send_snapshot_t *>(ss_tag->req_msgbuf.buf);
    ss_req->node_id = nid;
    ss_req->proxy_id = pid;
    ss_req->last_included_idx = proxy->last_included_index;
    ss_req->last_included_term = proxy->last_included_term;
    ss_req->rpc_n = rpc_n;
    ss_req->total_snapshot_size = total_snapshot_size;
    ss_req->size_before_bm = ss_size;
    ss_req->total_msg_size = total_msg_size;
    ss_req->msg_n = msg_n++;
    ss_req->length = length;
    ss_req->offset = static_cast<size_t>(curr -
        snapshot); // curr grows from snapshot // apparently char * is a long int
    rte_memcpy(&ss_req->snapshot_block, curr, length);
    curr += length;
    size_left -= length;

    proxy->c->rpc->enqueue_request(
        proxy->c->session_num_vec[static_cast<int>(data->idx)],
        static_cast<uint8_t>(ReqType::kSendSnapshot),
        &ss_tag->req_msgbuf, &ss_tag->resp_msgbuf,
        send_snapshot_cont, reinterpret_cast<void *>(ss_tag));
  }
  free(snapshot);
  return 0;
}

/** Callback for saving who we voted for to disk.
 * For safety reasons this callback MUST flush the change to disk.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] vote The node we voted for
 * @return 0 on success */
inline int __persist_vote(
    raft_server_t *raft,
    void *user_data,
    raft_node_id_t vote
) {
  (void) raft;
  (void) user_data;
  (void) vote;
  LOG_RAFT("__persist_vote\n");
  return 0;
}

/** Callback for saving current term (and nil vote) to disk.
 * For safety reasons this callback MUST flush the term and vote changes to
 * disk atomically.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] term Current term
 * @param[in] vote The node value dictating we haven't voted for anybody
 * @return 0 on success */
inline int __persist_term(
    raft_server_t *raft,
    void *user_data,
    raft_term_t term,
    raft_node_id_t vote
) {
  (void) raft;
  (void) user_data;
  (void) term;
  (void) vote;
  LOG_RAFT("__persist_term\n");
  return 0;
}


/** Callback for saving log entry changes.
 *
 * This callback is used for applying entries.
 *
 * For safety reasons this callback MUST flush the change to disk.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entry The entry that the event is happening to.
 *    For offering, polling, and popping, the user is allowed to change the
 *    memory pointed to in the raft_entry_data_t struct. This MUST be done if
 *    the memory is temporary.
 * @param[in] entry_idx The entries index in the log
 * @return 0 on success */
inline int __applylog(
    raft_server_t *,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t
) {
  erpc::rt_assert(entry->type == RAFT_LOGTYPE_NORMAL);

  // check which type of replication this entry was and then do the corresponding action
  auto *tmp_entry = reinterpret_cast<entry_t *>(entry->data.buf);
  auto *my_data = static_cast<replica_data_t *>(user_data);
  auto *wc = my_data->wc;
  Proxy *proxy = wc->proxies[my_data->pid];

  assert(my_data->node_id == my_raft_id);

  switch (tmp_entry->type) {
    case EntryType::kDummy:
      // do nothing
      proxy->dummy_replicated = true;
      LOG_WARN("committing dummy entry\n");
      break;

      // it can be a noop here
      // I just replicated a sequence number
    case EntryType::kSequenceNumberNoncontig: {
      auto *our_entry = deserialize_entry(entry->data.buf);

      Batch *batch = proxy->appended_batch_map[our_entry->batch_id];

      // these maintenance things should be more clear...
      proxy->delete_done_batches(our_entry->highest_cons_batch_id);
      if (proxy->highest_cons_batch_id < our_entry->highest_cons_batch_id) {
        proxy->highest_cons_batch_id = our_entry->highest_cons_batch_id;
      }
      // checking base seqnum now done in log_offer

      for (auto &op : batch->batch_client_ops) {
        op->committed = true;
      }

      debug_print(DEBUG_RAFT, "[%zu] Doing batch bookkeeping\n",
                  proxy->c->thread_id);
      // we no longer need the pointer in the appended_batch_map
      // or in the need_seqnum batch_map, safe to use/return to client
      debug_print(DEBUG_RAFT, "[%zu] Updating the batch maps: seqnum %zu\n",
                  batch->seqnum);
      proxy->appended_batch_map.erase(our_entry->batch_id);
      proxy->done_batch_map[our_entry->batch_id] = batch;

      fmt_rt_assert(our_entry->batch_size == batch->batch_size(),
                    "applylog batch sizes aren't the same!? entry %zu "
                    "batch %zu ebid %zu abid %zu enops %zu anops %zu\n",
                    our_entry->batch_size,
                    batch->batch_size(),
                    our_entry->batch_id,
                    batch->batch_id);

      debug_print(DEBUG_RAFT, "[%zu] Recording seqnums\n", proxy->c->thread_id);
      // update FIFO stuff
      for (auto op : batch->batch_client_ops) {
        LOG_FIFO("highest_sequenced_crid %ld, client_reqid %zu\n",
                 proxy->highest_sequenced_crid[op->client_id],
                 op->client_reqid);
        proxy->highest_sequenced_crid[op->client_id] = std::max(
            proxy->highest_sequenced_crid[op->client_id],
            op->client_reqid);
      }

      batch->record_ms_seqnums();

      // this needs to be done after updating or we could try to enter a smaller
      // number into the bitmap than the base
      for (size_t i = 0; i < nsequence_spaces; i++) {

        wc->received_ms_seqnums[i]->mutex.lock();
        if (our_entry->base_seqnums[i] >
            wc->received_ms_seqnums[i]->base_seqnum) {
          LOG_GC("[%zu] Setting pending_truncates = 1 in applylog\n",
                 proxy->c->thread_id);
          wc->received_ms_seqnums[i]->pending_truncates = 1;
        }
        wc->received_ms_seqnums[i]->mutex.unlock();
      }

      if (raft_is_leader(proxy->raft)) {
        debug_print(DEBUG_RAFT,
                    "[%zu] I am the leader submitting to system batch with seq num: %lu\n",
                    proxy->c->thread_id,
                    batch->seqnum);
        batch->submit_batch_to_system();
      }
      debug_print(DEBUG_RAFT, "[%zu] done with applylog\n",
                  proxy->c->thread_id);
    }
      break;

    case EntryType::kSwitchToBackupSeq: {
      LOG_RECOVERY("[%zu] Applying log entry for recovery leader? %d\n",
                   proxy->c->thread_id, raft_is_leader(proxy->raft));

      // Connect to the appropriate thread on the new sequencer
      if (!proxy->c->using_backup) {
        int remote_tid = proxy->c->thread_id % N_SEQTHREADS;
        replace_seq_connection(proxy->c, remote_tid);
        proxy->c->using_backup = true;
      }

      // Only need to do this if we're in recovery
      // We have a count of how many leaders there are in this thread;
      // once all the leaders know about recovery and can commit, we can
      // return.
      if (raft_is_leader(proxy->raft) && proxy->c->in_recovery) {
        uint8_t resps = --proxy->c->leaders_this_thread;
        LOG_RECOVERY("[%zu] Have %u resps left to receive\n",
                     wc->thread_id, resps);
        if (resps == 0) {
          // All proxy leaders have committed
          proxy->c->confirm_recovery_replication(true);
        }
      }
    }
      break;

    default: {
      printf("trying to commit type: %d\n", static_cast<int>(tmp_entry->type));
      erpc::exit_assert(false, "Something was committed with unknown type!\n");
      break;
    }
  }

  return 0;
}

/** Callback for saving log entry changes.
 *
 * This callback is used for adding entries to the log.
 *
 * For safety reasons this callback MUST flush the change to disk.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entry The entry that the event is happening to.
 *    For offering, polling, and popping, the user is allowed to change the
 *    memory pointed to in the raft_entry_data_t struct. This MUST be done if
 *    the memory is temporary.
 * @param[in] entry_idx The entries index in the log
 * @return 0 on success */
inline int __log_offer(
    raft_server_t *raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
) {
  (void) raft;
  (void) user_data;
  (void) entry;
  (void) entry_idx;

  auto *tmp_entry = reinterpret_cast<entry_t *>(entry->data.buf);
  auto *my_data = static_cast<replica_data_t *>(user_data);
  auto *wc = my_data->wc;
  Proxy *proxy = wc->proxies[my_data->pid];

  LOG_RAFT("__log_offer idx %ld len %d buf %p\n",
           entry_idx, entry->data.len, entry->data.buf);

  switch (tmp_entry->type) {
    case EntryType::kDummy:
      break;

    case EntryType::kSequenceNumberNoncontig: {
      auto *our_entry = deserialize_entry(entry->data.buf);

      if (!raft_is_leader(proxy->raft)) {
        debug_print(DEBUG_RAFT, "In log_offer for kSequenceNumber\n");

        // should be the only place a batch is created on a follower
        Batch *batch = proxy->create_new_follower_batch(our_entry);

        // loop through ops (for now just op metadata) and add them to the batch
        auto *cmdata = our_entry->cmdata_buf;
        for (int i = 0; i < our_entry->batch_size; i++) {
          ClientOp *op = proxy->client_op_pool.alloc();

          op->populate(cmdata[i].reqtype,
                       cmdata[i].client_id,
                       cmdata[i].client_reqid,
                       proxy->proxy_id, nullptr, 0, proxy, cmdata[i].seq_reqs);

          copy_seq_reqs(op->seq_reqs, cmdata[i].seq_reqs);
          proxy->add_op_to_batch(op, batch);
          proxy->client_retx_in_progress_map[op->client_id][op->client_reqid] =
              op;
        }
        debug_print(DEBUG_RAFT, "Creating new follower batch with srid %zu\n",
                    batch->seq_req_id);
        proxy->appended_batch_map[our_entry->batch_id] = batch;
        debug_print(DEBUG_RAFT, "I AM NOT THE LEADER the leader is %d\n",
                    raft_get_current_leader(proxy->raft));
      } else {
        // I already have a batch constructed as a leader, don't construct again
        // log_offer happens immediately on the leader
        // appended even if I am the leader (I can lose leadership at any time)
        proxy->appended_batch_map[our_entry->batch_id] = our_entry->batch;
        auto *batch = our_entry->batch;
        for (auto op : batch->batch_client_ops) {
          proxy->client_retx_in_progress_map[op->client_id][op->client_reqid] =
              op;
        }
      }

      // we have already created the batch. We don't give it the sequence number until it is committed
      // do nothing
      auto *batch = proxy->appended_batch_map[our_entry->batch_id];

      erpc::exit_assert(our_entry->batch_size == batch->batch_size(),
                        "log offer batch sizes aren't the same!?\n");

      // we received the sequence number, but it is not yet committed, so it is not safe to
      // send this batch to the system/return to client
      batch->has_seqnum = true;
    }
      break;

    case EntryType::kSwitchToBackupSeq:
      // Don't do anything until we apply
      LOG_RECOVERY("[%zu] In __log_offer for replicating recovery... "
                   "leader? %d\n", proxy->c->thread_id,
                   raft_is_leader(proxy->raft));
      break;

    default:
      printf("trying to append type: %d\n",
             static_cast<int>(tmp_entry->type));
      erpc::exit_assert(false, "Something was committed with unknown type!\n");
      break;

  }

  return 0;
}

/** Callback for saving log entry changes.
 *
 * This callback is used for removing the last entry (youngest) from the log (ie. popping).
 *
 * This happens when an invalid leader finds a valid leader
 * and has to delete superseded log entries.
 *
 * For safety reasons this callback MUST flush the change to disk.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entry The entry that the event is happening to.
 *    For offering, polling, and popping, the user is allowed to change the
 *    memory pointed to in the raft_entry_data_t struct. This MUST be done if
 *    the memory is temporary.
 * @param[in] entry_idx The entries index in the log
 * @return 0 on success */
int __log_pop(
    raft_server_t *raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
) {
  (void) raft;
  (void) user_data;
  (void) entry;
  (void) entry_idx;

  LOG_RAFT("__log_pop");

  auto *entry_to_pop = reinterpret_cast<entry_t *>(entry->data.buf);
  auto *my_data = static_cast<replica_data_t *>(user_data);
  auto *wc = my_data->wc;
  Proxy *proxy = wc->proxies[my_data->pid];

  erpc::exit_assert(!raft_is_leader(proxy->raft),
                    "I am leader popping an entry!?\n");
  free(entry_to_pop);
  return 0;
}

/** Callback for saving log entry changes.
 *
 * This callback is used for removing the first entry (eldest) from the log
 *   (ie. polling).
 *
 * This happens when deleting old entries for log compaction.
 *
 * For safety reasons this callback MUST flush the change to disk.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entry The entry that the event is happening to.
 *    For offering, polling, and popping, the user is allowed to change the
 *    memory pointed to in the raft_entry_data_t struct. This MUST be done if
 *    the memory is temporary.
 * @param[in] entry_idx The entries index in the log
 * @return 0 on success */
int __log_poll(
    raft_server_t *raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
) {
  (void) raft;
  (void) user_data;
  (void) entry;
  (void) entry_idx;

  auto *entry_to_poll = reinterpret_cast<entry_t *>(entry->data.buf);
  free(entry_to_poll);

  // this gets called a lot by raft_end_snapshot
  return 0;
}

void __raft_log(raft_server_t *raft, raft_node_t *node, void *udata,
                const char *buf) {
  (void) raft;
  (void) node;
  (void) udata;

  Proxy *proxy = raft_get_proxy(udata);
  if (proxy->c->in_recovery) {
    printf("[%s] __raft_log Proxy ID: %d %s\n",
           erpc::get_formatted_time().c_str(), proxy->proxy_id, buf);
    fflush(stdout);
  }
}

inline raft_cbs_t *
set_raft_callbacks() {
  // set the callbacks
  auto *raft_callbacks = new raft_cbs_t;
  raft_callbacks->send_requestvote = __send_requestvote;
  raft_callbacks->send_appendentries = __send_appendentries;
  raft_callbacks->applylog = __applylog;
  raft_callbacks->persist_vote = __persist_vote;
  raft_callbacks->persist_term = __persist_term;
  raft_callbacks->log_offer = __log_offer;
  raft_callbacks->log_poll = __log_poll;
  raft_callbacks->log_pop = __log_pop;
//  raft_callbacks->log = __raft_log;
  raft_callbacks->send_snapshot = __send_snapshot;

  return raft_callbacks;
}
