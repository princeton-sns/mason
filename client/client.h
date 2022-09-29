#define IDENT(x) x
#define XSTR(x) #x
#define STR(x) XSTR(x)
#define PATH(x, y) STR(IDENT(x)IDENT(y))

#define Fname /common.h

#include PATH(COMMON_DIR, Fname)

#include "common.h"
#include "limits.h"

#define MAX_CONCURRENCY 4096
#define DEBUG_SEQNUMS 0
#define DEBUG_FAILOVER 0

DEFINE_uint64(proxy_threadid, 0,
              "Proxy threadid to connect to");
DEFINE_uint64(concurrency, 16,
              "Number of concurrent requests per client");
DEFINE_uint64(nthreads, 16,
              "Number of threads (independent clients) to run "
              "on this client");
DEFINE_uint64(nproxy_leaders, 0,
              "Number of proxy leaders per proxy machine");
DEFINE_uint64(nproxy_threads, 0,
              "Number of proxy threads per proxy machine");
DEFINE_uint64(expduration, 10,
              "Experiment duration");
DEFINE_string(my_ip, "",
              "IP address for this machine ");
DEFINE_string(proxy_ip_0, "",
              "IP address for the proxy n");
DEFINE_string(proxy_ip_1, "",
              "IP address for the proxy n");
DEFINE_string(proxy_ip_2, "",
              "IP address for the proxy n");
DEFINE_uint32(proxy_id, 0,
              "Proxy id");
DEFINE_string(out_dir, "",
              "IP address for the proxy n");
DEFINE_uint64(nsequence_spaces, 0,
              "Total number of sequence spaces");
DEFINE_double(create_percent, 0,
              "Percent of create znode ops.");
DEFINE_double(rename_percent, 0,
              "Percent of rename znode ops.");
DEFINE_double(write_percent, 0,
              "Percent of write znode ops.");
DEFINE_double(read_percent, 0,
              "Percent of read znode ops.");
DEFINE_double(delete_percent, 0,
              "Percent of delete znode ops.");
DEFINE_double(exists_percent, 0,
              "Percent of exists ops.");
DEFINE_double(get_children_percent, 0,
              "Percent of get_children ops.");
DEFINE_uint32(nzk_servers, 0,
              "Number of CATSKeeper servers.");

extern uint64_t nsequence_spaces;
uint64_t nzk_shards;

volatile bool force_quit = false;
double failover_to = 10;


struct app_stats_t;

class Operation;
class Tag;
class ClientContext;

typedef struct client_heartbeat_tag {
  erpc::MsgBuffer req_mbuf;
  erpc::MsgBuffer resp_mbuf;
} client_heartbeat_tag_t;

class Tag {
 public:
  bool msgbufs_allocated = false;
  ClientContext *c;
  Operation *op;

  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;

  ~Tag();

  void alloc_msgbufs(ClientContext *c, Operation *);
};


class Operation {
 public:
  ReqType reqtype;
  client_reqid_t local_reqid;  // client-assigned reqid
  size_t local_idx;  // Index into ops array
  size_t cur_px_conn = 0;
  bool received_response = false;

  Operation(size_t i) {
    local_idx = i;
  }

  void reset(client_reqid_t local_reqid) {
    reqtype = static_cast<ReqType>(0);
    this->local_reqid = local_reqid;
  }
};

struct CmpReqIds {
  // returns true if a comes before b, and should be output later
  bool operator()(uint64_t a, uint64_t b) const {
    return a > b;
  }
};

class ClientContext : public ThreadContext {
 public:
  // For stats
  struct timespec tput_t0;  // Throughput start time
  app_stats_t *app_stats;
  size_t stat_resp_rx_tot = 0;

  uint16_t client_id;

  std::vector<std::string> current_ephemeral_nodes;

  std::vector<int> sessions;

  erpc::Latency latency;

  uint16_t proxy_id;
  client_reqid_t reqid_counter = 0;

  uint64_t client_connection_counter = 0;

  client_reqid_t highest_cons_reqid = -1;
  std::priority_queue<client_reqid_t, std::vector<client_reqid_t>, CmpReqIds> done_req_ids;
  size_t ncreated_pathnames = 0;
  std::vector<std::string> path_names;

  bool alive_reps[3];

  char *stats;
  size_t completed_slots = 0;

  FILE *fp;

  Operation *ops[MAX_CONCURRENCY];
  size_t req_tsc[MAX_CONCURRENCY];
  size_t last_retx[MAX_CONCURRENCY];

  AppMemPool<Tag> tag_pool;

  void ping_connections();

  std::string str() {
    std::ostringstream str;
    str << thread_id << ", " << client_id;
    return str.str();
  }

  std::string &get_random_path() {
    size_t idx = static_cast<size_t>(random()) %
        (path_names.size() + current_ephemeral_nodes.size());
    if (idx < path_names.size())
      return path_names.at(idx);
    return current_ephemeral_nodes.at(idx % path_names.size());
  }

  void allocate_ops() {
    for (size_t i = 0; i < FLAGS_concurrency; i++) {
      ops[i] = new Operation(i);
    }
  }

  void next_proxy(Operation *op) {
    size_t tmp = op->cur_px_conn;
    // all ops follow 0. this seems to work better than individually changing
    if (op->local_idx == 0) {
      for (size_t i = 0; i < 3; i++) {
        op->cur_px_conn = (op->cur_px_conn + 1) % 3;
        // always true
        if (this->alive_reps[op->cur_px_conn]) {
          debug_print(1, "[%zu] Switching from %zu to %zu, op %zu\n",
                      thread_id, tmp, op->cur_px_conn, op->local_idx);

          // set all ops to be the same proxy and only change if I'm op 0
          for (size_t j = 0; j < FLAGS_concurrency; j++) {
            ops[j]->cur_px_conn = op->cur_px_conn;
          }
          return;
        }
      }
    }
  }

  void delete_connection() {
    current_ephemeral_nodes.clear();
    client_connection_counter++;
  }

  void push_and_update_highest_cons_req_id(client_reqid_t rid) {
    done_req_ids.push(rid);
    while (!done_req_ids.empty() &&
        (done_req_ids.top() == highest_cons_reqid + 1 ||
            done_req_ids.top() == highest_cons_reqid)) { // handle duplicates...
      highest_cons_reqid = done_req_ids.top();
      done_req_ids.pop();
    }
  }

  void print_stats();
  ~ClientContext() {
    LOG_INFO("Destructing the client context for %zu\n", thread_id);
    for (size_t i = 0; i < FLAGS_concurrency; i++) {
      delete ops[i];
    }
    free(stats);
  }

};


inline void
Tag::alloc_msgbufs(ClientContext *_c, Operation *_op) {
  this->c = _c;
  this->op = _op;

  size_t bufsize = client_payload_size();


  if (!msgbufs_allocated) {
    req_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
    // may need to make a smaller limit for response size
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(kMaxClientResponseSize);
    msgbufs_allocated = true;
  }

}


Tag::~Tag() {
}