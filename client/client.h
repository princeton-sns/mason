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
DEFINE_string(results_file, "",
              "File to print results");
DEFINE_uint64(nsequence_spaces, 0,
              "Total number of sequence spaces");

extern uint64_t nsequence_spaces;

volatile bool force_quit = false;
double failover_to = 1;


struct app_stats_t;

class Operation;
class Tag;
class ClientContext;

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
    this->local_reqid = local_reqid;
  }
};

struct CmpReqIds {
  // returns true if a comes before b, and should be output later
  bool operator()(uint64_t a, uint64_t b) const {
    return a > b;
  }
};

enum State {
  warmup,
  experiment,
  cooldown,
};

class ClientContext : public ThreadContext {
public:
  // For stats
  struct timespec tput_t0;  // Throughput start time
  app_stats_t *app_stats;
  size_t stat_resp_rx_tot = 0;
  State state;

  uint16_t client_id;

  std::vector<int> sessions;

  erpc::Latency latency;

  uint16_t proxy_id;
  client_reqid_t reqid_counter = 0;

  client_reqid_t highest_cons_reqid = -1;
  std::priority_queue<client_reqid_t, std::vector<client_reqid_t>,
      CmpReqIds> done_req_ids;

  bool alive_reps[3];

  char *stats;
  size_t completed_slots = 0;

  FILE *fp;

  Operation *ops[MAX_CONCURRENCY];
  size_t req_tsc[MAX_CONCURRENCY];
  size_t last_retx[MAX_CONCURRENCY];

  AppMemPool<Tag> tag_pool;

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

  ~ClientContext() {
    LOG_INFO("Destructing the client context for %zu\n", thread_id);
    for (size_t i = 0; i < FLAGS_concurrency; i++) {
      delete ops[i];
    }
    free(stats);
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
};


inline void
Tag::alloc_msgbufs(ClientContext *_c, Operation *_op) {
  this->c = _c;
  this->op = _op;

  size_t bufsize = client_payload_size( nsequence_spaces);

  if (!msgbufs_allocated) {
    req_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
    msgbufs_allocated = true;
  }
}


Tag::~Tag() {}

// added for more accurate timing
const int SEC_TIMER_US = 1000000;
class Timer {
 public:
  uint64_t timeout_us = 0;
  uint64_t timeout_tsc = 0;
  uint64_t prev_tsc = 0;
  uint64_t diff_tsc;
  double freq_ghz;
  bool running = false;
  void (*callback)(void *);  // The function to call when timer runs out
  void *arg;

  // Can explicitly init with null to have a timer with no cb
  void init(uint64_t to_us, void (*cb)(void *), void *a) {
    freq_ghz = erpc::measure_rdtsc_freq();
    timeout_us = to_us;
    timeout_tsc = erpc::us_to_cycles(to_us, freq_ghz);

    callback = cb;
    arg = a;
    running = false;
  }

  void set_cb (void (*cb)(void *), void *a) {
    callback = cb;
    arg = a;
  }

  // Maintain the callback
  void start() {
    prev_tsc = erpc::rdtsc();
    running = true;
  }

  // Change the callback
  void start(void (*cb)(void *)) {
    callback = cb;
    start();
  }

  void stop() {
    running = false;
  }

  void change_period(uint64_t to_us) {
    timeout_us = to_us;
    timeout_tsc = erpc::us_to_cycles(to_us, freq_ghz);
  }

  // If the timer has run out, call the callback
  void check() {
    if (!running) {
      return;
    }

    uint64_t curr_tsc = erpc::rdtsc();
    if (curr_tsc - prev_tsc > timeout_tsc) {
      stop();
      erpc::rt_assert(callback != nullptr, "Timer with null cb expired!\n");
      callback(arg);
    }
  }

  bool expired() {
    if (!running) return false;
    uint64_t curr_tsc = erpc::rdtsc();
    return curr_tsc - prev_tsc > timeout_tsc;
  }

  bool expired_reset() {
    if (!running) return false;
    uint64_t curr_tsc = erpc::rdtsc();
    if (curr_tsc - prev_tsc > timeout_tsc) {
      start(); return true;
    }
    return false;
  }

  bool expired_stop() {
    if (!running) return false;
    uint64_t curr_tsc = erpc::rdtsc();
    if (curr_tsc - prev_tsc > timeout_tsc) {
      running = false;
      return true;
    }
    return false;
  }

  // set_cb MUST be reset after deserialization
  template<class Archive>
  void serialize(Archive & ar, const unsigned int) {
    ar & freq_ghz;
    ar & timeout_us;
    ar & timeout_tsc;
    ar & prev_tsc;
    ar & diff_tsc;
    ar & running;
  }
};

// Hacky but need to be able to run event loop over smaller timescales
// This is currently broken for some reason.
inline void
run_event_loop_us(erpc::Rpc<erpc::CTransport> *rpc, size_t timeout_us)
{
  static auto freq_ghz = erpc::measure_rdtsc_freq();
  size_t timeout_tsc = erpc::us_to_cycles(timeout_us, freq_ghz);
  size_t start_tsc = erpc::rdtsc();  // For counting timeout_us
  size_t now;

  while (true) {
    rpc->run_event_loop_once();
    now = erpc::dpath_rdtsc();
    if (unlikely(now - start_tsc > timeout_tsc)) break;
  }
}
