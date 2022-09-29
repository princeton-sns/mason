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
DEFINE_string(seq_ip, "",
              "IP address for the sequencer");
DEFINE_uint32(proxy_id, 0,
              "Proxy id");
DEFINE_string(out_dir, "",
              "IP address for the proxy n");
DEFINE_string(results_file, "",
              "IP address for the proxy n");
DEFINE_uint64(max_log_position, 0,
              "Max log positino to read from, indicates read only");
// Corfu
DEFINE_string(corfu_ips, "",
              "Corfu IP addresses");

volatile bool force_quit = false;

struct app_stats_t;

class Operation;
class Tag;
class ClientContext;

class Tag {
 public:
  bool msgbufs_allocated = false;
  ClientContext *c;
  Operation *op;

  // Corfu stuff
  size_t corfu_replies;

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
  uint64_t read_pos = 0;

  uint64_t seqnum;
  char entry_val[MAX_CORFU_ENTRY_SIZE];

  Operation(size_t i) {
    local_idx = i;
  }

  void reset(client_reqid_t _local_reqid) {
    local_reqid = _local_reqid;
    seqnum = 0;
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

  std::vector<int> sessions; // proxies only, we loop around it
  int seq_session_num;
  // corfu session numbers
  // vector from replica group to session nums for each replica in the group
  std::vector<std::vector<int>> corfu_session_nums;
  std::vector<std::string> corfu_ips;

  erpc::Latency latency;

  uint16_t proxy_id;
  client_reqid_t reqid_counter = 0;

  client_reqid_t highest_cons_reqid = -1;
  std::priority_queue<client_reqid_t, std::vector<client_reqid_t>, CmpReqIds>
      done_req_ids;

  bool alive_reps[3];

  char *stats;
  size_t completed_slots = 0;

  uint64_t max_log_position = 0;

  FILE *fp;

  std::random_device rd;
  std::mt19937_64 gen;
  std::uniform_int_distribution<unsigned long long> dis;

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
    for (size_t i = 0; i < 3; i++) {
      op->cur_px_conn = (op->cur_px_conn + 1) % 3;
      if (this->alive_reps[op->cur_px_conn]) {
        debug_print(1, "[%zu] Switching from %zu to %zu, op %zu\n",
                    thread_id, tmp, op->cur_px_conn, op->local_idx);
        return;
      }
    }

    // Will never print
    LOG_INFO("Thread: %zu cid %d all proxies declared DEAD\n",
             thread_id,
             client_id);
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

  std::vector<int> *log_pos_to_session_num(uint64_t log_pos) {
    return &corfu_session_nums[
      log_pos % ((corfu_ips.size() * N_CORFUTHREADS)/kCorfuReplicationFactor)];
  }

  void print_stats();
};

inline void
Tag::alloc_msgbufs(ClientContext *_c, Operation *_op) {
  c = _c;
  op = _op;

  size_t bufsize = std::max(sizeof(client_payload_t), sizeof(corfu_entry_t));

  if (!msgbufs_allocated) {
    LOG_INFO("Allocating msg_bufs for the first time op rid %zu size %zu\n",
             op->local_reqid,
             bufsize);
    req_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
    msgbufs_allocated = true;
  }

  c->rpc->resize_msg_buffer(&req_msgbuf, bufsize);
  c->rpc->resize_msg_buffer(&resp_msgbuf, bufsize);
}

Tag::~Tag() {
  LOG_INFO("Destructing Tag?\n");
}

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
    // can't expire if it wasn't running?
    if (!running) return false;
    uint64_t curr_tsc = erpc::rdtsc();
    return curr_tsc - prev_tsc > timeout_tsc;
  }

  bool expired_reset() {
    // can't expire if it wasn't running?
    if (!running) return false;
    uint64_t curr_tsc = erpc::rdtsc();
    if (curr_tsc - prev_tsc > timeout_tsc) {
      start(); return true;
    }
    return false;
  }

  bool expired_stop() {
    // can't expire if it wasn't running?
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
