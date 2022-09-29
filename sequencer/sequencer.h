#ifndef SEQUENCER_H
#define SEQUENCER_H

#define IDENT(x) x
#define XSTR(x) #x
#define STR(x) XSTR(x)
#define PATH(x, y) STR(IDENT(x)IDENT(y))

#define Fname /common.h

#include <cstdint>
#include <stdint-gcc.h>
#include PATH(COMMON_DIR, Fname)

#include "common.h"
#include "bitmap.h"

#define DEBUG_RECOVERY 1
#define DEBUG_RECOVERY_REPL 0
#define DEBUG_BITMAPS 0
#define DEBUG 0

DEFINE_bool(am_backup, false,
            "Set to true if this sequencer is a backup");
DEFINE_string(my_ip, "",
              "Sequencer IP address for eRPC setup");
DEFINE_string(other_ips, "",
              "IP addresses to connect to if this is the backup");
DEFINE_string(out_dir, "",
              "Output directory");
DEFINE_uint64(nleaders, 0,
              "Total number of proxy groups (needed for recovery)");
DEFINE_uint64(nsequence_spaces, 0,
              "Total number of sequence spaces.");

extern size_t numa_node;
extern volatile bool force_quit;

extern std::vector<std::string> other_ip_list;

extern size_t nproxy_threads;
extern size_t nproxy_machines;
extern std::vector<std::string> other_ip_list;

DECLARE_bool(am_backup);
DECLARE_string(myip);
DECLARE_string(other_ips);

class SeqContext;
class AmoMapElem;

class SequenceSpaces {
 public:
  std::mutex mu;
  size_t nsequence_spaces;
  uint64_t *sequence_spaces;  // [] doesn't compile...

  explicit SequenceSpaces(size_t n) {
    sequence_spaces = new uint64_t[n];
    for (size_t i = 0; i < n; i++)
      sequence_spaces[i] = 0;
    nsequence_spaces = n;
  }

  /**
   * Takes an array of seq_reqs and inserts the seqnum
   * one below the next to be handed out.
   */
  inline void allocate_seqnums(seq_req_t seq_reqs[]) {
    mu.lock();
    for (size_t i = 0; i < nsequence_spaces; i++) {
      sequence_spaces[i] += seq_reqs[i].batch_size;
      seq_reqs[i].seqnum = sequence_spaces[i] - 1;
    }
    mu.unlock();
  }
};

struct app_stats_t {
  double mrps;
  size_t num_re_tx;

  app_stats_t() { memset(this, 0, sizeof(app_stats_t)); }

  static std::string get_template_str() {
    std::string ret = "mrps num_re_tx";
    return ret;
  }

  std::string to_string() {
    auto ret = std::to_string(mrps) + " " + std::to_string(num_re_tx);

    return ret;
  }

  /// Accumulate stats
  app_stats_t &operator+=(const app_stats_t &rhs) {
    this->mrps += rhs.mrps;
    this->num_re_tx += rhs.num_re_tx;
    return *this;
  }
};

class SeqContext : public ThreadContext {
 public:
  struct timespec tput_t0;
  int nproxies;
  app_stats_t *app_stats;

  uint64_t stat_resp_tx_tot = 0;

  // we have a vector to proxy_id vectors
  std::vector<std::vector<AmoMapElem> > amo_map;

  SequenceSpaces *sequence_spaces;

  void print_stats();
};

// why isn't this just a struct?
class AmoMapElem {
 public:
  seq_req_t *seq_reqs = new seq_req_t[FLAGS_nsequence_spaces];
  bool assigned = false;

  // assigns new numbers or copies
  inline bool
  assign_numbers(SeqContext *c, seq_req_t _seq_reqs[]) {
    if (!assigned) {
      c->sequence_spaces->allocate_seqnums(_seq_reqs);
      for (size_t i = 0; i < c->sequence_spaces->nsequence_spaces; i++) {
        seq_reqs[i] = _seq_reqs[i];
      }
      assigned = true;

      return false;
    } else {
      for (size_t i = 0; i < c->sequence_spaces->nsequence_spaces; i++) {
        _seq_reqs[i] = seq_reqs[i];
      }
      return true;
    }
  }
};

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *);
void seqnumreq_handler(erpc::ReqHandle *, void *);
void heartbeat_handler(erpc::ReqHandle *, void *);
void thread_func(int, erpc::Nexus *, app_stats_t *);
void launch_threads(int, erpc::Nexus *);
void run_recovery_loop(erpc::Nexus *);

#endif // SEQUENCER_H
