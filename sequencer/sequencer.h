#ifndef SEQUENCER_H
#define SEQUENCER_H

#define IDENT(x) x
#define XSTR(x) #x
#define STR(x) XSTR(x)
#define PATH(x,y) STR(IDENT(x)IDENT(y))

#define Fname /common.h

#include PATH(COMMON_DIR,Fname)

#include "common.h"
#define DEBUG_RECOVERY 1
#define DEBUG_RECOVERY_REPL 0
#define DEBUG_BITMAPS 0
#define DEBUG 0

extern size_t numa_node;
extern volatile bool force_quit; 
extern volatile std::atomic_uint64_t sequence_number;

extern std::vector<std::string> other_ip_list;

extern size_t nproxy_threads;
extern size_t nproxy_machines;
extern std::vector<std::string> other_ip_list;


DECLARE_bool(am_backup);
DECLARE_string(myip);
DECLARE_string(other_ips);


class AmoMapElem {
    public:
        uint64_t seqnum = UINT64_MAX;
        uint64_t batch_size = 0; 
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
        std::vector< std::vector<AmoMapElem> > amo_map;

        void print_stats();
};

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *);
void seqnumreq_handler(erpc::ReqHandle *, void *);
void heartbeat_handler(erpc::ReqHandle *, void *);
void thread_func(int, erpc::Nexus *, app_stats_t *);
void launch_threads(int, erpc::Nexus *);
void run_recovery_loop(erpc::Nexus *);

#endif // SEQUENCER_H
