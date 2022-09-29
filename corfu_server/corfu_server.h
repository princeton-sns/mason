#ifndef CORFU_SERVER_H
#define CORFU_SERVER_H

#define IDENT(x) x
#define XSTR(x) #x
#define STR(x) XSTR(x)
#define PATH(x,y) STR(IDENT(x)IDENT(y))

#define Fname /common.h

#include PATH(COMMON_DIR,Fname)

#define DEBUG 0

#define INIT_SIZE 2048
#define MAX_SIZE INIT_SIZE*1024


#include "common.h"
extern size_t numa_node;
extern volatile bool force_quit;

DEFINE_string(my_ip, "",
"This Corfu Server's IP address for eRPC setup");
DEFINE_uint64(ncorfu_servers, 0,
"The number of servers in the Corfu array");
DEFINE_uint64(max_log_position, 0,
"What to fill up to if read only experiment");

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

class CorfuContext : public ThreadContext {
public:
    struct timespec tput_t0;
    int nproxies; // may not need
    app_stats_t *app_stats;
    uint64_t stat_resp_tx_tot = 0;

    // each thread is a logical corfu server: could instead have one large server with locks
    // the drawback of current design is that proxies need to connect to every corfu thread...
    std::vector <corfu_entry_t> corfu_server_vector;
    corfu_entry_t default_entry;
    size_t nCorfuServers; // n physical servers * corfu_threads per server
    // functions
    void print_stats();

    size_t get_idx(uint64_t log_position) {
        debug_print(DEBUG, "log pos %lu nCorfuServers %lu rep factor %lu denominator %lu numberator %lu frac %lu\n",
                log_position, nCorfuServers, kCorfuReplicationFactor, ((nCorfuServers)/kCorfuReplicationFactor), log_position,
               (log_position / ((nCorfuServers)/kCorfuReplicationFactor)));

        return (log_position / ((nCorfuServers)/kCorfuReplicationFactor));// +
    }

    void write_to_vector(corfu_entry_t *entry) {
        size_t idx = get_idx(entry->log_position);

        debug_print(DEBUG, "Thread %d: trying to write to index: %lu with capacity %lu\n",
                    thread_id, idx, corfu_server_vector.capacity());

        if (corfu_server_vector.capacity() >= MAX_SIZE) {
            idx = idx % corfu_server_vector.capacity();
        } else {
            while (unlikely(corfu_server_vector.capacity() <= idx)) {
                corfu_server_vector.resize(idx + 100, default_entry);
                debug_print(DEBUG, "Thread %d: resized to %lu\n",
                            thread_id, corfu_server_vector.capacity());
            }
        }
        corfu_server_vector[idx] = *entry;
        if (entry->return_code == RetCode::kNoop) {
            corfu_server_vector[idx].return_code = RetCode::kNoop;
        } else {
            corfu_server_vector[idx].return_code = RetCode::kSuccess; // this needs to be better
        }
    }

    // returns false if it was not in the vector
    // o.w. puts value into the entry field (only!) and returns true
    bool read_from_vector(corfu_entry_t *entry) {
        size_t idx = get_idx(entry->log_position);

        if (corfu_server_vector.capacity() <= idx){
            return false;
        } else if (corfu_server_vector[idx].return_code == RetCode::kDoesNotExist) {
            return false;
        }

        memcpy(entry->entry_val, corfu_server_vector[idx].entry_val, MAX_CORFU_ENTRY_SIZE);
        entry->return_code = corfu_server_vector[idx].return_code;

        debug_print(DEBUG, "Thread %d: index: %lu val %lu\n",
                       thread_id, idx, *reinterpret_cast<int64_t *>(entry->entry_val));
        return true;
    }
};

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *);
void corfu_append_req_handler(erpc::ReqHandle *, void *);
void corfu_read_req_handler(erpc::ReqHandle *, void *);
void launch_threads(int, erpc::Nexus *);

#endif //CORFU_SERVER_H
