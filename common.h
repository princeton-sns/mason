#ifndef COMMON_H
#define COMMON_H

#include <array>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <ctype.h>
#include <cstring>
#include <errno.h>
#include <execinfo.h>
#include <getopt.h>
#include <gflags/gflags.h>
#include <inttypes.h>
#include <map>
#include <math.h>
#include <rte_timer.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <tuple>
#include <unistd.h>
#include <unordered_set>
#include <limits.h>

#include "util/numautils.h"
#include "rpc.h"

// for bitmap serialization
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include "logger.h"

#define N_SEQTHREADS 4
#define MAX_PROXIES 32
#define USEC_PER_SEC 1000000

#define PLOT_RECOVERY 1
#define CHECK_FIFO 0

#ifndef DUAL_PHY_PORTS
#define NPHY_PORTS 1
#else
#define NPHY_PORTS 2
#endif

const int INIT_N_BLOCKS = 2;
const int BYTES_PER_BLOCK = 65536 * 2;
const int SEQNUMS_PER_BLOCK = (BYTES_PER_BLOCK * 8);
const int MAX_BLOCKS_PER_MSG = 32;

const int RECOVERY_RPCID = 254;

// Used for indexing the session_num_vecs in the WorkerContext
enum class MachineIdx : uint8_t {
  SEQ = 0,
  NEXTPX0,
  NEXTPX1,
  NEXTPX2,
  BACKUP_SEQ,
  REPLICA_1,
  REPLICA_2,
  CLIENT,
  OTHER, // unused
};

enum class ReqType : uint8_t {
  kGetBitmap,
  kGetDependencies,
  kAssignSeqnumToHole,
  kBackupReady,
  kRecoveryComplete,

  kGetSeqNum,
  kHeartbeat,
  kInitiateRecovery,

  kRequestVote,
  kAppendEntries,
  kAppendEntriesResponse,
  kSendSnapshot,

  kDoGarbageCollection,

  kExecuteOpA,  // Dummy request type
  kNoop,

  kRecordNoopSeqnum,
};

// typedefs to keep consistent types
typedef int64_t client_reqid_t;

// Base class for per-thread context
class ThreadContext {
public:
  erpc::Rpc<erpc::CTransport> *rpc = nullptr;
  std::vector<int> session_num_vec;
  size_t nconnections = 0;
  size_t thread_id;
};

typedef struct dependency {
  uint64_t sequence_space;
  uint16_t machine_idx;
  uint8_t thread_id;
  uint8_t proxy_id;
  uint64_t batch_size;
  uint64_t seqnum;
} __attribute__((__packed__)) dependency_t;


// Recovery data sent from a proxy
typedef struct recovery_payload {
  uint64_t sequence_space;
  uint64_t base_seqnum;
  uint16_t nblocks_total;  // How many blocks this proxy will send
  uint16_t nblocks_this_resp;  // How many blocks in this message
  uint16_t head_block;  // Index of first block maintained by this proxy
  uint16_t tail_block;  // Index of last block maintained by this proxy
  uint16_t nrequests;  // Number of outstanding requests
  uint8_t received_seqnums[1];
} __attribute__((packed)) recovery_payload_t;

typedef struct seq_req {
  uint64_t batch_size;
  uint64_t seqnum;

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & batch_size;
    ar & seqnum;
  }
} seq_req_t;


// Seq payload
typedef struct sequencer_payload {
  uint64_t proxy_id;
  uint64_t batch_id; // used for local sanity checks
  uint64_t seq_req_id;
  bool retx = false;
  seq_req_t seq_reqs[1];
} __attribute__((__packed__)) payload_t;

inline uint64_t sequencer_payload_size(uint64_t n) {
  return sizeof(payload_t) - sizeof(seq_req_t) + n * sizeof(seq_req_t);
}

inline void print_seqreqs(seq_req_t *seqnums, size_t n) {
  std::string line;
  for (size_t i = 0; i < n; i++) {
    line += std::to_string(seqnums[i].batch_size) + " " +
            std::to_string(seqnums[i].seqnum) + " ";
  }
  line += "\n";
  LOG_INFO("%s", line.c_str());
}

void print_payload(payload_t *payload, size_t n) {
  LOG_INFO("pid %zu bid %zu srid %zu retx %d\n",
           payload->proxy_id,
           payload->batch_id,
           payload->seq_req_id,
           payload->retx);
  print_seqreqs(payload->seq_reqs, n);
}

// Client structs
typedef struct client_payload {
  ReqType reqtype;  // ReqType for this operation
  uint16_t client_id;  // Passed in at the command line
  client_reqid_t client_reqid;  // Client-assigned reqid
  uint16_t proxy_id;
  bool not_leader;
  client_reqid_t highest_recvd_reqid; // highest cli reqid successfully received
  seq_req_t seq_reqs[1];
} __attribute__((__packed__)) client_payload_t;

inline uint64_t client_payload_size(uint64_t x) {
  return sizeof(client_payload_t) - sizeof(seq_req_t) + x * sizeof(seq_req_t);
}

typedef struct offset_payload {
  uint64_t offset;
  uint64_t sequence_space;
} offset_payload_t;


/* ----------- Utilities ------------- */

// Forward decls
static uint64_t usec_to_cycles(uint64_t hz, uint64_t usec);
static int __attribute__((unused)) check_double_overflow(double, double);
static int debug_print(int, const char *, ...) __attribute__((unused));
static inline void fmt_rt_assert(bool, const char *, ...);


// A basic mempool for preallocated objects of type T. Copied (nearly) verbatim from
// eRPC code (https://github.com/erpc-io/eRPC/blob/master/apps/apps_common.h)
template<class T>
class AppMemPool {
public:
  size_t num_to_alloc = 1;
  std::vector<T *> backing_ptr_vec;
  std::vector<T *> pool;

  void extend_pool() {
    T *backing_ptr = new T[num_to_alloc];
    for (size_t i = 0; i < num_to_alloc; i++) pool.push_back(&backing_ptr[i]);
    backing_ptr_vec.push_back(backing_ptr);
    num_to_alloc *= 2;
  }

  T *alloc() {
    if (pool.empty()) extend_pool();
    T *ret = pool.back();
    pool.pop_back();
    return ret;
  }

  void free(T *t) { pool.push_back(t); }
  AppMemPool() {}
  ~AppMemPool() {
    // todo there may be memory leaks with serialization?
    // for (T *ptr : backing_ptr_vec) delete[] ptr;
  }
};

inline static uint64_t usec_to_cycles(uint64_t hz, uint64_t usec) {
  return usec * (hz / USEC_PER_SEC);
}

static int check_double_overflow(double before, double after) {
  if (before > after) {
    return 0;
  }
  return 1;
}

static inline void fmt_rt_assert(bool condition, const char *fmt, ...) {
  if (unlikely(!condition)) {
    char buf[1024];
    va_list args;
    sprintf(buf, "[%s] ", erpc::get_formatted_time().c_str());

    va_start(args, fmt);
    vsprintf(buf + strlen(buf), fmt, args);
    va_end(args);

    throw std::runtime_error(buf);
  }
}

// todo replace calls with LOG_* calls (some branches do this)
inline static int __attribute__((unused))
debug_print(int flag, const char *format, ...) {
  if (unlikely(flag)) {
    flockfile(stdout);
    va_list args;
    printf("[%s] ", erpc::get_formatted_time().c_str());
    va_start(args, format);
    int ret = vfprintf(stdout, format, args);
    va_end(args);
    fflush(stdout);
    funlockfile(stdout);
    return ret;
  }
  return 0;
}


inline
static std::string get_formatted_time_from_offset(uint32_t offset) {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  char buf[20];
  uint32_t seconds = t.tv_sec * 1000000;
  uint32_t usec = t.tv_nsec / 1000;
  uint32_t offset_time = seconds + usec - offset;

  sprintf(buf, "%u", offset_time);
  return std::string(buf);
}

#endif
