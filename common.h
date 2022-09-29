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

// for serialization of the bitmap
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>


#include "logger.h"

#define N_SEQTHREADS 4  // probably enough?
#define N_CORFUTHREADS 4  // probably enough?
#define MAX_PROXIES 32  // Arbitrary for now
#define USEC_PER_SEC 1000000

#define CLI_REQ_SIZE sizeof(client_payload_t)
#define CLI_RESP_SIZE 8

#define DEBUG_BMP 0
#define PLOT_RECOVERY 0
#define CHECK_FIFO 0

#ifndef DUAL_PHY_PORTS
#define NPHY_PORTS 1
#else
#define NPHY_PORTS 2
#endif

#define MAX_CORFU_ENTRY_SIZE 64


const int INIT_N_BLOCKS = 2;
const int BYTES_PER_BLOCK = 65536 * 2;
const int SEQNUMS_PER_BLOCK = (BYTES_PER_BLOCK * 8);
const int MAX_BLOCKS_PER_MSG = 32;

const int RECOVERY_RPCID = 254;

size_t kCorfuReplicationFactor = 2;

// Used for indexing the session_num_vecs in the WorkerContext
enum class MachineIdx : uint8_t {
        SEQ = 0,
        DT0,
        DT1,
        DT2,
        NEXTPX,
        BACKUP_SEQ,
        REPLICA_1,
        REPLICA_2,
        CLIENT,
        OTHER,
};

enum class ReqType : uint8_t {
    // Sequencer -> proxy requests
    kGetBitmap,
    kGetDependencies,  
    kAssignSeqnumToHole, 
    kBackupReady,
    kRecoveryComplete,

    // proxy -> sequencer requests
    kGetSeqNum,
    kHeartbeat,
    kInitiateRecovery,

    // replica -> replica
    kRequestVote,
    kAppendEntries,
    kAppendEntriesResponse,
    kSendSnapshot,

    // proxy -> prevproxy requests
    kDoGarbageCollection,

    // client -> proxy request
    kCorfuAppend,
    kCorfuRead,
    kNoop,

    // proxy->client
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
    uint16_t machine_idx; 
    uint8_t thread_id;
    uint8_t proxy_id;
    uint16_t batch_size;
    uint64_t seqnum;
} __attribute__((__packed__)) dependency_t;


// Recovery data sent from a proxy
typedef struct recovery_payload {
    uint64_t base_seqnum;  
    uint16_t nblocks_total;  // How many blocks this proxy will send
    uint16_t nblocks_this_resp;  // How many blocks in this message
    uint16_t head_block;  // Index of first block maintained by this proxy
    uint16_t tail_block;  // Index of last block maintained by this proxy
    uint16_t nrequests;  // Number of outstanding requests
    uint8_t received_seqnums[1];
} __attribute__((packed)) recovery_payload_t;
    

// Seq/deptracker payloads 
typedef struct payload {
    uint64_t seqnum;
    uint64_t proxy_id;
    uint64_t batch_id;
    uint16_t batch_size;
    uint64_t seq_req_id;
    bool retx = false; // Is this a retransmitted seqnum? 
} __attribute__((__packed__)) payload_t;


enum class RetCode : uint8_t {
    kSuccess = 0,
    kDoesNotExist,
    kNoop,
};


// Client structs
typedef struct client_payload {
    ReqType reqtype;
    uint16_t client_id;
    client_reqid_t client_reqid;  // Client-assigned reqid
    uint64_t seqnum;
    uint16_t proxy_id;
    bool not_leader;
    RetCode return_code;
    client_reqid_t highest_recvd_reqid; // the highest cli reqid it got a successful response for
    char entry_val[MAX_CORFU_ENTRY_SIZE];
} __attribute__((__packed__)) client_payload_t;


typedef struct corfu_entry {
    uint64_t log_position;
    char entry_val[MAX_CORFU_ENTRY_SIZE];
    RetCode return_code;
} corfu_entry_t;

typedef struct offset_payload {
    uint64_t offset;
} offset_payload_t;


/* ----------- Utilities ------------- */
// Forward decls
static uint64_t usec_to_cycles (uint64_t hz, uint64_t usec);
static int __attribute__((unused)) check_double_overflow (double, double);
static int debug_print(int, const char *, ...) __attribute__((unused));
static inline void fmt_rt_assert(bool, const char *, ...);


// A basic mempool for preallocated objects of type T. Copied verbatim from
// eRPC code (https://github.com/erpc-io/eRPC/blob/master/apps/apps_common.h)
template <class T>
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
        }
};

// For tracking seqnums already used
class Bitmap {
    public:
        int thread_id = -1;
        size_t head_block = 0; 
        size_t tail_block = INIT_N_BLOCKS - 1;
		size_t nblocks = INIT_N_BLOCKS;
        uint64_t base_seqnum = 0;

        uint8_t pending_truncates;

		uint64_t *counts; 
        uint8_t *bitmap;

        std::mutex mutex;

        Bitmap() {
            bitmap = new uint8_t[INIT_N_BLOCKS * BYTES_PER_BLOCK]();
            counts = new uint64_t[INIT_N_BLOCKS]();
        }

        ~Bitmap() {
            delete[] bitmap;
            delete[] counts;
        }

        // Grow the bitmap to accommodate this seqnum
        void grow(uint64_t seqnum) {
			while (seqnum >= (base_seqnum + nblocks * SEQNUMS_PER_BLOCK)) {
                debug_print(1, "[%zu] growing from base %lu largest %lu to %lu for sn %zu cap %zu\n",
                            thread_id, base_seqnum, base_seqnum + nblocks * SEQNUMS_PER_BLOCK,
                            base_seqnum + 2*nblocks * SEQNUMS_PER_BLOCK, seqnum, 2*nblocks * SEQNUMS_PER_BLOCK);

				size_t new_size = 2 * nblocks; 

                uint8_t *tmp_bitmap = new uint8_t[new_size * BYTES_PER_BLOCK]();
                uint64_t *tmp_counts = new uint64_t[new_size](); 

                rte_memcpy(tmp_bitmap, bitmap + head_block * BYTES_PER_BLOCK,
                        (nblocks - head_block) * BYTES_PER_BLOCK);
                rte_memcpy(tmp_bitmap + (nblocks - head_block) * BYTES_PER_BLOCK, 
                        bitmap, head_block * BYTES_PER_BLOCK);

                rte_memcpy(tmp_counts, counts + head_block, 
                        (nblocks - head_block) * sizeof(uint64_t));
                rte_memcpy(tmp_counts + nblocks - head_block, counts, 
                        head_block * sizeof (uint64_t));

				delete bitmap; 
				bitmap = tmp_bitmap; 

				delete counts;
				counts = tmp_counts;

                // Update bookkeeping variables
				nblocks = new_size;
                head_block = 0;
                tail_block = nblocks - 1;
			}
            print();
        }

        long unsigned int capacity() {
            return nblocks*SEQNUMS_PER_BLOCK;
        }

		// Truncate first block
        void truncate() {
            memset(bitmap + head_block * BYTES_PER_BLOCK,
                    0, BYTES_PER_BLOCK);
            counts[head_block] = 0;
            head_block = (head_block + 1) % nblocks;
            tail_block = (tail_block + 1) % nblocks;
            base_seqnum += SEQNUMS_PER_BLOCK;
        }

        uint64_t get_seqnum_index(uint64_t seqnum) {
            erpc::rt_assert(seqnum >= base_seqnum, 
                    "get_seqnum_index: Seqnum is lower than base of bitmap!\n");
            return (head_block * BYTES_PER_BLOCK + ((seqnum - base_seqnum)/8)) % (
                    nblocks * BYTES_PER_BLOCK);
        }

        uint8_t get_seqnum_bitmask(uint64_t seqnum) {
            erpc::rt_assert(seqnum >= base_seqnum, 
                    "get_seqnum_bitmask: Seqnum is lower than base of bitmap!\n");
            return static_cast<uint8_t>(1 << ((seqnum - base_seqnum) % 8));
        }

        uint64_t get_seqnum_block(uint64_t seqnum) {
            erpc::rt_assert(seqnum >= base_seqnum, 
                    "get_seqnum_block: Seqnum is lower than base of bitmap!\n");
            return (((seqnum - base_seqnum)/SEQNUMS_PER_BLOCK) + head_block) % nblocks;
        }

        uint64_t get_seqnum_from_loc(uint64_t idx, size_t bit) {
            uint64_t start_idx = head_block * BYTES_PER_BLOCK;
            if (idx >= start_idx) {
                return (idx * 8 - head_block * SEQNUMS_PER_BLOCK +
                        base_seqnum + bit);
            } else {
                return (base_seqnum + (nblocks - head_block) * SEQNUMS_PER_BLOCK + 
                        idx * 8 + bit);
            }
        }

        void insert_seqnum(uint64_t seqnum) {
            fmt_rt_assert(seqnum >= base_seqnum,
                    "Seqnum %lu is smaller than base_seqnum %lu!", 
                    seqnum, base_seqnum);

            // Check if bitmap size needs to increase
            if (seqnum >= (base_seqnum + nblocks * SEQNUMS_PER_BLOCK)) {
                grow(seqnum);
            }

            // Calculate bit position based on base_seqnum
            uint64_t byte_idx = get_seqnum_index(seqnum);
            uint8_t bitmask = get_seqnum_bitmask(seqnum);

            // Flip bit (if not already flipped, it shouldn't be!) and increment counts
            fmt_rt_assert(!(bitmap[byte_idx] & bitmask),
                    "Trying to record a seqnum twice!: %lu at byte_idx %zu, "
                    "bitmask %zu, byte value %zu", seqnum, byte_idx, bitmask,
                    bitmap[byte_idx]);
            bitmap[byte_idx] |= bitmask;
            counts[get_seqnum_block(seqnum)]++;
        }

        void print() {
            debug_print(DEBUG_BMP, "[%zu] BITMAP INFODUMP, address %p!\n"
                    "\thead_block: %zu\n"
                    "\ttail_block: %zu\n"
                    "\tnblocks: %zu\n"
                    "\tbase_seqnum: %lu\n"
                    "\tmax recordable seqnum: %zu\n",
                    thread_id, this,
                    head_block, tail_block, nblocks, base_seqnum,
                    base_seqnum + SEQNUMS_PER_BLOCK * nblocks - 1);
        }

        template <class Archive>
        void serialize(Archive & ar, const unsigned int) {
            ar & thread_id;
            ar & head_block;
            ar & tail_block;
            ar & nblocks;
            ar & base_seqnum;

            ar & pending_truncates;

            if (Archive::is_loading::value) {
                counts = new uint64_t[nblocks]();
                bitmap = new uint8_t[nblocks * BYTES_PER_BLOCK]();
            }

            for (size_t i = 0; i < nblocks; i++) {
                ar & counts[i];
            }
        }

        void write_to_file(const char *file_name) {
            FILE *file = fopen(file_name, "w");
            erpc::rt_assert(file != NULL, "Could not open file for bitmap\n");
            size_t n = 0;
            while ((n += fwrite(bitmap+n, 1, (nblocks*BYTES_PER_BLOCK)-n, file))
                   < nblocks*BYTES_PER_BLOCK);
            erpc::rt_assert(n == nblocks*BYTES_PER_BLOCK, "n not equal to size of bitmap?\n");
            fclose(file);
        }

        // does this need to free bitmap!?
        void read_from_file(const char *file_name, size_t size) {
            erpc::rt_assert(nblocks*BYTES_PER_BLOCK == size,
                    "read_from_file size arg is wrong\n");
            FILE *file = fopen(file_name, "rb");
            bitmap = reinterpret_cast<uint8_t *>(malloc(size));
            erpc::rt_assert(fread(bitmap, 1, size, file),
                    "Could not read in bitmap from file.\n");
            fclose(file);
        }
};

template<class Archive>
void serialize(Archive & ar, uint64_t & t, const unsigned int){
    ar & t;
}

template<class Archive>
void serialize(Archive & ar, uint8_t & t, const unsigned int){
    ar & t;
}

inline static uint64_t
usec_to_cycles (uint64_t hz, uint64_t usec)
{
    return usec * (hz/USEC_PER_SEC);
}

static int
check_double_overflow (double before, double after)
{
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
        vsprintf(buf+strlen(buf), fmt, args);
        va_end(args);

        throw std::runtime_error(buf);
    }
}

// for debug printing
// todo ideally this should be a macro so that it isn't even called if the flag is not set
inline static int  __attribute__((unused))
debug_print(int flag, const char *format, ...) {
    if (unlikely(flag)) {
        va_list args;
        printf("[%s] ", erpc::get_formatted_time().c_str());
        va_start(args, format);
        int ret = vfprintf(stdout, format, args);
        va_end(args);
        fflush(stdout);
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
  uint32_t offset_time = seconds+usec - offset;

  sprintf(buf, "%u", offset_time);
  return std::string(buf);
}

#endif
