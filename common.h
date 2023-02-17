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
#include <boost/serialization/is_bitwise_serializable.hpp>

#include "logger.h"

#define N_SEQTHREADS 4
#define MAX_PROXIES 32
#define USEC_PER_SEC 1000000

#define PLOT_RECOVERY 0
#define CHECK_FIFO 0

#ifndef DUAL_PHY_PORTS
#define NPHY_PORTS 1
#else
#define NPHY_PORTS 2
#endif

#define MAX_PRE_RESP_MSGBUF 1024

const int INIT_N_BLOCKS = 8;
const int BYTES_PER_BLOCK = 65536 * 16;
const int SEQNUMS_PER_BLOCK = (BYTES_PER_BLOCK * 8);
const int MAX_BLOCKS_PER_MSG = 32;

const int RECOVERY_RPCID = 254;

constexpr size_t kMaxMsgSize = erpc::Rpc<erpc::CTransport>::kMaxMsgSize;

// typedefs to keep consistent types
typedef uint16_t client_id_t;
typedef int64_t client_reqid_t;
typedef uint16_t proxy_id_t;

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
  kCreateZNode, /// Triggers watches left by kExists and kGetChildren (parent)
  /// Triggers watches left by kExists (kNodeRenamed) on new server,
  /// kGetchildren (kNodeChildrenChanged) on parent,
  /// kReadZNode kNodeRenamed on old server
  kRenameZNode,
  kWriteZNode, /// Triggers watches left by kReadZNode
  kReadZNode, /// Leaves a watch triggered by kWriteZNode, kDeleteZNode
  kDeleteZNode, /// Triggers watches left by kExists, kReadZNode, kGetChildren(p)
  kExists, /// Leaves a watch triggered by kCreateZNode(false), kDeleteZNode(true)
  kGetChildren, /// Leaves a watch triggered by kCreateZNode(child), kDeleteZNode(c)
  kClientHeartbeat,
  kDeleteClientConnection,

  // server->client
  kWatchNotification,

  kNoop,

  // proxy->client
  kRecordNoopSeqnum,
};

/*** ZooKeeper specific structs and functions ***/
#define N_ZKTHREADS 1
#define MAX_ZNODE_NAME_SIZE 32
#define MAX_ZNODE_DATA 8
#define MAX_ZK_SHARDS 16

const uint8_t kZKReplicationFactor = 3;

// I am deciding znode name are not allowed to end in "/".
std::string get_parent(std::string name) {
  size_t last = name.find_last_of('/');
  if (last == 0) return std::string("/");
  if (last == name.size() - 1) {
    last = name.substr(0, last).find_last_of('/');
  }
  return name.substr(0, last);
}

// types of ZooKeeper watches
enum WatchType : uint8_t {
  kNone, // 0
  kNodeCreated, // 1
  kNodeDeleted, // 2
  kNodeDataChanged, // 3
  kNodeChildrenChanged, // 4
  kNodeRenamed, // 5
};

/*
 * create():
 *  add_child: on parent
 *  create_node: on node
 * rename:
 *  rename_child: on parent
 *  rename_node: on node
 */
enum OpType : uint8_t {
  kCreateNode,
  kCreateNodeWithChildren,
  kWriteNode,
  kReadNode,
  kRenameNode,
  kDeleteAndReadNode, // required for rename, returns data
  kDeleteNode, // only used when a client requests to delete the node
  kAddChild,
  kRemoveChild,
  kRenameChild,
  kExists,
  kGetChildren,
  kClientHeartbeat,
  kDeleteClientConnection,
};

typedef struct basic_tag {
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
} basic_tag_t;

// Watch notifications push the node and type of the watch, not the data.
// Clients can then submit a read for the data later.
typedef struct watch_notification {
  WatchType type;
  char name[MAX_ZNODE_NAME_SIZE];
} watch_notification_t;

// payloads
typedef struct add_child {
  uint64_t seqnum;
  char name[MAX_ZNODE_NAME_SIZE];
  char child[MAX_ZNODE_NAME_SIZE];
} add_child_t;

typedef struct remove_child {
  uint64_t seqnum;
  char name[MAX_ZNODE_NAME_SIZE];
  char child[MAX_ZNODE_NAME_SIZE];
} remove_child_t;

enum CreateNodeOptions : uint8_t {
  kPersistent,
  kPersistentSequential,
  kEphemeral,
  kEphemeralSequential,
};

static const uint64_t kClientConnectionTimeout = 5000;
typedef struct create_node {
  uint64_t seqnum;
  CreateNodeOptions options;
  int32_t version; // for internal use for renames
  bool is_rename;
  // for ephemeral, having a different operation is better but this will work
  // for now create_node has seemingly become highly inefficient
  bool not_owner;

  uint64_t client_connection_id;
  char name[MAX_ZNODE_NAME_SIZE];
  char data[MAX_ZNODE_DATA];
  client_id_t client_id; // added for ephemeral
  uint8_t nshards;      // added for ephemeral
  uint8_t shards[MAX_ZK_SHARDS];      // added for ephemeral
} create_node_t;

typedef struct write_node {
  uint64_t seqnum;
  int32_t version; // writes iff version matches
  char name[MAX_ZNODE_NAME_SIZE];
  char data[MAX_ZNODE_DATA];
} write_node_t;

// a deleted node triggers all kNodeDataChanged, kNodeChildrenChanged
typedef struct read_node {
  uint64_t seqnum;
  int32_t version; // this is returned to the client
  bool watch; // kNodeDataChanged, kNodeDeleted
  client_id_t client_id;
  char name[MAX_ZNODE_NAME_SIZE];
  char data[MAX_ZNODE_DATA];
} read_node_t;

/**
 * Delete:
 *  On this node triggers watches left by exists() kNodeDeleted, kNodeDataChanged
 *  On parent node triggers watches left by getChildren() kNodeChildrenChanged
 *
 * WriteNode:
 *  On this node triggers kNodeDataChanged
 *
 * RenameNode:
 *  OUR WATCHTYPE NOT IN ZK API
 *
 * Create:
 *  On this node triggers kNodeCreated left by exists()
 *  On parent node triggers kNodeChildrenChanged
 *
 */

typedef struct exists {
  uint64_t seqnum;
  bool exists;
  bool watch; // registers a kNodeCreated/kNodeDeleted and kNodeDataChanged watch
  client_id_t client_id;
  char name[MAX_ZNODE_NAME_SIZE];
} exists_t;

typedef struct get_children {
  uint64_t seqnum;
  int32_t version; // this is returned to the client
  bool watch; // registers a kNodeChildrenChanged and kNodeDeleted watch
  client_id_t client_id;
  char name[MAX_ZNODE_NAME_SIZE];
  uint32_t nchildren;
  char children[1];
} get_children_t;

template<typename Iter>
void serialize_paths(char *arr, Iter first, Iter last){
  while (first != last) {
    strcpy(arr, (*first).c_str());
    arr += MAX_ZNODE_NAME_SIZE;
    first++;
  }
}

void serialize_paths(char *arr, std::set<std::string> &paths) {
  for (const std::string &path : paths) {
    strcpy(arr, path.c_str());
    arr += MAX_ZNODE_NAME_SIZE;
  }
}

// Explicitly register a watch
typedef struct register_watch {
  uint64_t seqnum;
  WatchType type;
} register_watch_t;


constexpr uint64_t sizeof_get_children(uint32_t nchildren) {
  return sizeof(get_children_t) - 1 + MAX_ZNODE_NAME_SIZE*nchildren;
}

typedef struct rename_child {
  uint64_t seqnum;
  char from[MAX_ZNODE_NAME_SIZE];
  char to[MAX_ZNODE_NAME_SIZE];
} rename_child_t;

typedef struct rename_node {
  uint64_t seqnum;
  char from[MAX_ZNODE_NAME_SIZE];
  char to[MAX_ZNODE_NAME_SIZE];
} rename_node_t;

/**
 * ZK API: This operation, if successful, will trigger all the watches on the
 * node of the given path left by exists API calls, and the watches on the
 * parent node left by getChildren API calls.
**/
typedef struct delete_node {
  uint64_t seqnum;
  int32_t version; // -1 matches any version
  char name[MAX_ZNODE_NAME_SIZE];
} delete_node_t;

typedef struct delete_client_connection {
  uint64_t seqnum;
  client_id_t client_id;
  uint64_t client_connection_id;
  bool deleted;
  uint8_t nshards;
  uint8_t shards[MAX_ZK_SHARDS];
} delete_client_connection_t;

void set_shards(std::set<uint8_t> &shards, uint8_t *shards_arr) {
  size_t i = 0;
  for (uint8_t shard : shards) {
    shards_arr[i] = shard;
    i++;
  }
}

// for use from client -> proxy.
// proxy -> server just client_id_t
// server response is a bool (deleted)
typedef struct client_heartbeat {
  client_id_t client_id; // only thing sent to client
  uint16_t proxy_id;
  uint64_t client_connection_number;
  uint8_t nshards;
  uint8_t shards[MAX_ZK_SHARDS];
} client_heartbeat_t;

typedef struct client_heartbeat_req {
  client_id_t client_id;
  uint64_t client_connection_number;
} client_heartbeat_req_t;
typedef struct client_heartbeat_resp {
  bool deleted;
  uint64_t client_connection_number;
} client_heartbeat_resp_t;

constexpr uint64_t sizeof_client_heartbeat(uint32_t nznodes) {
  return sizeof(client_heartbeat_t) + sizeof(size_t)*(nznodes - 1);
}

uint8_t znode_to_shard_idx(std::string name, uint32_t nzk_servers) {
  return std::hash<std::string>{}(name) % (nzk_servers/kZKReplicationFactor);
}

// The structs the clients see.
// For now seqnum is piggybacked but empty...
// todo better design of these structs
// Holds client operations
typedef struct zk_payload {
  union {
    create_node_t               create_node;
    rename_node_t               rename_node;
    write_node_t                write_node; // this is actually setData() in the zk api
    read_node_t                 read_node; // this is getData() in the zk api
    delete_node_t               delete_node;
    exists_t                    exists;
    get_children_t              get_children;
    client_heartbeat_t          client_heartbeat;
    delete_client_connection_t  delete_client_connection;
  };
} zk_payload_t;
BOOST_IS_BITWISE_SERIALIZABLE(zk_payload_t)

/*** End ZooKeeper ***/

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

#define MAX_SEQUENCE_SPACES 64

#define SEQUENCE_PAYLOAD_SIZE(n) (sizeof(payload_t) - sizeof(seq_req_t) + n*sizeof(seq_req_t)) // - sizeof(seq_req_t)
// Seq payload
typedef struct sequencer_payload {
  uint64_t proxy_id;
  uint64_t batch_id;
  uint64_t seq_req_id;
  bool retx = false;
  seq_req_t seq_reqs[1];
} __attribute__((packed)) payload_t;

inline uint64_t sequencer_payload_size(uint64_t n) {
  return sizeof(payload_t) - sizeof(seq_req_t) + n * sizeof(seq_req_t);
}

inline void print_seqreqs(seq_req_t *seqnums, size_t n) {
  std::string line;
  for (size_t i = 0; i < n; i++) {
    line += std::to_string(seqnums[i].batch_size) + " "
        + std::to_string(seqnums[i].seqnum) + " ";
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

#define CLIENT_PAYLOAD_SIZE(x) (sizeof(client_payload_t))
typedef struct client_payload {
  ReqType reqtype;  // ReqType for this operation
  client_id_t client_id;  // Passed in at the command line
  client_reqid_t client_reqid;  // Client-assigned reqid
  proxy_id_t proxy_id;
  bool not_leader;
  client_reqid_t
      highest_recvd_reqid; // the highest cli reqid it got a successful response for
  zk_payload_t zk_payload; // app payload
} __attribute__((__packed__)) client_payload_t;

// this used to require nsequence_spaces...
inline uint64_t client_payload_size() {
  return sizeof(client_payload_t);
}
inline uint64_t client_payload_size(uint64_t nshards) {
  return sizeof(client_payload_t) - sizeof(size_t) + nshards * sizeof(size_t);
}

typedef struct offset_payload {
  uint64_t offset;
  uint64_t sequence_space;
} offset_payload_t;

constexpr size_t kMaxStaticMsgSize = sizeof(client_payload_t);
// We need to alloc response buffers large enough to handle get_children
// responses; but, using kMaxMsgSize runs out of memory.
constexpr size_t kMaxChildrenInGetChildren = 1024;
constexpr size_t kMaxGetChildrenSize =
    sizeof_get_children(kMaxChildrenInGetChildren);
constexpr size_t kMaxClientResponseSize =
    kMaxGetChildrenSize + sizeof(client_payload_t) - sizeof(zk_payload_t);
static_assert(kMaxClientResponseSize < kMaxMsgSize, "kMaxGetChildrenSize too big");

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
    T *backing_ptr;
    try{
      backing_ptr = new T[num_to_alloc];
    } catch(std::bad_alloc&) {
      fmt_rt_assert(false, "AppMemPool alloc of %zu failed\n", num_to_alloc);
    }
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
    LOG_INFO("IN APPMEMPOOL DESTRUCTOR\n");
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
