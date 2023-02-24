#ifndef PROXY_MAIN_H
#define PROXY_MAIN_H

#define DEBUG_RAFT_SMALL 0 // unused
#define DEBUG_RAFT 0
#define DEBUG_FAILOVER 0
#define DEBUG_SEQ 0
#define DEBUG_SEQ1 0 // unused
#define DEBUG_TIMERS 0 // unused
#define DEBUG_THREAD 0
#define DEBUG_RECOVERY 0
#define DEBUG_BITMAPS 0
#define DEBUG_COMPACTION 0 // unused

#define DEBUG_GC 0

#define MOCK_SEQ 0
#define MOCK_DT 0
#define MOCK_CLI 0
#define PRINT_TIMING 0

#include <string>

bool kUtilPrinting = true;
bool kPrintTiming = false;
bool kHeartbeats = false;

extern double freq_ghz;

#define MAX_OPS_PER_BATCH 512

#define MAX_OUTSTANDING_AE 16
int kRaftElectionTimeout = 3000;

#define NO_ERPC (MOCK_DT && MOCK_CLI && MOCK_SEQ)

const int MAX_LEADERS_PER_THREAD = 256;
const int SEC_TIMER_US = 1000000;
const int STAT_TIMER_US = SEC_TIMER_US;
const int SEQ_HEARTBEAT_US = 12500000;
const int GC_TIMER_US = 10000;

enum {
  STAT_TIMER_IDX = 0,
  GC_TIMER_IDX,
  SEQ_HEARTBEAT_IDX,
  REPLICA_1_TIMER_IDX, // unused
  REPLICA_2_TIMER_IDX,
};

// Horrible preprocessor stuff for makefile-specified common.h path
#define IDENT(x) x
#define XSTR(x) #x
#define STR(x) XSTR(x)
#define PATH(x, y) STR(IDENT(x)IDENT(y))

#define Fname /common.h
#include PATH(COMMON_DIR, Fname)

#include "common.h"
#include "bitmap.h"

extern "C" {
#include <raft/raft.h>
#include <raft/raft_private.h>
}

#include <rte_cycles.h>
#include <cinttypes>
#include <iostream>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/priority_queue.hpp>
#include <boost/serialization/is_bitwise_serializable.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include "timer.h"

size_t numa_node = 0;
volatile bool force_quit = false;
static constexpr double kAppLatFac = 3.0;        // Precision factor for latency
extern std::string backupseq_ip;

extern size_t nsequence_spaces;


// Configuration
DEFINE_uint64(nthreads, 0,
              "Number of worker threads to launch on this machine");
DEFINE_uint64(proxy_id_start, 0,
              "Where proxy ids begin, 1 proxy group for each thread!!!");
DEFINE_uint64(nleaders, 0,
              "Number of leaders to run on each worker thread");
DEFINE_uint64(nfollowers, 0,
              "Number of followers to run on each worker thread");
DEFINE_uint64(nclients,
              0,
              "Max number of client threads that will be serviced on this machine");
DEFINE_uint64(nservers, 0,
              "Max number of servers we will connect to with this machine");
DEFINE_bool(am_gc_leader, false,
            "Whether this proxy initiates garbage collection");
DEFINE_bool(no_gc, false,
            "Whether to do garbage collection");

// IP addresses
DEFINE_string(my_ip, "",
              "IP address for this machine");
DEFINE_string(nextproxy0_ip, "",
              "IP address of next proxy in garbage collection ring");
DEFINE_string(nextproxy1_ip, "",
              "IP address of next proxy in garbage collection ring");
DEFINE_string(nextproxy2_ip, "",
              "IP address of next proxy in garbage collection ring");
DEFINE_string(seq_ip, "",
              "IP address of sequencer");
DEFINE_string(backupseq_ip, "",
              "IP address of backup sequencer");
DEFINE_string(dt0_ip, "",
              "IP address of deptracker 0");
DEFINE_string(dt1_ip, "",
              "IP address of deptracker 1");
DEFINE_string(dt2_ip, "",
              "IP address of deptracker 2");
DEFINE_string(replica_1_ip, "",
              "IP address of replica 1");
DEFINE_string(replica_2_ip, "",
              "IP address of replica 2");
DEFINE_string(client_ip, "",
              "IP address of client for recording noop seqnums");

DEFINE_uint64(batch_to, 1000,
              "Batch timeout in us");
DEFINE_int32(my_raft_id, 0,
             "The unique Raft ID for this machine's proxy groups.");
DEFINE_int32(replica_1_raft_id, 0,
             "The unique Raft ID for this machine's first replica.");
DEFINE_int32(replica_2_raft_id, 0,
             "The unique Raft ID for this machine's second replica.");
DEFINE_int64(max_log_size, 10000,
             "The maximum size to keep proxy's log. "
             "Determines how often to snapshot.");
DEFINE_uint64(nsequence_spaces, 5,
              "The number of sequence spaces we are running with.");
DEFINE_string(zk_ips, "",
              "ZooKeeper IP addresses");
DEFINE_string(client_ips, "",
              "All of the clients IPs for watches");
DEFINE_int64(nclient_threads, 0,
             "number of client threads per machine");

raft_node_id_t my_raft_id;
raft_node_id_t replica_1_raft_id;
raft_node_id_t replica_2_raft_id;

extern std::string replica_1_ip;
extern std::string replica_2_ip;

inline static long double cycles_to_usec(uint64_t);

// Lifted nearly verbatim from erpc code:
// https://github.com/erpc-io/eRPC/blob/master/apps/small_rpc_tput/small_rpc_tput.cc
struct app_stats_t {
  double mrps;
  double avg_batch_size;
  size_t num_re_tx;

  // Used only if latency stats are enabled
  double lat_us_50;
  double lat_us_99;
  double lat_us_999;
  double lat_us_9999;
  uint8_t pad[8];  // bring up to cache line size

  app_stats_t() { memset(this, 0, sizeof(app_stats_t)); }

  static std::string get_template_str() {
    std::string ret = "mrps num_re_tx";
    ret += " lat_us_50 lat_us_99 lat_us_999 lat_us_9999";
    return ret;
  }

  std::string to_string() {
    auto ret = std::to_string(mrps) + " " + std::to_string(num_re_tx);
    return ret + " " + std::to_string(lat_us_50) + " " +
        std::to_string(lat_us_99) + " " + std::to_string(lat_us_999) +
        " " + std::to_string(lat_us_9999) + " " +
        std::to_string(avg_batch_size);
  }

  /// Accumulate stats
  app_stats_t &operator+=(const app_stats_t &rhs) {
    this->mrps += rhs.mrps;
    this->avg_batch_size += rhs.avg_batch_size;
    this->num_re_tx += rhs.num_re_tx;

    this->lat_us_50 += rhs.lat_us_50;
    this->lat_us_99 += rhs.lat_us_99;
    this->lat_us_999 += rhs.lat_us_999;
    this->lat_us_9999 += rhs.lat_us_9999;
    return *this;
  }
};

// Forward declarations
class WorkerContext;
class Batch;
class Proxy;

static void batch_to_cb(void *);
static void initiate_garbage_collection(void *);
static void print_stats(void *);
static void initiate_recovery(void *);
static void send_heartbeat(void *);
void replace_seq_connection(WorkerContext *, int);
int
connect_and_store_session_async(WorkerContext *, std::string, int, MachineIdx);
void reset_dt_timer(WorkerContext *c, uint8_t dt_idx);

// Replication structs
struct app_requestvote_t;
raft_cbs_t *set_raft_callbacks();

typedef struct replica_data {
  int node_id;
  MachineIdx idx; // the index into the connections vector
  WorkerContext *wc;
  uint16_t pid;

  int nOutstanding;
} replica_data_t;

template<class Archive>
void serialize(Archive &ar, replica_data_t &rd, const unsigned int) {
  ar & rd.node_id;
  ar & rd.idx;
  ar & rd.pid;
  ar & rd.nOutstanding;
}

typedef struct raft_tag {
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
  raft_node_t *node; // receiver
  Proxy *proxy; // sender
} raft_tag_t;

#define AE_RESPONSE_SIZE(x) (sizeof(ae_response_t) - sizeof(uint64_t) + sizeof(uint64_t)*(x))
typedef struct ae_response {
  raft_node_id_t node_id;
  uint16_t proxy_id;
  msg_appendentries_response_t ae_response;
  uint64_t base_seqnums[1];
} ae_response_t;

enum class EntryType : uint8_t {
  kSequenceNumberNoncontig = 0,
  kSwitchToBackupSeq, // Switch to backup sequencer
  kDummy,
};

typedef struct client_mdata {
  ReqType reqtype;
  uint16_t client_id;
  client_reqid_t client_reqid;
  zk_payload_t zk_payload;
  seq_req_t *seq_reqs;
} client_mdata_t;

#define ENTRY_SIZE(x) (sizeof(entry_t) + (sizeof(client_mdata_t) + sizeof(seq_req_t) * nsequence_spaces) * x + sizeof(uint64_t)*nsequence_spaces + 8)
typedef struct entry {
  // this stuff should be separated to a different struct because
  // we use it for other entries and in applylog without
  // deserializing to use cmdata_buf
  Batch *batch;
  uint64_t highest_cons_batch_id;

  EntryType type;
  uint64_t seq_req_id;
  uint64_t batch_id;
  uint16_t batch_size;

  uint64_t *base_seqnums;
  client_mdata_t *cmdata_buf;

  ~entry() {
    for (size_t i = 0; i < batch_size; i++) {
      delete[] cmdata_buf[i].seq_reqs;
    }
    delete[] cmdata_buf;
    delete[] base_seqnums;
  }
} entry_t;

inline entry_t *init_entry(size_t nops) {
  auto *entry = new entry_t;
  entry->base_seqnums = new uint64_t[nsequence_spaces];
  erpc::rt_assert(entry->base_seqnums != nullptr, "basesegnums null\n");
  entry->cmdata_buf = new client_mdata_t[nops];
  for (size_t i = 0; i < nops; i++) {
    entry->cmdata_buf[i].seq_reqs = new seq_req_t[nsequence_spaces];
  }
  return entry;
}

inline void serialize_entry(uint8_t *_buf, entry_t *src_entry) {
  uint8_t *buf = _buf;
  *(reinterpret_cast<entry_t *>(buf)) = *src_entry;
  reinterpret_cast<entry_t *>(buf)->cmdata_buf = nullptr;
  reinterpret_cast<entry_t *>(buf)->base_seqnums = nullptr;
  buf += sizeof(entry_t);

  // cpy base seqnums array
  memcpy(buf, src_entry->base_seqnums, sizeof(uint64_t) * nsequence_spaces);
  buf += sizeof(uint64_t) * nsequence_spaces;

  for (size_t i = 0; i < src_entry->batch_size; i++) {
    *(reinterpret_cast<client_mdata_t *>(buf)) = src_entry->cmdata_buf[i];
    reinterpret_cast<client_mdata_t *>(buf)->seq_reqs = nullptr;
    buf += sizeof(client_mdata_t);

    memcpy(buf,
           src_entry->cmdata_buf[i].seq_reqs,
           nsequence_spaces * sizeof(seq_req_t));
    buf += nsequence_spaces * sizeof(seq_req_t);
  }
}

inline entry_t *deserialize_entry(void *src_buf) {
  auto *buf = static_cast<uint8_t *>(src_buf);
  auto *entry = init_entry(reinterpret_cast<entry_t *>(buf)->batch_size);

  auto *tmp = entry->cmdata_buf;
  auto *seqnums_tmp = entry->base_seqnums;
  *entry = *(reinterpret_cast<entry_t *>(buf));
  entry->cmdata_buf = tmp;
  entry->base_seqnums = seqnums_tmp;

  erpc::rt_assert(reinterpret_cast<entry_t *>(buf)->cmdata_buf == nullptr);
  buf += sizeof(entry_t);

  // copy base seqnums array
  memcpy(entry->base_seqnums, buf, sizeof(uint64_t) * nsequence_spaces);
  buf += sizeof(uint64_t) * nsequence_spaces;

  for (size_t i = 0; i < entry->batch_size; i++) {
    auto *tmp1 = entry->cmdata_buf[i].seq_reqs;
    entry->cmdata_buf[i] = *(reinterpret_cast<client_mdata_t *>(buf));
    entry->cmdata_buf[i].seq_reqs = tmp1;

    erpc::rt_assert(
        reinterpret_cast<client_mdata_t *>(buf)->seq_reqs == nullptr);
    buf += sizeof(client_mdata_t);

    memcpy(entry->cmdata_buf[i].seq_reqs,
           buf,
           nsequence_spaces * sizeof(seq_req_t));
    buf += nsequence_spaces * sizeof(seq_req_t);
  }
  return entry;
}

extern std::vector<WorkerContext *> *context_vector;

typedef struct gc_pair {
  uint64_t base_seqnum;
  uint64_t count;
} gc_pair_t;

#define GC_PAYLOAD_SIZE(x) (sizeof(gc_payload_t) - sizeof(gc_pair_t) + x*sizeof(gc_pair_t))
typedef struct gc_payload {
  gc_pair_t pairs[1];
} __attribute__((__packed__)) gc_payload_t;

class ClientOp;

class Tag {
 public:
  bool msgbufs_allocated = false;
  Batch *batch;
  uint64_t batch_id;
  WorkerContext *c;
  bool failover = false;
  ClientOp *op;

  std::vector<size_t> *replies; // how many replies from each shard
  bool *returned_to_client;
  seq_req_t *seq_reqs;
  size_t my_shard_idx;

#if !NO_ERPC
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
#else
  char *req_msgbuf;
      char *resp_msgbuf;
#endif

  ~Tag();
  void alloc_msgbufs(WorkerContext *, Batch *, bool);
  void alloc_msgbufs(WorkerContext *);
  void alloc_zk_msgbufs(WorkerContext *);
};

uint64_t update_avg(uint64_t s, double prev_avg, int prev_N) {
  return ((prev_avg * prev_N) + static_cast<double>(s)) / (prev_N + 1);
}

void copy_seq_reqs(seq_req_t dst[], seq_req_t src[]) {
  for (size_t i = 0; i < nsequence_spaces; i++) {
    dst[i].seqnum = src[i].seqnum;
    dst[i].batch_size = src[i].batch_size;
  }
}

// Classes
class ClientOp {
 public:
  WorkerContext *c;
  ReqType reqtype;
  uint16_t client_id;
  uint16_t proxy_id;
  uint64_t batch_id;

  client_reqid_t local_reqid;  // proxy-assigned reqid
  client_reqid_t client_reqid;  // client-assigned reqid

  bool committed = false;
  bool has_handle = false;

  // if we rebuilt from a snapshot, we don't have this handle
  // To respond when the op is completed
  erpc::ReqHandle *req_handle;

  // if we serialize these, it will reserialize the entire proxy --> cycle
  // these pointers need to be constructed after deserialization
  Batch *batch;
  Proxy *proxy;

  seq_req_t *seq_reqs = new seq_req_t[nsequence_spaces];
  zk_payload_t zk_payload;

  // we need an arbitrarily sized app buffer now because get_children has
  // some n children where n is unknown
  // for now we could just use this for get_children...
  size_t op_buf_size = 128;
  uint8_t *op_buf = new uint8_t[128];

  __inline__ void grow_to_accommodate(size_t size) {
    if (unlikely(op_buf_size < size)) {
      delete[] op_buf;
      op_buf = new uint8_t[size];
      op_buf_size = size;
    }
  }

  void populate(ReqType rtype, uint16_t cid, client_reqid_t c_reqid,
                uint16_t pid, erpc::ReqHandle *handle, client_reqid_t l_reqid,
                Proxy *px) {
    reqtype = rtype;
    client_id = cid;
    client_reqid = c_reqid;
    proxy_id = pid;
    proxy = px;
    req_handle = handle;
    local_reqid = l_reqid;
    has_handle = handle != nullptr;
    committed = false;
  }

  void populate(ReqType rtype, uint16_t cid, client_reqid_t c_reqid,
                uint16_t pid, erpc::ReqHandle *handle, client_reqid_t l_reqid,
                Proxy *px, WorkerContext *_c, zk_payload_t _zk_payload) {
    reqtype = rtype;
    client_id = cid;
    client_reqid = c_reqid;
    proxy_id = pid;
    proxy = px;
    req_handle = handle;
    local_reqid = l_reqid;
    has_handle = handle != nullptr;
    committed = false;
    c = _c;
    zk_payload = _zk_payload;

    memset(seq_reqs, 0, nsequence_spaces * sizeof(seq_req_t));
  }

  void respond_to_client();

  void mock_respond_to_client();

  erpc::MsgBuffer *populate_client_response();

  void mock_populate_client_response(client_payload_t *);

  void respond_or_record_client_resp();

  void submit_operation(void);

  void free();

  void print() {
    std::cout << "reqtype " << static_cast<uint8_t>(reqtype)
              << " client_id " << client_id
              << " proxy_id " << proxy_id
              << " local_reqid " << local_reqid
              << " client_reqid " << client_reqid
              //                << " seqnum " << seqnum
              << std::endl;
  }

  void set_seq_reqs();

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & reqtype;
    ar & client_id;
    ar & proxy_id;
    ar & batch_id;
    ar & local_reqid;  // proxy-assigned reqid
    ar & client_reqid;  // client-assigned reqid
    ar
        & boost::serialization::make_array(reinterpret_cast<uint8_t *>(&zk_payload),
                                           sizeof(zk_payload_t));
    ar & op_buf_size;
    for (size_t i = 0; i < op_buf_size; i++) {
      ar & op_buf[i];
    }
    for (size_t i = 0; i < nsequence_spaces; i++)
      ar & seq_reqs[i];
  }
};

struct CmpCrid {
  bool operator()(const ClientOp *a, const ClientOp *b) const {
    return a->client_reqid > b->client_reqid;
  }
};

struct CmpBatchIds {
  // returns true if a comes before b, and should be output later
  bool operator()(uint64_t a, uint64_t b) const {
    return a > b;
  }
};

class Batch {
 public:
  uint64_t batch_id;
  uint16_t proxy_id;
  Proxy *proxy;
  WorkerContext *c;
  uint64_t seq_req_id;

  // Operations in this batch
  std::vector<ClientOp *>
      batch_client_ops;

  // For FIFO ordering: maps client id to last client op in this batch
  // (if one has been added)
  std::unordered_map<uint64_t, int64_t> highest_crid_this_batch;

  uint16_t completed_ops = 0;

  // not needed after the response from the sequencer
  // not used on the followers
  seq_req_t *seq_reqs = new seq_req_t[nsequence_spaces];

  std::unordered_map<std::pair<uint16_t, client_reqid_t>, bool,
                     boost::hash<std::pair<uint16_t, client_reqid_t>>>
      acked_ops;

  // this is only used as a pointer to free the old Tag in case of recovery
  // it has no meaning outside of this machine
  // so it should not be serialized as part of a Batch
  // For recovery: the tag used for the seqnum request
  Tag *recovery_tag;

  // Data about this batch from SEQ
  uint64_t seqnum = 0;

  msg_entry_response_t raft_entry_response;

  bool has_seqnum = false; // only used on followers becoming leaders

  inline size_t batch_size(void) {
    return batch_client_ops.size();
  }

  // Managing class variables/memory
  void reset(WorkerContext *, Proxy *, int, uint64_t);
  void free();

  // Sequencer-related functions
  void request_seqnum(bool);
  void mock_request_seqnum(bool);
  void populate_seqnum_request_buffer(Tag *);
  void assign_seqnums_to_ops_ms_test();
  void replicate_seqnums();
  void record_ms_seqnums();
  void replicate(EntryType);

  // Overlying system-related functions
  void submit_batch_to_system();
  bool ack_op(ClientOp *);

  bool in_highest_crid_this_batch_map(uint64_t cid) {
    auto it = highest_crid_this_batch.find(cid);
    return (it != highest_crid_this_batch.end());
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & batch_id;
    ar & proxy_id;
    ar & seq_req_id;
    ar & batch_client_ops; 
    ar & completed_ops;
    ar & highest_crid_this_batch;
    ar & acked_ops;
    ar & seqnum;
    ar & raft_entry_response;
    ar & has_seqnum;
    for (size_t i = 0; i < nsequence_spaces; i++)
      ar & seq_reqs[i];
  }
};

template<class Archive>
void serialize(Archive &ar, msg_entry_response_t &mer, const unsigned int) {
  ar & mer.id;
  ar & mer.term;
  ar & mer.idx;
}

// Reset values for new and re-used objects from the pool
inline void
Batch::reset(WorkerContext *ctx, Proxy *px, int pid,
             uint64_t id) {
  batch_id = id;
  c = ctx;
  proxy = px;
  proxy_id = pid;
  seqnum = 0;
  completed_ops = 0;
  seq_req_id = 0;
  batch_client_ops.clear();
  acked_ops.clear();
  highest_crid_this_batch.clear();
  has_seqnum = false;

  for (size_t i = 0; i < nsequence_spaces; i++) {
    seq_reqs[i].seqnum = 0;
    seq_reqs[i].batch_size = 0;
  }
}

// ZooKeeper node
class ZNode {
 public:
  std::string my_name;
  std::set<std::string> children;
  char data[MAX_ZNODE_DATA];
  int flags = 0;
  int32_t version = 0;

  ZNode() {}
  ~ZNode() {
    printf("IN DESTRUCTOR FOR ZNODE!?!?!?\n");
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    (void) ar;
    ar & my_name;
    ar & children;
    ar & data;
    ar & flags;
    ar & version;
  }

  void reset(std::string *_my_name, char *_data) {
    my_name = *_my_name;
    children.clear();
    version = 0;
    if (_data == nullptr) memset(data, 0, MAX_ZNODE_DATA);
    else memcpy(data, _data, MAX_ZNODE_DATA);
  }

  ZNode(std::string *_my_name, char *_data) {
    my_name = *_my_name;
    if (_data == nullptr) memset(data, 0, MAX_ZNODE_DATA);
    else memcpy(data, _data, MAX_ZNODE_DATA);
  }

  void add_child(std::string child) {
    children.insert(child);
  }

  void remove_child(std::string child) {
    children.erase(child);
  }

  bool rename_child(std::string from, std::string to) {
    if (!children.erase(from)) return false;
    add_child(to);
    return true;
  }
};

class WatchTag {
 public:
  int session_num = -1;
  WorkerContext *c = nullptr;
  Proxy *p = nullptr;
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
  bool allocated = false;

  void set(WorkerContext *_c, Proxy *_p) {
    c = _c;
    p = _p;
  }
  void alloc();
};

// response is just an ack
__inline__ void notify_cont_func(void *_c, void *_tag);

typedef std::unordered_map<std::string, std::vector<uint16_t>> watch_map_t;

class ClientConnection {
 public:
  client_id_t client_id;
  Timer timer;
  uint64_t client_connection_id;
  // all znodes the client has created during this connection
  std::vector<ZNode *> znodes;
  std::set<uint8_t> shards;

  template<class Archive>
  // took a version
  void serialize(Archive &ar, const unsigned int) {
    ar & client_id;
    ar & client_connection_id;
    ar & znodes;
    ar & shards;
  }

  ClientConnection(){}
  ClientConnection(client_id_t cid) : client_id(cid) {}
};

typedef struct connection_cb_arg {
  Proxy *p;
  ClientConnection *cc;
} connection_cb_arg_t;

void client_connection_cb(void *);



// Batch and ClientOp should be subclasses of Proxy?
class Proxy {
 public:
  uint16_t proxy_id;
  WorkerContext *c;

  Batch *next_batch_to_persist;
  Batch *current_batch = nullptr;

  uint64_t max_received_seqnum = 0;
  uint64_t highest_seen_seqnum[3];

  uint64_t batch_counter = 0;
  client_reqid_t op_counter = 0;

  bool gaining_leadership = false;

  Timer *batch_timer;  // Only one batch with a timer

  // local pools
  // these local pools and timers need to be managed when deserializing
  // into a new proxy struct
  AppMemPool<Batch> batch_pool;
  AppMemPool<ClientOp> client_op_pool;
  AppMemPool<raft_tag_t> raft_tag_pool;

  uint64_t seq_req_id = 1;
  uint64_t highest_del_seq_req_id = 0;

  // last time sent a snapshot to prevent Raft from spamming snapshots
  bool snapshot_done_sending[3] = {true, true, true};
  uint64_t freq_ghz = erpc::measure_rdtsc_freq();

  std::priority_queue<uint64_t, std::vector<uint64_t>, CmpBatchIds>
      deleted_seq_req_ids;

  // client_id -> highest req_id x where all req_ids < x have been ackd
  std::unordered_map<uint16_t, int64_t> last_ackd_crid;

  // FIFO: client_id -> highest client_reqid where the op has been put in a batch.
  std::unordered_map<uint64_t, int64_t> highest_sequenced_crid;

  // FIFO: client_id -> queue of waiting requests
  std::unordered_map<uint64_t,
                     std::priority_queue<
                         ClientOp *, std::vector<ClientOp *>, CmpCrid>>
      op_queues;

  /**
   * raft state
   **/
  raft_server_t *raft;
  bool am_leader;

  // used in raft to make sure don't try to become leader too early
  bool got_leader = false;

  // replica data indexed by node_id
  replica_data_t replica_data[3];

  // snapshot metadata
  raft_term_t last_included_term;
  raft_index_t last_included_index;

  // Time since last invocation of raft_periodic() with a non-zero
  // msec_elapsed argument
  size_t raft_periodic_tsc;
  size_t cycles_per_msec;  // rdtsc cycles in one millisecond

  // holds only those batches that have already been applied on the leader, and need to be continued
  // other batches (that have been appended but not yet committed) are pointed to in entries
  // and will be acted upon when I become the leader
  // batches pointed to in committed can execute the next request
  // if in committed and kDep, we can request a seqnum
  // in committed and kSeq already have a seqnum
  // in appended with kDep: will not be executed until I become leader
  // in appended with kSeq: will not be executed until I become leader
  std::unordered_map<uint64_t, Batch *> appended_batch_map;
  // todo, this isn't even used anymore since there are no DTs remove
  std::unordered_map<uint64_t, Batch *> need_seqnum_batch_map;

  // sticks around until destroyed when clients ack
  // == has_seqnum_batch_map
  // need_seqnum and done_batch_map can be rebuilt
  std::unordered_map<uint64_t, Batch *> done_batch_map;

  // for when we were not the leader when we got the sequence number
  // clients retransmit the request a different proxy after a timeout
  // client_id -> cli_req_id -> ClientOp
  std::unordered_map<uint16_t, std::unordered_map<client_reqid_t, ClientOp *> >
      client_retx_done_map;
  std::unordered_map<uint16_t, std::unordered_map<client_reqid_t, ClientOp *> >
      client_retx_in_progress_map;

  std::unordered_map<uint16_t, std::unordered_map<client_reqid_t, ClientOp *> >
      ops_with_handles;

  std::priority_queue<uint64_t, std::vector<uint64_t>, CmpBatchIds>
      done_batch_ids;
  uint64_t highest_cons_batch_id = 0; // shipped during replication

  // Used for recovery and for dummy entries
  msg_entry_response_t dummy_entry_response;

  AppMemPool<Tag> tag_pool;

  // Recovery-related state
  Bitmap **received_seqnums; // todo remove?

  std::string p_string();

  /// --------------------- ZooKeeper state ---------------------
  // these need to be serialized but the pointers are all local?
  AppMemPool<ZNode> znode_pool;
  AppMemPool<WatchTag> watch_tag_pool;
  // the main struct of filenames to data and its children
  std::unordered_map<std::string, ZNode *> znodes;

  // we maintain a set of watch maps, per each operation type that sets a watch
  // the op that triggers a watch pulls from these maps
  // I think this structure is ok...
  watch_map_t read_node_watches;
  watch_map_t exists_true_watches;
  watch_map_t exists_false_watches;
  watch_map_t get_children_watches;

  // ephemeral node connections
  std::unordered_map<client_id_t, ClientConnection *> client_connections;
  // map of client_id to the current connection number
  // still necessary because client AND server can delete
  // it should be idempotent
  std::unordered_map<client_id_t, uint64_t> client_connection_numbers;

  // eRPC does not check queue unless it has received a response
  std::queue<WatchTag *> watch_notification_queue;

  /// -------------------- end ZooKeeper state --------------------

  /// --------------------- ZooKeeper methods ---------------------
  ZNode *in_znodes(std::string name) {
    return znodes.find(name) != znodes.end() ? znodes[name] : nullptr;
  }

  void check_client_connections() {
    for (auto pair : client_connections) { pair.second->timer.check(); }
  }

  // returns true if deleted
  bool reset_client_timer(client_id_t client_id) {
    // it is possible to have not started the connection yet
    //  because the connection begin is sequence ordered while the heartbeat
    //  is not
    try {
      client_connections.at(client_id)->timer.reset();
      erpc::rt_assert(client_connections.at(client_id)->client_connection_id ==
          client_connection_numbers.at(client_id), "reset should have same cxn nums");
      LOG_INFO("successful reset in try\n");
      return false;
    } catch (std::out_of_range &) {
      LOG_INFO("reset client_id %d out of range\n", client_id);
      return true;
    }
    // will terminate on other exceptions
  }

  // can't access c->rpc from here...
  void send_connect_request_and_store_session(std::string ip);
  void connect_to_clients();
  void check_notification_queue();
  __inline__ void notify(const std::string &, WatchType, uint16_t);
  __inline__ void check_watches_map_and_notify(
      const std::string &znode_name, WatchType type, watch_map_t &watch_map);

  __inline__ void execute_create_node(create_node_t *node) {
    std::string name(node->name);
    auto *znode = znode_pool.alloc();
    znode->reset(&name, node->data);
    auto *old = in_znodes(name);
    if (old) znode_pool.free(old); // if there was one there update it

    // this is if we are creating with a version number as in rename_node
    // local rename node shouldn't need to call this though
    znode->version = node->version;
    znodes[znode->my_name] = znode;

    if (node->options == CreateNodeOptions::kEphemeral) {
      LOG_SERVER("Thread %hu: creating an ephemeral node cid %d %s\n",
               proxy_id, node->client_id, node->name);
      ClientConnection *cc = client_connections[node->client_id];
      if (cc) {
        LOG_SERVER("Thread %hu: connection was alive\n", proxy_id);
        // existing connection, add znode and reset the timer
        cc->znodes.push_back(znode);
        if (raft_is_leader(raft)) {
          cc->timer.reset();
        }
      } else {
        // we need to create the connection
        LOG_SERVER("Thread %hu: creating connection for client_id %d\n",
                 proxy_id, node->client_id);
        cc = new ClientConnection(node->client_id);
        client_connections[node->client_id] = cc;
        cc->znodes.push_back(znode);
        cc->client_connection_id = node->client_connection_id;
        try { erpc::rt_assert(cc->client_connection_id >=
                                  client_connection_numbers.at(node->client_id),
                              "connection number going backwards\n");
        } catch (std::out_of_range &) { }
        client_connection_numbers[node->client_id] = cc->client_connection_id;


        // only start the timer if I'm the leader
        if (raft_is_leader(raft)) {
          auto *arg = new connection_cb_arg_t;
          arg->p = this;
          arg->cc = cc;
          LOG_SERVER("Client Connection address %p\n",
                   reinterpret_cast<void *>(cc));
          cc->timer.init(kClientConnectionTimeout, client_connection_cb, arg);
          cc->timer.start();
        }
      }
    }

    check_watches_map_and_notify(znode->my_name, WatchType::kNodeCreated,
                                 exists_false_watches);
    assert(exists_false_watches[znode->my_name].empty());
  }

  // always succeeds
  __inline__ void execute_delete_client_connection(
      delete_client_connection_t *dcc) {
    LOG_SERVER("In execute delete client connection cid %d\n", dcc->client_id);
    // should always have this, will throw if not
    ClientConnection *cc;
    try { // could be a redundant delete
      cc = client_connections.at(dcc->client_id);
    } catch (std::out_of_range &) {
      dcc->deleted = false;
      return;
    }

    if (dcc->client_connection_id < cc->client_connection_id) {
      dcc->deleted = false;
      return;
    }
    fmt_rt_assert(dcc->client_connection_id == cc->client_connection_id,
                  "Delete client connection impossibly ordered"
                  " dcc %zu mine %zu\n",
                  dcc->client_connection_id, cc->client_connection_id);

    // delete each eph node in the connection and trigger watches
    for (ZNode *znode : cc->znodes) {
      znodes.erase(znode->my_name);
      znode_pool.free(znode);

      // trigger watches left by exists and read
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeDeleted,
                                   read_node_watches);
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeDeleted,
                                   exists_true_watches);
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeDeleted,
                                   get_children_watches);
      assert(read_node_watches[znode->my_name].empty());
      assert(exists_true_watches[znode->my_name].empty());
      assert(get_children_watches[znode->my_name].empty());
    }
    client_connections.erase(cc->client_id);
    dcc->deleted = true;
    delete cc;
  }

  __inline__ void execute_write_node(write_node_t *write_node) {
    std::string name(write_node->name);
    ZNode *znode = in_znodes(name);
    // write does not create the node if it doesn't exist
    if (!znode) return;
    if (write_node->version == -1 ||
        write_node->version == znode->version) {
      memcpy(znode->data, write_node->data, MAX_ZNODE_DATA);
      znode->version++;

      LOG_SERVER("executing write vsn %d new vsn %d\n",
                 znode->version - 1, znode->version);
      // the node has been updated, check watches and notify
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeDataChanged,
                                   read_node_watches);
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeDataChanged,
                                   exists_true_watches);
      assert(exists_true_watches[znode->my_name].empty());
      assert(read_node_watches[znode->my_name].empty());
    } else {
      LOG_SERVER("version wrong not writing zvsn %d opvsn %d\n",
                 znode->version, write_node->version);
    }
  }

  // returns false if node did not exist
  // I don't actually want this op.
  // Rename is a delete followed by a create and rename child.
  __inline__ void execute_rename_node(rename_node_t *rename_node) {
    ZNode *znode = in_znodes(rename_node->from);
    if (!znode) {
      LOG_SERVER("Trying to rename node that doesn't exist.\n");
      return;
    }
    // for now rename
    znode->my_name = std::string(rename_node->to);
    // fix the pointer in the map
    znodes[znode->my_name] = znode;

    // rename child on parent
    // takes advantage of the structs being defined the same way
    // requires rename_node_t is the same as rename_child_t
    execute_rename_child(reinterpret_cast<rename_child_t *>(rename_node));

    // this node is renamed
    // the parent's children has changed, watch is triggered in above call
    check_watches_map_and_notify(rename_node->from, WatchType::kNodeRenamed,
                                 read_node_watches);
    check_watches_map_and_notify(rename_node->from, WatchType::kNodeRenamed,
                                 exists_true_watches);
    check_watches_map_and_notify(znode->my_name, WatchType::kNodeRenamed,
                                 exists_false_watches);
    assert(read_node_watches[znode->my_name].empty());
    assert(exists_true_watches[znode->my_name].empty());
    assert(exists_false_watches[znode->my_name].empty());
  }

  // responds false if no znode of name
  __inline__ void execute_remove_child(remove_child_t *remove_child) {
    auto *znode = in_znodes(remove_child->name);
    if (!znode) {
      return;
    }
    znode->remove_child(remove_child->child);

    check_watches_map_and_notify(znode->my_name,
                                 WatchType::kNodeChildrenChanged,
                                 get_children_watches);
    assert(get_children_watches[znode->my_name].empty());
  }

  // always succeeds
  __inline__ void execute_delete_node(delete_node_t *delete_node) {
    auto *znode = in_znodes(delete_node->name);
    if (!znode) {
      return;
    }

    // remove the child from the parent
    remove_child_t remove_child;
    strcpy(remove_child.name, get_parent(delete_node->name).c_str());
    strcpy(remove_child.child, delete_node->name);
    execute_remove_child(&remove_child);

    // delete the znode
    znodes.erase(znode->my_name);
    znode_pool.free(znode);

    // trigger watches left by exists and read
    check_watches_map_and_notify(znode->my_name, WatchType::kNodeDeleted,
                                 read_node_watches);
    check_watches_map_and_notify(znode->my_name, WatchType::kNodeDeleted,
                                 exists_true_watches);
    check_watches_map_and_notify(znode->my_name, WatchType::kNodeDeleted,
                                 get_children_watches);
    assert(read_node_watches[znode->my_name].empty());
    assert(exists_true_watches[znode->my_name].empty());
    assert(get_children_watches[znode->my_name].empty());
  }

  __inline__ void execute_read_node(read_node_t *read_node) {
    ZNode *znode = in_znodes(read_node->name);
    if (!znode) {
      read_node->version = 0;
      memset(read_node->data, 0, MAX_ZNODE_DATA);
      return;
    }
    read_node->version = znode->version;
    memcpy(read_node->data, znode->data, MAX_ZNODE_DATA);

    if (read_node->watch) {
      read_node_watches[read_node->name].push_back(read_node->client_id);
    }
    return;
  }

  __inline__ void execute_add_child(add_child_t *ad) {
    auto *znode = in_znodes(ad->name);
    LOG_SERVER("Executing add child %s\n", ad->child);
    if (!znode) {
      // creates the parent if the child doesn't exist?
      // perhaps should just fail? this should never happen...
      std::string name(ad->name);
      znode = znode_pool.alloc();
      znode->reset(&name, nullptr);
      znodes[znode->my_name] = znode;
    }
    znode->add_child(ad->child);

    // trigger the parents children changed watcher
    check_watches_map_and_notify(znode->my_name,
                                 WatchType::kNodeChildrenChanged,
                                 get_children_watches);
    assert(get_children_watches[znode->my_name].empty());
  }

  // returns false if node or child did not exist
  __inline__ void execute_rename_child(rename_child_t *rename_child) {
    // if parent doesn't exist create it with no data
    LOG_SERVER("Executing rename_child from %s to %s, parent: %s\n",
             rename_child->from, rename_child->to,
             get_parent(rename_child->from).c_str());
    auto *znode = in_znodes(get_parent(rename_child->from));
    if (!znode) {
      std::string name(get_parent(rename_child->from));
      znode = znode_pool.alloc();
      znode->reset(&name, nullptr);
      znodes[znode->my_name] = znode;
    }
    znode->rename_child(rename_child->from, rename_child->to);

    // trigger the parents children changed watcher
    check_watches_map_and_notify(znode->my_name,
                                 WatchType::kNodeChildrenChanged,
                                 get_children_watches);
    assert(get_children_watches[znode->my_name].empty());
  }

  __inline__ void execute_exists(exists_t *exists) {
    LOG_SERVER("executing exists for node %s\n", exists->name);
    exists->exists = in_znodes(exists->name);

    // register watches
    if (exists->watch) {
      if (exists->exists) {
        LOG_SERVER("adding to exists true\n");
        exists_true_watches[exists->name].push_back(exists->client_id);
      } else {
        LOG_SERVER("exists returning false %s\n", exists->name);
        exists_false_watches[exists->name].push_back(exists->client_id);
      }
    }
  }

  // the proxy code uses a separate op buffer for get children
  // because the size can vary
  __inline__ void execute_get_children(ClientOp *cop, get_children_t *gc) {
    ZNode *znode = in_znodes(gc->name);
    if (!znode) {
      auto *gc_ret = reinterpret_cast<get_children_t *>(cop->op_buf);
      gc_ret->nchildren = 0;
      strcpy(gc_ret->name, gc->name);
      gc_ret->version = -1; // used to return null node
      return;
    }
    // address of reference is the address of original object
    auto gc_size = sizeof_get_children(znode->children.size());
    fmt_rt_assert(gc_size < kMaxMsgSize,
                  "Size of get children response too large. %zu for %zu children. "
                  "Max eRPC msg size is %zu.\n", gc_size, znode->children.size(),
                  kMaxMsgSize);
    cop->grow_to_accommodate(gc_size);

    auto *gc_ret = reinterpret_cast<get_children_t *>(cop->op_buf);

    gc_ret->version = znode->version;
    strcpy(gc_ret->name, znode->my_name.c_str());
    gc_ret->seqnum = gc->seqnum;
    gc_ret->nchildren = znode->children.size();
    char *gc_child_ptr = gc_ret->children;
    for (const std::string &child : znode->children) {
      strcpy(gc_child_ptr, child.c_str());
      gc_child_ptr += MAX_ZNODE_NAME_SIZE;
    }

    // register watch
    if (gc->watch) {
      get_children_watches[gc->name].push_back(
          gc->client_id);
    }
  }

  /// --------------------- end ZooKeeper methods ---------------------

  // For creating a new batch when old one closes
  void create_new_batch() {
    Batch *batch = batch_pool.alloc();
    batch->reset(c, this, proxy_id, batch_counter++);
    current_batch = batch;

    reset_batch_timer();
  }

  void update_ackd_reqids(uint16_t, int64_t);

  void push_and_update_highest_cons_batch_id(uint64_t bid) {
    if (highest_cons_batch_id > bid) {
      // already processed
      return;
    }

    done_batch_ids.push(bid);
    // if I was a follower, we could have a top() that is lagging
    // behind highest_cons_batch_id, but it is safe to get rid of
    // everything up until highest_cons_batch_id:
    // pop until top >= highest_cons_batch_id
    while (unlikely(done_batch_ids.top() < highest_cons_batch_id)) {
      // LOG_ERROR("highest cons was higher than top\n");
      done_batch_ids.pop();
    }

    while (!done_batch_ids.empty() &&
        (done_batch_ids.top() == highest_cons_batch_id + 1 ||
            done_batch_ids.top() == highest_cons_batch_id)) {
      highest_cons_batch_id = done_batch_ids.top();
      done_batch_ids.pop();
    }
  }

  bool enqueue_or_add_to_batch(ClientOp *);

  void release_queued_ops();
  void complete_and_send_current_batch();
  void enqueue_op(ClientOp *);

  void replicate_recovery();

  bool dummy_replicated = false;

  void replicate_dummy() {
#if PRINT_TIMING
    auto start = erpc::get_formatted_time();
#endif
    entry_t *entry;
    auto type = EntryType::kDummy;

    printf("[%u] replicating dummy\n", proxy_id);
    fflush(stdout);

    // if it is kSeq we send no metadata
    entry = reinterpret_cast<entry_t *>(malloc(ENTRY_SIZE(1)));
    entry->batch = nullptr;

    // initialize raft entry
    entry->type = type;

    // create raft entry
    msg_entry_t raft_entry;
    raft_entry.type = RAFT_LOGTYPE_NORMAL;
    raft_entry.data.buf = entry;
    raft_entry.data.len =
        sizeof(entry_t) + sizeof(client_mdata_t) * 1;
    raft_entry.id =
        my_raft_id;

    raft_recv_entry(this->raft, &raft_entry, &this->dummy_entry_response);

    dummy_replicated = false;
#if PRINT_TIMING
    printf("[%s] replicate dummy [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
  }

  void gain_leadership();

  /**
   * This is for when the proxy loses leadership!
   *
   */
  void stop_timers() {
    LOG_ERROR("Stopping all timers...\n");
    batch_timer->stop();
  }

  void lose_leadership() {
    printf("I JUST LOST LEADERSHIP for %d\n", proxy_id);
    fflush(stdout);
    // stop all timers
    stop_timers();
  }

  ~Proxy() {
    delete[] received_seqnums;
    printf("in proxy destructor, btimer %p\n",
           reinterpret_cast<void *>(batch_timer));
    if (am_leader) {
      batch_timer->stop();
    }
  }

  void add_op_to_batch(ClientOp *op) {
    current_batch->batch_client_ops.push_back(op);
    op->batch = current_batch;
    op->batch_id = current_batch->batch_id;
    current_batch->acked_ops[{op->client_id, op->client_reqid}] = false;

    for (size_t i = 0; i < nsequence_spaces; i++) {
      current_batch->seq_reqs[i].batch_size += op->seq_reqs[i].batch_size;
    }
  }

  void add_op_to_batch(ClientOp *op, Batch *batch) {
    LOG_SEQ("Adding an op to batch %zu\n",
            batch->batch_id);
    batch->batch_client_ops.push_back(op);
    op->batch = batch;
    op->batch_id = batch->batch_id;
    batch->acked_ops[{op->client_id, op->client_reqid}] = false;

    for (size_t i = 0; i < nsequence_spaces; i++) {
      batch->seq_reqs[i].batch_size += op->seq_reqs[i].batch_size;
    }
  }

  Proxy(WorkerContext *ctx, bool a, int p);

  Proxy();

  void deserial_proxy(WorkerContext *ctx, bool a, int p);

  void reset_heartbeat_timer();

  void check_for_persisted_client_ops();

  void init_timers();

  void reset_batch_timer();

  void proxy_snapshot();
  /*** rest of proxy def here ***/

  // For creating a batch on a follower!
  Batch *create_new_follower_batch(entry_t *ety) {
    Batch *b = batch_pool.alloc();

    if (ety->batch_id >= batch_counter)
      batch_counter = ety->batch_id + 1;
    if (ety->seq_req_id >= seq_req_id)
      seq_req_id = ety->seq_req_id + 1;

    b->batch_id = ety->batch_id;
    b->c = c;
    b->proxy = this;
    b->proxy_id = proxy_id;
    b->completed_ops = 0; // todo
    b->batch_client_ops.clear();
    b->acked_ops.clear();
    b->has_seqnum = false;
    b->seq_req_id = ety->seq_req_id;

    return b;
  }

  ClientOp *in_in_progress_map(uint16_t cid, int64_t rid) {
    auto it0 = client_retx_in_progress_map.find(cid);
    if (it0 != client_retx_in_progress_map.end()) {
      auto it1 = it0->second.find(rid);
      if (it1 != it0->second.end()) {
        return client_retx_in_progress_map[cid][rid];
      }
    }
    return nullptr;
  }

  ClientOp *in_client_retx_done_map(uint16_t cid, int64_t rid) {
    auto it0 = client_retx_done_map.find(cid);
    if (it0 != client_retx_done_map.end()) {
      auto it1 = it0->second.find(rid);
      if (it1 != it0->second.end()) {
        return client_retx_done_map[cid][rid];
      }
    }
    return nullptr;
  }

  bool in_highest_sequenced_crid_map(uint64_t cid) {
    auto it = highest_sequenced_crid.find(cid);
    return it != highest_sequenced_crid.end();
  }

  bool in_last_acked_map(uint64_t cid) {
    auto it = last_ackd_crid.find(cid);
    return it != last_ackd_crid.end();
  }

  bool in_appended_batch_map(uint64_t bid) {
    auto it = appended_batch_map.find(bid);
    return it != appended_batch_map.end();
  }

  bool in_need_seqnum_batch_map(uint64_t bid) {
    auto it = need_seqnum_batch_map.find(bid);
    return it != need_seqnum_batch_map.end();
  }

  bool in_done_batch_map(uint64_t bid) {
    auto it = done_batch_map.find(bid);
    return it != done_batch_map.end();
  }

  // bid must be monotonically increasing across calls
  void delete_done_batches(uint64_t bid) {
    // uint64_t... to avoid corner case we don't delete 0 until 1 is ackd
    if (bid == 0) return;

    // all batches <= bid are safe to delete, unless it is 0
    // it will not be 1 unless 0 has been acked, 0 won't be acked until it is safe to delete.
    // if we made it here, it is safe to delete 0
    while (in_done_batch_map(bid)) {
      Batch *b = done_batch_map[bid];
      // for PQ
      deleted_seq_req_ids.push(b->seq_req_id);
      while (!deleted_seq_req_ids.empty() &&
          (deleted_seq_req_ids.top() == highest_del_seq_req_id + 1 ||
              deleted_seq_req_ids.top() == highest_del_seq_req_id)) {
        highest_del_seq_req_id = deleted_seq_req_ids.top();
        deleted_seq_req_ids.pop();
      }

      for (ClientOp *op : b->batch_client_ops) {
        client_retx_in_progress_map[op->client_id].erase(op->client_reqid);
        client_retx_done_map[op->client_id].erase(op->client_reqid);
        client_op_pool.free(op);
      }
      LOG_FAILOVER("deleting batch id %zu\n", bid);
      done_batch_map.erase(bid--);
      batch_pool.free(b);
    }
  }

  // calls raft_periodic if it has been at least nms ms since the last call
  inline void call_raft_periodic(size_t nms);

  template<class Archive>
  // took a version
  void serialize(Archive &ar, const unsigned int);

  // Testing
  void add_dummy_client_ops();
};

// only prints when it is acking won't show the thing we are waiting for...
bool Batch::ack_op(ClientOp *op) {
  fmt_rt_assert(acked_ops.size() == batch_size(),
                "acked_ops not the correct size ao %zu bs %zu",
                acked_ops.size(), batch_size());
  // on new leaders I think this can be false but already acked if we have
  // serialized and started from snapshot on a follower that became the leader
  if (!acked_ops[{op->client_id, op->client_reqid}]) {
    acked_ops[{op->client_id, op->client_reqid}] = true;
  }
  // the above assert should actually be enough for this...
  fmt_rt_assert(!acked_ops.empty(), "acking a client op (%zu, %ld) with an"
                                    " empty acked_ops map\n",
                op->client_id, op->client_reqid);
  // if any is not acked this isn't acked, if all acked the batch is
  for (auto pair : acked_ops) {
    if (!pair.second) return false;
  }
  return true;
}

class RecoveryContext {
 public:
  WorkerContext *c;
  Bitmap *agg;
  std::vector<Batch *> *recovery_batches;
  FILE *fp;

  std::vector<dependency_t> dependencies;

  RecoveryContext(WorkerContext *c) {
    this->c = c;
  }

  ~RecoveryContext() {
  }
};

typedef struct snapshot_request {
  size_t rpc;
  raft_index_t last_included_idx;
  raft_term_t last_included_term;
  size_t msgs;
  size_t msgs_rcvd = 0;
  uint64_t start_time; // tsc we received the first packet
  size_t size_before_bm = 0;
  size_t size;
  char *snapshot;
} snapshot_request_t;

class WorkerContext : public ThreadContext {
 public:
  std::string my_ip;
  int nops = -1;
  // this should be a map
  // proxy ids are not consecutive because they are unique
  // indexing always with [0] since there is only 1 now
  std::map<uint16_t, Proxy *> proxies;
  int nproxies;

  FILE *fp;

  double avg_batch_size = 0;
  int nbatches = 1;

  app_stats_t *app_stats;
  size_t stat_resp_tx_tot = 0;
  size_t ae_pkts = 0;
  size_t ae_bytes = 0;
  erpc::Latency latency;
  struct timespec tput_t0;  // Throughput start time
  size_t stat_batch_count = 0;

  // Recovery-related state
  std::vector<Bitmap *> received_ms_seqnums;
  bool heartbeat_last_call = false;
  volatile bool in_recovery = false;
  volatile bool using_backup = false;

  RecoveryContext *recovery_context;
  std::atomic_uint8_t recovery_confirmations;
  std::atomic_uint8_t total_leaders;
  uint8_t recovery_repls_needed;
  uint8_t leaders_this_thread;
  erpc::ReqHandle *backup_ready_handle = nullptr;

  // using these seems to be ok
  bool received_gc_response0 = true;
  bool received_gc_response1 = true;
  bool received_gc_response2 = true;
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
  erpc::MsgBuffer gc_req_msgbufs[3];
  erpc::MsgBuffer gc_resp_msgbufs[3];

  // gc metrics
  double avg_time_bw_trying_to_send = 0;
  int tts_c = 1;
  uint64_t lt_tts = 0;

  double avg_time_bw_sending = 0;
  int s_c = 1;
  uint64_t lt_s = 0;

  double avg_ring_duration = 0;
  int rd_c = 1;

  std::array<Timer, MAX_LEADERS_PER_THREAD> batch_timers;
  std::array<Timer, 8> util_timers;
  std::array<bool, 8> sent;

  bool only_proxy = false;

  size_t snapshot_rpcs = 0;
  std::unordered_map<size_t, snapshot_request_t *> snapshot_requests;

  std::vector<std::vector<int>> zk_session_nums;
  std::vector<std::string> zk_ips;

  // ZooKeeper connections for watches that are local
  // relies on assumption of 1 thread
  std::unordered_map<uint16_t, int> cid_to_session_num;
  std::vector<std::string> client_ips;
  uint16_t nclient_threads;

  // takes n, but this should be the number of shards
  // there are nsequence_spaces shards == (nphys machines/3) * N_ZKTHREADS shards
  size_t znode_to_shard_idx(std::string name) {
    (void) name;
    return 0; //std::hash<std::string>{}(name) % // zk_session_nums.size();
  }

  bool in_snapshot_requests_map(size_t rpc) {
    auto it = snapshot_requests.find(rpc);
    return it != snapshot_requests.end();
  }

  bool in_proxies_map(uint16_t pid) {
    auto it = proxies.find(pid);
    return it != proxies.end();
  }

  WorkerContext() {
    session_num_vec.resize(16);
    my_ip = FLAGS_my_ip;

    for (size_t i = 0; i < nsequence_spaces; i++) {
      received_ms_seqnums.push_back(new Bitmap());
      received_ms_seqnums[i]->assign_sequence_space_and_f_name(i);
    }

    std::string np = FLAGS_nextproxy0_ip;
    only_proxy = np.empty();
    for (size_t i = 0; i < 8; i++) {
      sent[i] = false;
    }
  }

  ~WorkerContext() {
#if !NO_ERPC
    for (size_t i = 0; i < 3; i++) {
      rpc->free_msg_buffer(req_msgbuf);
      rpc->free_msg_buffer(resp_msgbuf);
    }
#endif
  }

  uint16_t get_thread_id(void) {
    return thread_id;
  }

  bool am_gc_leader() {
    return (FLAGS_proxy_id_start + thread_id == 0) &&
        raft_is_leader(proxies[FLAGS_proxy_id_start + thread_id]->raft);
  }

  void allocate_gc_mbufs() {
    req_msgbuf = rpc->alloc_msg_buffer_or_die(
        GC_PAYLOAD_SIZE(nsequence_spaces));
    resp_msgbuf = rpc->alloc_msg_buffer_or_die(1);
    for (size_t i = 0; i < 3; i++) {
      gc_req_msgbufs[i] = rpc->alloc_msg_buffer_or_die(
          GC_PAYLOAD_SIZE(nsequence_spaces));
      gc_resp_msgbufs[i] = rpc->alloc_msg_buffer_or_die(1);
    }
  }

  void update_avg_batch_size(uint16_t batch_size) {
    avg_batch_size =
        ((avg_batch_size * nbatches) + static_cast<double>(batch_size))
            / (nbatches + 1);
    nbatches++;
  }

  void check_batch_timers() {
#if PRINT_TIMING
    auto start = erpc::get_formatted_time();
#endif
    for (auto timer : batch_timers) {
      timer.check();
    }
#if PRINT_TIMING
    printf("[%s] check_batch_timers [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
  }

  void check_timers() {
#if PRINT_TIMING
    auto start = erpc::get_formatted_time();
#endif
    for (auto timer : batch_timers) {
      timer.check();
    }

    if (kUtilPrinting) {
      util_timers[STAT_TIMER_IDX].check();
      if (kHeartbeats)
        util_timers[SEQ_HEARTBEAT_IDX].check();

      util_timers[GC_TIMER_IDX].check();
    }

#if PRINT_TIMING
    printf("[%s] check_timers [%s]\n", start.c_str(), erpc::get_formatted_time().c_str());
#endif
  }

  void check_gc_timer() {
    util_timers[GC_TIMER_IDX].check();
  }

  void init_util_timers() {
    LOG_INFO("Initiating util timers.\n");
    util_timers[STAT_TIMER_IDX].init(STAT_TIMER_US,
                                     print_stats,
                                     reinterpret_cast<void *>(this));
    util_timers[STAT_TIMER_IDX].start();

    if (kHeartbeats) {
      util_timers[SEQ_HEARTBEAT_IDX].init(SEQ_HEARTBEAT_US,
                                          send_heartbeat,
                                          reinterpret_cast<void *>(this));
      util_timers[SEQ_HEARTBEAT_IDX].start();
    }
  }

  // Must specify the callback
  void reset_heartbeat_timer(void (*callback)(void *)) {
    if (kHeartbeats)
      util_timers[SEQ_HEARTBEAT_IDX].start(callback);
  }

  void stop_heartbeat_timer() {
    if (kHeartbeats) {
      heartbeat_last_call = false;
      reset_heartbeat_timer(send_heartbeat);
      util_timers[SEQ_HEARTBEAT_IDX].stop();
    }
  }

  void confirm_recovery_replication(bool);

  void respond_backup_ready();
};

__inline__ void Proxy::check_notification_queue() {
  // the queue will not be used once all client connections are established
  if (likely(watch_notification_queue.empty())) {
    return;
  }
  while (!watch_notification_queue.empty()) {
    if (c->rpc->is_connected(watch_notification_queue.front()->session_num)) {
      WatchTag *tag = watch_notification_queue.front();
      LOG_SERVER("Sending queued notification\n");
      c->rpc->enqueue_request(tag->session_num,
                              static_cast<uint8_t>(ReqType::kWatchNotification),
                              &tag->req_msgbuf, &tag->resp_msgbuf,
                              notify_cont_func, tag);
      watch_notification_queue.pop();
    } else {
      break;
    }
  }
}

// delete the client connection on behalf of the client
// this should look as if the client submitted the delete
void client_connection_cb(void *_arg){
  Proxy *p = reinterpret_cast<connection_cb_arg_t *>(_arg)->p;
  ClientConnection *cc = reinterpret_cast<connection_cb_arg_t *>(_arg)->cc;
  // this node could have many ephemeral nodes, but we only need to send the
  // id because each server will have the same state
  LOG_SERVER("In client connection cb for cid %d addr %p cxn num %zu\n",
           cc->client_id, reinterpret_cast<void *>(cc),
           cc->client_connection_id);

  // need to submit a multi-op delete for all the nodes in this connection
  if (cc->znodes.empty()) {
    LOG_SERVER("Client connection for %d had no znodes\n", cc->client_id);
    erpc::rt_assert(false, "no znodes???\n");
    return;
  }
  LOG_SERVER("There are %zu eph nodes\n", cc->znodes.size());
  if (LOG_DEBUG_SERVER)
    for (auto znode : cc->znodes) {
      (void) znode; LOG_SERVER("\t%s\n", znode->my_name.c_str());
    }

  ClientOp *op = p->client_op_pool.alloc();

  op->populate(ReqType::kServerDeleteClientConnection,
               get_client_id(p->c->my_ip, p->c->thread_id), 0,
               p->proxy_id, nullptr, 0, p, p->c, op->zk_payload);
  delete_client_connection_t &dcc = op->zk_payload.delete_client_connection;
  dcc.client_id = cc->client_id;
  dcc.deleted = false;
  dcc.client_connection_id = cc->client_connection_id;
  dcc.nshards = 0;

  // adding to batch will eventually execute it
  p->add_op_to_batch(op);
  p->current_batch->acked_ops[{op->client_id, op->client_reqid}];

  op->zk_payload.delete_client_connection.client_id = cc->client_id;

  delete reinterpret_cast<connection_cb_arg_t *>(_arg);
}

void Proxy::send_connect_request_and_store_session(std::string ip) {
  std::string uri;
  std::string port = ":31850";

  uri = ip + port;
  LOG_INFO("Connecting to client with uri %s with %d threads each\n",
           uri.c_str(), c->nclient_threads);
  for (int i = 0; i < c->nclient_threads; i++) {
    LOG_INFO("Connecting to client with uri %s thread %d\n",
             uri.c_str(), i);
    auto cid = get_client_id(ip, static_cast<size_t>(i));
    c->cid_to_session_num[cid] = c->rpc->create_session(uri, i);
    fmt_rt_assert(c->cid_to_session_num[cid] >= 0,
                  "Failed to create client session cid %d sn %d\n",
                  cid, c->cid_to_session_num[cid]);
  }
}
void Proxy::connect_to_clients() {
  LOG_INFO("Establishing connection to %zu clients\n", c->client_ips.size());
  for (const std::string &ip : c->client_ips) {
    send_connect_request_and_store_session(ip);
  }
  LOG_INFO("My RPC ID %u\n", c->rpc->get_rpc_id());
  LOG_INFO("Waiting for client connections\n");
  for (auto pair : c->cid_to_session_num) {
    int session_num = pair.second;
    LOG_INFO("Making sure session_num %d is connected\n", session_num);
    while (!c->rpc->is_connected(session_num) && !force_quit) {
      c->rpc->run_event_loop_once();
      call_raft_periodic(0);
    }
  }
  LOG_INFO("Connected to all clients\n");
}

__inline__ void Proxy::notify(
    const std::string &znode_name, WatchType type, uint16_t client_id) {
  LOG_SERVER("Notifying a client for node %s type %u\n", znode_name.c_str(),
             static_cast<uint8_t>(type));
  WatchTag *tag = watch_tag_pool.alloc();
  tag->set(c, this);
  tag->alloc();
  tag->session_num = c->cid_to_session_num[client_id];

  auto *watch_notification = reinterpret_cast<watch_notification_t *>(
      tag->req_msgbuf.buf);
  watch_notification->type = type;
  strcpy(watch_notification->name, znode_name.c_str());

  if (!c->rpc->is_connected(tag->session_num)) {
    LOG_SERVER("Queueing notification for node %s type %u\n",
             znode_name.c_str(), static_cast<uint8_t>(type));
    LOG_SERVER("tag %p ses num %d req %p resp %p connected %d\n",
             reinterpret_cast<void *>(tag),
             tag->session_num,
             reinterpret_cast<void *>(&tag->req_msgbuf),
             reinterpret_cast<void *>(&tag->resp_msgbuf),
             c->rpc->is_connected(tag->session_num));
    watch_notification_queue.push(tag);
  } else {
    c->rpc->enqueue_request(tag->session_num,
                            static_cast<uint8_t>(ReqType::kWatchNotification),
                            &tag->req_msgbuf, &tag->resp_msgbuf,
                            notify_cont_func, tag);
  }
}

__inline__ void Proxy::check_watches_map_and_notify(
    const std::string &znode_name, WatchType type, watch_map_t &watch_map) {
  LOG_SERVER("Thread %zu: in check_watches_map_and_notify\n", c->thread_id);
  if (watch_map[znode_name].empty()) {
    LOG_SERVER("No watches for %s of type %u\n",
               znode_name.c_str(), static_cast<uint8_t>(type));
  }

  for (auto cid : watch_map[znode_name]) {
    notify(znode_name, type, cid);
  }
  // all clients will be notified with notify() and watches are triggered
  // exactly once.
  watch_map[znode_name].clear();
}

void WatchTag::alloc() {
  if (!allocated) {
    req_msgbuf = c->rpc->alloc_msg_buffer_or_die(sizeof(watch_notification_t));
    // responses are just an ack
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(1);
    allocated = true;
  }
}

// response is just an ack
__inline__ void notify_cont_func(void *, void *_tag) {
  auto *tag = reinterpret_cast<WatchTag *>(_tag);
  tag->p->watch_tag_pool.free(tag);
}

// used for both serializing and deserializing
template<class Archive>
// took a version
void Proxy::serialize(Archive &ar, const unsigned int) {
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & last_included_term;
  ar & last_included_index;
  ar & proxy_id;
  ar & max_received_seqnum;
  ar & highest_seen_seqnum;
  ar & batch_counter;
  ar & op_counter;
  ar & seq_req_id;
  // todo these calls were for testing
  //  remove and retest when the machines are available
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & deleted_seq_req_ids;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & last_ackd_crid;
  ar & highest_sequenced_crid;
  ar & highest_del_seq_req_id;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & got_leader;
  ar & need_seqnum_batch_map;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & done_batch_map;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & client_retx_done_map;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & client_retx_in_progress_map;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & ops_with_handles;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & done_batch_ids;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & highest_cons_batch_id;
  if (Archive::is_saving::value) c->check_gc_timer();
  ar & dummy_entry_response;
  if (Archive::is_saving::value) c->check_gc_timer();
  for (size_t i = 0; i < nsequence_spaces; i++) {
    if (Archive::is_saving::value) {
      received_seqnums[i] = c->received_ms_seqnums[i];
      ar & received_seqnums[i];
    } else { // deserializing
      ar & received_seqnums[i];
    }
  }

  // ----- [De]Serialize the ZooKeeper state -----
  ar & znodes;
  ar & read_node_watches;
  ar & exists_true_watches;
  ar & exists_false_watches;
  ar & get_children_watches;
  ar & client_connections;
  ar & client_connection_numbers;
  if (Archive::is_saving::value) c->check_gc_timer();
}

// 1. Figure out which request_ids are missing, i.e., have not been replicated
// 2. For the missing request_ids, contact the sequencer for the seqnum (if available)
//      ---these requests may have gotten seqnums but we don't know which client ops
//      they correspond to. Therefore we must assign noops to them.
// I.e., if it's in appended_batch_map, ignore it; if it's in done_map, ignore
void Proxy::gain_leadership() {
  std::vector<size_t> done_srids;
  std::vector<size_t> appended_srids;
  std::vector<size_t> diff;
  std::vector<size_t> missing_srids;

  // to prevent from adding client requests to the batch that this
  // code expects to be empty...
  // we also shouldn't accept client request while we are still becoming the
  // leader
  gaining_leadership = true;

  init_timers();

  // init ZooKeeper timers
  for (auto pair : client_connections) {
    ClientConnection *cc = pair.second;
    auto *arg = new connection_cb_arg_t;
    arg->p = this;
    arg->cc = cc;
    LOG_INFO("Client Connection address %p\n",
             reinterpret_cast<void *>(cc));
    cc->timer.init(kClientConnectionTimeout, client_connection_cb, arg);
    cc->timer.start();
  }

  if (current_batch != nullptr && current_batch->batch_size() != 0) {
    complete_and_send_current_batch();
    create_new_batch();
  } else if (current_batch == nullptr) {
    create_new_batch();
  } else {// the batch is empty but created, don't advance batch_counter
    LOG_ERROR("PID: %d cur batch is empty but was already created, likely"
              "regaining leadership\n", proxy_id);
  }
  LOG_ERROR("PID: %d starting from batch id %zu\n",
            proxy_id, current_batch->batch_id);

  auto start = erpc::rdtsc();
  // Replicate dummy entry to commit requests from previous terms in case of
  // no incoming requests
  replicate_dummy();
  raft_apply_all(raft);
  call_raft_periodic(0);

  // what if the proxy has not been caught up?
  // this is not correct, we need to wait here until the dummy is replicated
  // is adding this correct?
  while (!dummy_replicated) {
    c->rpc->run_event_loop(1);
    raft_apply_all(raft);
    call_raft_periodic(0);
  }

  LOG_ERROR("PID: %u time to replicate dummy %Lf\n", proxy_id,
            cycles_to_usec(erpc::rdtsc() - start));
  debug_print(1, "applied dummy\n");
  LOG_ERROR("PID: %d scanning through appended_batch_map\n", proxy_id);

  for (auto &elem : appended_batch_map) {
    // Call raft_periodic every 5ms to prevent failover
    call_raft_periodic(5);
    Batch *batch = elem.second;
    debug_print(DEBUG_FAILOVER, "Found %zu in appended_batch_map\n",
                batch->seq_req_id);
    appended_srids.push_back(batch->seq_req_id);
  }

  // this was not the max if not applied up to dummy
  LOG_ERROR("PID %d: Done looking through appended batch map! "
            "Found %zu srids in appended batch map "
            "Max used srid is %zu\n",
            proxy_id, appended_srids.size(), seq_req_id - 1);

  // can probably improve performance by just having these arrays go from
  // highest_del_seq_req_id to seq_req_id... rather than from 1.
  std::vector<size_t> incomplete_srids(seq_req_id - 1 - highest_del_seq_req_id);
  // we know everything up to and including highest_del_seq_req_id is done.
  std::iota(incomplete_srids.begin(), incomplete_srids.end(),
            highest_del_seq_req_id + 1);

  LOG_ERROR("PID %d: Done creating incomplete_srids vector size %zu\n",
            proxy_id, incomplete_srids.size());

  // add all deleted srids
  done_srids.resize(highest_del_seq_req_id);
  std::iota(done_srids.begin(), done_srids.end(), 1);
  // there could be done srids later than hsrid
  for (auto &elem : done_batch_map) {
    call_raft_periodic(5);
    Batch *batch = elem.second;
    done_srids.push_back(batch->seq_req_id);
  }
  int dsrid_p = done_srids.size();

  LOG_ERROR("PID %d: highest_del_seq_req_id %zu top (0 if empty) %zu\n",
            proxy_id, highest_del_seq_req_id,
            !deleted_seq_req_ids.empty() ? deleted_seq_req_ids.top() : 0);
  // you can't iterate over pqueues
  // take them out, add to done_srids
  // the srids we deleted will not be in the done map
  std::vector<size_t> you_cannot_it_over_pq;
  while (!deleted_seq_req_ids.empty()) {
    call_raft_periodic(5);
    you_cannot_it_over_pq.push_back(deleted_seq_req_ids.top());
    done_srids.push_back(deleted_seq_req_ids.top());
    deleted_seq_req_ids.pop();
  }
  int dsrid_a = done_srids.size();

  erpc::rt_assert(dsrid_a >= dsrid_p, "dsrid shrunk?\n");
  LOG_ERROR("PID %d: added %zu srids from the pq to done_srids dsrids before "
            "%d after %d diff %d\n",
            proxy_id, you_cannot_it_over_pq.size(), dsrid_p, dsrid_a,
            dsrid_a - dsrid_p);
  // put them back in
  for (uint64_t i : you_cannot_it_over_pq) {
    deleted_seq_req_ids.push(i);
  }

  LOG_ERROR("PID %d: Done looking through done batch map\n", proxy_id);

  std::sort(incomplete_srids.begin(), incomplete_srids.end());
  std::sort(appended_srids.begin(), appended_srids.end());
  std::sort(done_srids.begin(), done_srids.end());

  LOG_ERROR("PID: %d Searching for intersection and difference...\n", proxy_id);
  std::set_difference(incomplete_srids.begin(), incomplete_srids.end(),
                      done_srids.begin(), done_srids.end(),
                      std::back_inserter(diff));
  std::sort(diff.begin(), diff.end());
  std::set_difference(diff.begin(), diff.end(),
                      appended_srids.begin(), appended_srids.end(),
                      std::back_inserter(missing_srids));

  LOG_ERROR("PID %d: Found %zu srids to noop\n",
            proxy_id,
            missing_srids.size());


  // Request the missing seq_req_ids from the sequencer; these will be noops
  // The batch size will be set later, when the response from the sequencer
  // comes back.
  for (size_t i : missing_srids) {
    current_batch->seq_req_id = i;
    debug_print(1, "%zu requesting missing srid %zu\n", c->thread_id, i);
    appended_batch_map[current_batch->batch_id] = current_batch;
    erpc::rt_assert(current_batch->batch_client_ops.empty(),
                    "there are client ops in a missing srid batch\n");
    for (size_t j = 0; j < nsequence_spaces; j++) {
      erpc::rt_assert(current_batch->seq_reqs[j].seqnum == 0,
                      "seqnum not zero in empty noop batch\n");
      erpc::rt_assert(current_batch->seq_reqs[j].batch_size == 0,
                      "batch_size not zero in empty noop batch\n");
    }
    current_batch->request_seqnum(true);
    create_new_batch();
  }
  LOG_ERROR("[%zu] done responding and taking over as leader, "
            "found %zu missing seq_req_ids\n",
            c->thread_id, missing_srids.size());

  gaining_leadership = false;
}

inline void
Batch::free() {
  for (auto op : batch_client_ops) {
    op->free();
  }
  batch_client_ops.clear();
  acked_ops.clear();
  proxy->batch_pool.free(this);
}

// necessary for boost::serialize
Proxy::Proxy() {
  received_seqnums = new Bitmap *[nsequence_spaces];
}

// Constructor
Proxy::Proxy(WorkerContext *ctx, bool a, int p) {
  c = ctx;
  am_leader = a;
  proxy_id = p;

  received_seqnums = new Bitmap *[nsequence_spaces];

  debug_print(DEBUG_SEQ, "New logical proxy as %s with pid %d\n",
              am_leader ? "leader" : "follower", p);

  // initialize Raft
  cycles_per_msec = erpc::ms_to_cycles(1, erpc::measure_rdtsc_freq());
  raft_periodic_tsc = erpc::rdtsc();

  replica_data[my_raft_id].node_id = my_raft_id;
  replica_data[my_raft_id].wc = ctx;
  replica_data[my_raft_id].pid = p;

  replica_data[replica_1_raft_id].node_id = replica_1_raft_id;
  replica_data[replica_1_raft_id].idx = MachineIdx::REPLICA_1;
  replica_data[replica_1_raft_id].pid = p;
  replica_data[replica_1_raft_id].nOutstanding = 0;

  replica_data[replica_2_raft_id].node_id = replica_2_raft_id;
  replica_data[replica_2_raft_id].idx = MachineIdx::REPLICA_2;
  replica_data[replica_2_raft_id].pid = p;
  replica_data[replica_2_raft_id].nOutstanding = 0;

  // init raft struct
  raft = raft_new();

  // add each replica to the raft struct
  raft_add_node(raft, &replica_data[my_raft_id],
                replica_data[my_raft_id].node_id, true);
  raft_add_node(raft, &replica_data[replica_1_raft_id],
                replica_data[replica_1_raft_id].node_id, false);
  raft_add_node(raft, &replica_data[replica_2_raft_id],
                replica_data[replica_2_raft_id].node_id, false);

  raft_cbs_t *raft_callbacks = set_raft_callbacks();
  // Raft does a memcpy for pointers to the callbacks, ok to have local
  raft_set_callbacks(raft, raft_callbacks, &replica_data[my_raft_id]);

  fflush(stdout);
}

void
Proxy::deserial_proxy(WorkerContext *ctx, bool a, int p) {
  c = ctx;
  am_leader = a;
  proxy_id = p;
}

inline void
Proxy::init_timers() {
  printf("before vec at timers2\n");
  fflush(stdout);
  c->batch_timers.at(proxy_id).init(
      FLAGS_batch_to, batch_to_cb,
      reinterpret_cast<void *>(this));
  batch_timer = &c->batch_timers[proxy_id];
}

inline void
Proxy::reset_batch_timer() {
  batch_timer->start();
}

inline void
ClientOp::free() {
  batch->proxy->client_op_pool.free(this);
}

inline void
Tag::alloc_msgbufs(WorkerContext *c, Batch *batch, bool failover) {
  this->c = c;
  this->batch = batch;
  this->batch_id = batch->batch_id;
  this->failover = failover;

  size_t bufsize = std::max(
      sequencer_payload_size(nsequence_spaces),
      client_payload_size(nsequence_spaces));

  if (!msgbufs_allocated) {
#if !NO_ERPC
    req_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
#else
    req_msgbuf = new char[bufsize];
    resp_msgbuf = new char[bufsize];
#endif
    msgbufs_allocated = true;
  }
}

// unused in proxykeeper. this version is buggy, fixed in CATSKeeper
// only used for client operations
inline void
Tag::alloc_msgbufs(WorkerContext *_c) {
  this->c = _c;

  // should be max of all structs we might use this tag for
  // because we only allocate them once
  size_t bufsize = std::max(sizeof(create_node_t), sizeof(client_payload_t));

  if (!msgbufs_allocated) {
#if !NO_ERPC
    req_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(bufsize);
#else
    req_msgbuf = new char[bufsize];
    resp_msgbuf = new char[bufsize];
#endif
    msgbufs_allocated = true;
  }
}

inline void
send_noop_to_client_cont_func(void *_c, void *_tag) {
  (void) _c;
  Tag *tag = reinterpret_cast<Tag *>(_tag);
  Batch *b = tag->batch;

  // this batch is actually completed immediately
  // there is only one request
  b->completed_ops++;
  LOG_FAILOVER("Received response of noop batch from client %zu %d\n",
               b->batch_size(), b->completed_ops);

  b->proxy->tag_pool.free(tag);

  if (b->completed_ops == b->batch_size()) {
    LOG_FAILOVER("Freeing a noop batch id %zu\n", b->batch_id);
    b->proxy->done_batch_map[b->batch_id] = b;
    b->proxy->push_and_update_highest_cons_batch_id(b->batch_id);
  }
}

inline void
send_noop_to_client(Batch *batch, ClientOp *op)
{
  erpc::rt_assert(false, "zk not ready for noops");
  Proxy *proxy = batch->proxy;
  WorkerContext *c = batch->c;

  Tag *tag = proxy->tag_pool.alloc();
  tag->alloc_msgbufs(c, batch, false);

  uint8_t session_num =
      c->session_num_vec[static_cast<uint8_t>(MachineIdx::CLIENT)];
  erpc::rt_assert(c->rpc->is_connected(session_num),
                  "Not connected to the client for writing noops!");

  c->rpc->resize_msg_buffer(&tag->req_msgbuf,
                            client_payload_size(nsequence_spaces));
  auto *payload =
      reinterpret_cast<client_payload_t *>(tag->req_msgbuf.buf);

  payload->reqtype = ReqType::kNoop;

  printf("nooping the following seq_reqs:\n\t");
  print_seqreqs(op->seq_reqs, nsequence_spaces);

  c->rpc->enqueue_request(session_num,
                          static_cast<uint8_t>(ReqType::kRecordNoopSeqnum),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          send_noop_to_client_cont_func,
                          reinterpret_cast<void *>(tag));
}

void ClientOp::set_seq_reqs() {
  for (size_t i = 0; i < nsequence_spaces; i++) {
    seq_reqs[i].seqnum = 0;
    seq_reqs[i].batch_size = 0;
  }

  if (reqtype == ReqType::kCreateZNode) {
    if (std::string(zk_payload.create_node.name) ==
        std::string("/")) {
      seq_reqs[c->znode_to_shard_idx("/")].batch_size++;
    } else {
      seq_reqs[c->znode_to_shard_idx(zk_payload.create_node.name)].batch_size++;
      seq_reqs[c->znode_to_shard_idx(get_parent(zk_payload.create_node.name))].batch_size++;
    }
  } else if (reqtype == ReqType::kRenameZNode) {
    if (std::string(zk_payload.rename_node.from) ==
        std::string("/")) {
      erpc::rt_assert(false, "cannot rename root node!\n");
    } else {
      // delete on from
      seq_reqs[c->znode_to_shard_idx(zk_payload.rename_node.from)].batch_size++;
      // rename on parent
      seq_reqs[c->znode_to_shard_idx(get_parent(zk_payload.rename_node.from))].batch_size++;
      // create on to
      seq_reqs[c->znode_to_shard_idx(zk_payload.rename_node.to)].batch_size++;
    }
  } else if (reqtype == ReqType::kWriteZNode) {
    seq_reqs[c->znode_to_shard_idx(zk_payload.write_node.name)].batch_size++;
  } else if (reqtype == ReqType::kReadZNode) {
    seq_reqs[c->znode_to_shard_idx(zk_payload.read_node.name)].batch_size++;
  } else if (reqtype == ReqType::kDeleteZNode) {
    seq_reqs[c->znode_to_shard_idx(zk_payload.delete_node.name)].batch_size++;
    seq_reqs[c->znode_to_shard_idx(get_parent(zk_payload.delete_node.name))].batch_size++;
  }
  // remove before experiments
  for (size_t i = 0; i < nsequence_spaces; i++) {
    erpc::rt_assert(seq_reqs[i].batch_size <= 3,
                    "in set_seq_reqs with batch size >3");
  }
}

// Hacky but need to be able to run event loop over smaller timescales
inline void
run_event_loop_us(erpc::Rpc<erpc::CTransport> *rpc, size_t timeout_us) {
  size_t timeout_tsc = erpc::us_to_cycles(timeout_us, freq_ghz);
  size_t start_tsc = erpc::rdtsc();  // For counting timeout_us
  size_t now;

  while (true) {
    rpc->run_event_loop_once();
    now = erpc::dpath_rdtsc();
    if (unlikely(now - start_tsc > timeout_tsc)) break;
  }
}

inline static long double
cycles_to_usec(uint64_t cycles) {
  return (static_cast<long double>(cycles) /
      static_cast<long double>(rte_get_tsc_hz())) * USEC_PER_SEC;
}

// this no longer really makes sense since one pid per thread
inline std::string Proxy::p_string(void) {
  std::ostringstream ret;
  ret << "PID: " << proxy_id;
  return ret.str();
}

#endif // PROXY_MAIN_H
