#ifndef ZK_SERVER_H
#define ZK_SERVER_H

#define IDENT(x) x
#define XSTR(x) #x
#define STR(x) XSTR(x)
#define PATH(x, y) STR(IDENT(x)IDENT(y))

#define Fname /common.h

#include PATH(COMMON_DIR, Fname)

#define DEBUG 1

#include "common.h"
#include "timer.h"
extern size_t numa_node;
extern volatile bool force_quit;

DEFINE_string(my_ip, "",
              "This Corfu Server's IP address for eRPC setup");
DEFINE_string(client_ips, "",
              "All of the clients IPs for watches");
DEFINE_int64(nclient_threads, 0,
             "number of client threads per machine");
DEFINE_string(proxy_ip_0, "",
              "IP address for the proxy n");
DEFINE_string(proxy_ip_1, "",
              "IP address for the proxy n");
DEFINE_string(proxy_ip_2, "",
              "IP address for the proxy n");
DEFINE_uint32(proxy_id, 0,
              "Start of the proxy ids");
DEFINE_uint32(nzk_servers, 0,
              "Number of CATSKeeper servers.");

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

// Fwd decls
class ZKContext;
void respond_to_request(ZKContext *c,
                        erpc::ReqHandle *req_handle,
                        void *buf,
                        size_t size);

class Operation {
 public:
  OpType op_type;
  uint64_t seqnum;
  erpc::ReqHandle *req_handle;
  char *buf;

  union {
    create_node_t node;
    write_node_t write_node;
    read_node_t read_node;
    rename_node_t rename_node;
    add_child_t add_child;
    remove_child_t remove_child;
    rename_child_t rename_child;
    delete_node_t delete_node;
    exists_t exists;
    get_children_t get_children;
    delete_client_connection_t delete_client_connection;
  };

  Operation() {};

  void set(uint64_t _seqnum, create_node_t *_node) {
    op_type = OpType::kCreateNode;
    seqnum = _seqnum;
    node = *_node;
  }

  // children contains first a size_t saying how many children
  // this is for rename_node()
  void set(uint64_t _seqnum, create_node_t *_node, char *_children) {
    op_type = OpType::kCreateNodeWithChildren;
    seqnum = _seqnum;
    node = *_node;
    buf = _children;
  }

  // children contains first a size_t saying how many children
  void set(uint64_t _seqnum, write_node_t *_write_node) {
    op_type = OpType::kWriteNode;
    seqnum = _seqnum;
    write_node = *_write_node;
  }

  void set(uint64_t _seqnum, rename_node_t *_rename_node) {
    op_type = OpType::kRenameNode;
    seqnum = _seqnum;
    rename_node = *_rename_node;
  }

  void set(uint64_t _seqnum, delete_node_t *_delete_node,
           erpc::ReqHandle *_req_handle) {
    req_handle = _req_handle;
    op_type = OpType::kDeleteAndReadNode;
    seqnum = _seqnum;
    delete_node = *_delete_node;
  }

  void set(uint64_t _seqnum, delete_node_t *_delete_node) {
    op_type = OpType::kDeleteNode;
    seqnum = _seqnum;
    delete_node = *_delete_node;
  }

  void set(uint64_t _seqnum, read_node_t *_read_node,
           erpc::ReqHandle *_req_handle) {
    req_handle = _req_handle;
    op_type = OpType::kReadNode;
    seqnum = _seqnum;
    read_node = *_read_node;
  }

  void set(uint64_t _seqnum, add_child_t *_child) {
    op_type = OpType::kAddChild;
    seqnum = _seqnum;
    add_child = *_child;
  }

  void set(uint64_t _seqnum, remove_child_t *_child) {
    op_type = OpType::kRemoveChild;
    seqnum = _seqnum;
    remove_child = *_child;
  }

  void set(uint64_t _seqnum, rename_child_t *_rename_child) {
    op_type = OpType::kRenameChild;
    seqnum = _seqnum;
    rename_child = *_rename_child;
  }

  void set(uint64_t _seqnum, exists_t *_exists, erpc::ReqHandle *_req_handle) {
    req_handle = _req_handle;
    op_type = OpType::kExists;
    seqnum = _seqnum;
    exists = *_exists;
  }

  void set(uint64_t _seqnum, get_children_t *_get_children,
           erpc::ReqHandle *_req_handle) {
    req_handle = _req_handle;
    op_type = OpType::kGetChildren;
    seqnum = _seqnum;
    get_children = *_get_children;
  }

  void set(uint64_t _seqnum, delete_client_connection_t *_dcc,
           erpc::ReqHandle *_req_handle) {
    op_type = OpType::kDeleteClientConnection;
    seqnum = _seqnum;
    delete_client_connection = *_dcc;
    req_handle = _req_handle;
  }

};

class ZNode {
 public:
  std::string my_name;
  std::set<std::string> children;
  char data[MAX_ZNODE_DATA];
  int flags = 0; // todo
  int32_t version = 0;

  Timer timer; // delete timer for ephemeral
  client_id_t client_id; // owning client for ephemeral

  // In order to AppMemPool
  ZNode() {}
  ~ZNode() {
    printf("IN DESTRUCTOR FOR ZNODE!?!?!?\n");
    fflush(stdout);
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
    if (!children.erase(from)) return false; // was not in set
    add_child(to);
    return true;
  }

};

struct CmpOperation {
  bool operator()(const Operation *a, const Operation *b) const {
    return a->seqnum > b->seqnum;
  }
};

uint16_t
get_client_id(std::string ip, size_t thread_id) {
  std::istringstream ss(ip);
  std::string last_octet;
  getline(ss, last_octet, '.');
  getline(ss, last_octet, '.');
  getline(ss, last_octet, '.');
  getline(ss, last_octet, '.');

  auto client_id = static_cast<uint16_t>(std::stoul(last_octet));
  client_id <<= 4;
  client_id |= thread_id;
  return client_id;
}

class WatchTag {
 public:
  int session_num = -1;
  ZKContext *c = nullptr;
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
  bool allocated = false;

  void set(ZKContext *_c) {c = _c;}
  void alloc();
};

// response is just an ack
__inline__ void notify_cont_func(void *_c, void *_tag);

// znode name to a vector of client_ids that have a watch
typedef std::unordered_map<std::string, std::vector<client_id_t>> watch_map_t;

class ClientConnection {
 public:
  client_id_t client_id;
  Timer timer;
  uint64_t client_connection_id;
  // all znodes the client has created during this connection
  std::vector<ZNode *> znodes;
  std::set<uint8_t> shards;

  ClientConnection(client_id_t cid) : client_id(cid) {}
};

typedef struct connection_cb_arg {
  ZKContext *c;
  ClientConnection *cc;
} connection_cb_arg_t;
void client_connection_cb(void *);

class ZKContext : public ThreadContext {
 public:
  struct timespec tput_t0;
  app_stats_t *app_stats;
  uint64_t stat_resp_tx_tot = 0;

  // we are a client that sometimes deletes ephemeral nodes
  client_id_t my_client_id;
  client_reqid_t cur_req_id = 0;
  client_reqid_t highest_cons_reqid = 0;

  AppMemPool<Operation> op_pool;
  AppMemPool<ZNode> znode_pool;
  AppMemPool<WatchTag> watch_tag_pool;

  // the main struct of filenames to data and its children
  // should be pointer to ZNode?
  std::unordered_map<std::string, ZNode *> znodes;

  // the queue of operations ordered by seqnum
  uint64_t cur_seqnum = 0;
  uint64_t max_received_seqnum = 0;
  inline void set_max_received_seqnum(uint64_t sn) {
    max_received_seqnum = max_received_seqnum < sn ? sn : max_received_seqnum;
  }
  std::priority_queue<Operation *, std::vector<Operation *>, CmpOperation>
      op_queue;

  // we maintain a set of watch maps, per each operation type that sets a watch
  // the op that triggers a watch pulls from these maps
  watch_map_t read_node_watches;
  watch_map_t exists_true_watches;
  watch_map_t exists_false_watches;
  watch_map_t get_children_watches;

  // eRPC does not check queue unless it has received a response
  std::queue<WatchTag *> watch_notification_queue;

  std::unordered_map<client_id_t, ClientConnection *> client_connections;
  // map of client_id to the current connection number
  std::unordered_map<client_id_t, uint64_t> client_connection_numbers;

  ClientConnection *in_client_connections(client_id_t client_id) {
    return client_connections.find(client_id) !=
      client_connections.end() ? client_connections[client_id] : nullptr;
  }

  std::unordered_map<uint16_t, int> cid_to_session_num;
  std::vector<std::string> client_ips;
  uint16_t nclient_threads;

  // I don't think need to use iterator, it is pointer which is nullptr
  ZNode *in_znodes(std::string name) {
    try {return znodes.at(name);}
    catch(std::out_of_range &) {return nullptr;}
  }

  void check_client_connections() {
    for (auto pair : client_connections) {
      ClientConnection *cc = pair.second;
      cc->timer.check(); // calls the callback
    }
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
      return false;
    } catch (std::out_of_range &) {
      return true;
    }
    // will terminate on other exceptions
  }

  int proxy_session_num;
  void connect_to_proxy() {
    std::string ip = FLAGS_proxy_ip_0;
    std::string uri;
    std::string port = ":31850";

    uri = ip + port;
    LOG_INFO("Connecting to proxy with uri %s\n", uri.c_str());
    proxy_session_num = rpc->create_session(uri, 0);
    fmt_rt_assert(proxy_session_num >= 0,
                 "Failed to proxy session sn %d\n", proxy_session_num);
    while (!rpc->is_connected(proxy_session_num)) {
      rpc->run_event_loop_once();
    }
    LOG_INFO("Connected to proxy with uri %s\n", uri.c_str());
  }

  void send_connect_request_and_store_session(std::string ip) {
    std::string uri;
    std::string port = ":31850";

    uri = ip + port;
    LOG_INFO("Connecting to client with uri %s with %d threads each\n",
             uri.c_str(), nclient_threads);
    for (int i = 0; i < nclient_threads; i++) {
      LOG_INFO("Connecting to client with uri %s thread %d\n",
               uri.c_str(), i);
      auto cid = get_client_id(ip, static_cast<size_t>(i));
      cid_to_session_num[cid] = rpc->create_session(uri, i);
      fmt_rt_assert(cid_to_session_num[cid] >= 0,
                      "Failed to create client session cid %d sn %d\n",
                      cid, cid_to_session_num[cid]);
    }
  }

  void connect_to_clients() {
    LOG_INFO("Establishing connection to %zu clients\n", client_ips.size());
    for (const std::string &ip : client_ips) {
      send_connect_request_and_store_session(ip);
    }
    LOG_INFO("My RPC ID %u\n", rpc->get_rpc_id());
    LOG_INFO("Waiting for client connections\n");
    for (auto pair : cid_to_session_num) {
      int session_num = pair.second;
      LOG_INFO("Making sure session_num %d is connected\n", session_num);
      while (!rpc->is_connected(session_num) && !force_quit)
        rpc->run_event_loop_once();
    }
    LOG_INFO("Connected to all clients\n");
  }

  __inline__ void check_notification_queue() {
    // the queue will not be used once all client connections are established
    if (likely(watch_notification_queue.empty())) {
      return;
    }
    while (!watch_notification_queue.empty()) {
      if (rpc->is_connected(watch_notification_queue.front()->session_num)) {
        WatchTag *tag = watch_notification_queue.front();
        rpc->enqueue_request(tag->session_num,
                             static_cast<uint8_t>(ReqType::kWatchNotification),
                             &tag->req_msgbuf, &tag->resp_msgbuf,
                             notify_cont_func, tag);
        watch_notification_queue.pop();
      } else {
        break;
      }
    }
  }

  __inline__ void notify(
      const std::string &znode_name, WatchType type, uint16_t client_id) {
    LOG_SERVER("Notifying a client for node %s type %u\n", znode_name.c_str(),
               static_cast<uint8_t>(type));
    WatchTag *tag = watch_tag_pool.alloc();
    tag->set(this);
    tag->alloc();
    tag->session_num = cid_to_session_num[client_id];

    auto *watch_notification = reinterpret_cast<watch_notification_t *>(
        tag->req_msgbuf.buf);
    watch_notification->type = type;
    strcpy(watch_notification->name, znode_name.c_str());

    if (!rpc->is_connected(tag->session_num)) {
      LOG_SERVER("Queueing notification for node %s type %u\n"
               , znode_name.c_str(),
               static_cast<uint8_t>(type));
      watch_notification_queue.push(tag);
    } else {
      rpc->enqueue_request(tag->session_num,
                           static_cast<uint8_t>(ReqType::kWatchNotification),
                           &tag->req_msgbuf, &tag->resp_msgbuf,
                           notify_cont_func, tag);
    }
  }

  __inline__ void check_watches_map_and_notify(const std::string &znode_name,
                                               WatchType type,
                                               watch_map_t &watch_map) {
    LOG_SERVER("Thread %zu: in check_watches_map_and_notify\n", thread_id);
    if (watch_map.count(znode_name) == 0) return;
    if (watch_map[znode_name].empty()) {
      return;
    }

    for (auto cid : watch_map[znode_name]) {
      notify(znode_name, type, cid);
    }
    // all clients will be notified with notify() and watches are triggered
    // exactly once.
    watch_map[znode_name].clear();
  }

  __inline__ void execute_create_node(Operation *op) {
    LOG_SERVER("Thread %zu: executing create_node (ephemeral(2)? %u) sn %zu %s"
               " %d\n",
          thread_id, static_cast<uint8_t>(op->node.options),
          op->seqnum, op->node.name, op->node.not_owner);

    // this could just be updating a connection with new shards, it would be
    // better to have a different operation, but just catching here for now...
    if (op->node.not_owner) {
      try {
        ClientConnection *cc = client_connections.at(op->node.client_id);
        for (size_t i = 0; i < op->node.nshards; i++) {
          cc->shards.insert(op->node.shards[i]);
        }
      } catch (std::out_of_range &) {}
      return;
    }

    std::string name(op->node.name);
    auto *znode = znode_pool.alloc();
    znode->reset(&name, op->node.data);
    auto *old = in_znodes(name);
    if (old) znode_pool.free(old); // if there was one there update it

    // this allows it to be used for renames, proxy sends previous version
    znode->version = op->node.version;
    znodes[znode->my_name] = znode;

    if (op->node.options == CreateNodeOptions::kEphemeral) {
      ClientConnection *cc = client_connections[op->node.client_id];
      if (cc) {
        // existing connection, add znode and reset the timer
        cc->znodes.push_back(znode);
        cc->timer.reset();
      } else {
        // we need to create the connection
        cc = new ClientConnection(op->node.client_id);
        client_connections[op->node.client_id] = cc;
        cc->znodes.push_back(znode);
        cc->client_connection_id = op->node.client_connection_id;
        try { erpc::rt_assert(cc->client_connection_id >=
            client_connection_numbers.at(op->node.client_id),
            "connection number going backwards\n");
        } catch (std::out_of_range &) { }
        client_connection_numbers[op->node.client_id] = cc->client_connection_id;

        auto *arg = new connection_cb_arg_t;
        arg->c = this;
        arg->cc = cc;
        cc->timer.init(kClientConnectionTimeout, client_connection_cb, arg);
        cc->timer.start();
      }
    }

    // trigger exists_false_watches if this wasn't a rename
    LOG_SERVER("checking exists false map for %s size is %zu\n",
             znode->my_name.c_str(), exists_false_watches[znode->my_name].size());
    if (op->node.is_rename) {
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeRenamed,
                                   exists_false_watches);
    } else {
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeCreated,
                                   exists_false_watches);
    }
    assert(exists_false_watches[znode->my_name].empty());
  }

  __inline__ void execute_write_node(Operation *op) {
    LOG_SERVER("Thread %zu: executing write_node sn %zu\n", thread_id, op->seqnum);
    std::string name(op->write_node.name);
    ZNode *znode = in_znodes(name);
    // write does not create the node if it doesn't exist
    if (!znode) return;
    if (op->write_node.version == -1 ||
        op->write_node.version == znode->version) {
      memcpy(znode->data, op->write_node.data, MAX_ZNODE_DATA);
      znode->version++;
      LOG_SERVER("Thread %zu: executing write vsn %d new vsn %d\n", thread_id,
             znode->version - 1, znode->version);
      // the node has been updated, check watches and notify
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeDataChanged,
                                   read_node_watches);
      check_watches_map_and_notify(znode->my_name, WatchType::kNodeDataChanged,
                                   exists_true_watches);
      assert(exists_true_watches[znode->my_name].empty());
      assert(read_node_watches[znode->my_name].empty());
    } else {
      LOG_SERVER("Thread %zu: version wrong not writing zvsn %d opvsn %d\n",
             thread_id, znode->version, op->write_node.version);
    }
  }

  // never used.
  __inline__ void execute_create_node_with_children(Operation *op) {
    erpc::rt_assert(false, "Should not be in create with children\n");
    LOG_SERVER("Thread %zu: executing create_node sn %zu\n",
          thread_id, op->seqnum);
    std::string name(op->node.name);
    auto *znode = znode_pool.alloc();
    znode->reset(&name, op->node.data);
    auto *old = in_znodes(name);
    if (old) znode_pool.free(old); // if there was one there update it

    znodes[znode->my_name] = znode;
    char *buf = op->buf;
    size_t nchildren = *(reinterpret_cast<size_t *>(buf));
    buf += sizeof(size_t);
    for (size_t i = 0; i < nchildren; i++) {
      char child[MAX_ZNODE_NAME_SIZE];
      strcpy(child, buf);
      znode->children.insert(child);
      buf += strlen(child);
    }
    delete[] op->buf;
  }

  // returns false if node did not exist
  // Rename is a delete followed by a create and rename child.
  __inline__ void execute_rename_node(Operation *op) {
    LOG_SERVER("Thread %zu: executing rename node from %s to %s\n",
               thread_id, op->rename_node.from, op->rename_node.to);
    if (!in_znodes(op->rename_node.from)) {
      return;
    }
    // for now rename ONLY changes the name, it does not change data
    in_znodes(op->rename_node.from)->my_name = std::string(op->rename_node.to);
  }

  // always succeeds
  // does not trigger any watch
  __inline__ void execute_delete_and_read_node(Operation *op) {
    // delete ignores nullptr, doesn't matter if in map or not
    LOG_SERVER("Thread %zu delete and read node %s\n",
           thread_id, op->delete_node.name);
    auto *znode = in_znodes(op->delete_node.name);
    if (!znode) {
      LOG_SERVER("Thread ID %zu: deleting node not in znode map path %s sn %zu\n",
              thread_id, op->delete_node.name, op->seqnum);
      size_t total_size = MAX_ZNODE_DATA + sizeof(int32_t);
      rpc->resize_msg_buffer(&(op->req_handle->pre_resp_msgbuf), total_size);
      uint8_t *data = op->req_handle->pre_resp_msgbuf.buf;
      *reinterpret_cast<int32_t *>(data) = 0;
      data += sizeof(int32_t);
      memset(data, 0, MAX_ZNODE_DATA);

      rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
      stat_resp_tx_tot++;
      return;
    }
    size_t total_size = MAX_ZNODE_DATA + sizeof(int32_t);

    rpc->resize_msg_buffer(&(op->req_handle->pre_resp_msgbuf), total_size);
    uint8_t *data = op->req_handle->pre_resp_msgbuf.buf;
    *reinterpret_cast<int32_t *>(data) = znode->version;
    data += sizeof(int32_t);

    memcpy(data, znode->data, MAX_ZNODE_DATA);

    rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
    stat_resp_tx_tot++;

    check_watches_map_and_notify(znode->my_name, WatchType::kNodeRenamed,
                                 read_node_watches);
    check_watches_map_and_notify(znode->my_name, WatchType::kNodeRenamed,
                                 exists_true_watches);
    assert(read_node_watches[znode->my_name].empty());
    assert(exists_true_watches[znode->my_name].empty());

    // delete the znode
    znodes.erase(znode->my_name);
    znode_pool.free(znode);
  }

  // always succeeds
  __inline__ void execute_delete_node(Operation *op) {
    // delete ignores nullptr, doesn't matter if in map or not
    LOG_SERVER("Thread %zu: executing delete node %s\n",
               thread_id, op->delete_node.name);
    auto *znode = in_znodes(op->delete_node.name);
    if (!znode) {
      return;
    }
    // only delete if current version matches, unless any version match
    if (op->delete_node.version == znode->version ||
        op->delete_node.version == -1) {
      // delete the znode
      znodes.erase(znode->my_name);
      znode_pool.free(znode);
    }

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

  // always succeeds
  __inline__ void execute_delete_client_connection(Operation *op) {
    delete_client_connection &dcc = op->delete_client_connection;
    // should always have this, will throw if not
    ClientConnection *cc;
    try { // could have been deleted
      cc = client_connections.at(dcc.client_id);
    } catch (std::out_of_range &) {
      dcc.deleted = false;
      zk_payload_t zkp; zkp.delete_client_connection = dcc;
      respond_to_request(this, op->req_handle, &zkp, sizeof(zkp));
      return;
    }

    if (dcc.client_connection_id < cc->client_connection_id) {
      dcc.deleted = false;
      zk_payload_t zkp; zkp.delete_client_connection = dcc;
      respond_to_request(this, op->req_handle, &zkp, sizeof(zkp));
      return;
    }
    fmt_rt_assert(dcc.client_connection_id == cc->client_connection_id,
                  "Delete client connection impossibly ordered"
                  " dcc %zu mine %zu\n",
                  dcc.client_connection_id, cc->client_connection_id);

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
    dcc.deleted = true;
    zk_payload_t zkp; zkp.delete_client_connection = dcc;
    respond_to_request(this, op->req_handle, &zkp, sizeof(zkp));
    delete cc;
  }

  __inline__ void execute_read_node(Operation *op) {
    LOG_SERVER("Thread %zu: executing read of %s\n",
               thread_id, op->read_node.name);
    ZNode *znode = in_znodes(op->read_node.name);
    if (!znode) {
      // todo this is an extra copy...
      char data[MAX_ZNODE_DATA + sizeof(int32_t)];
      memset(data, 0, MAX_ZNODE_DATA + sizeof(int32_t));

      // todo should this create a watch for node creation?

      respond_to_request(this, op->req_handle, data,
                         MAX_ZNODE_DATA + sizeof(int32_t));
      return;
    }
    size_t total_size = MAX_ZNODE_DATA + sizeof(int32_t);

    rpc->resize_msg_buffer(&(op->req_handle->pre_resp_msgbuf), total_size);
    uint8_t *data = op->req_handle->pre_resp_msgbuf.buf;
    *reinterpret_cast<int32_t *>(data) = znode->version;
    data += sizeof(int32_t);

    memcpy(data, znode->data, MAX_ZNODE_DATA);
    rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
    stat_resp_tx_tot++;

    // register a read_node watch
    if (op->read_node.watch) {
      read_node_watches[op->read_node.name].push_back(op->read_node.client_id);
    }
  }

  __inline__ void execute_get_children(Operation *op) {
    ZNode *znode = in_znodes(op->get_children.name);
    if (!znode) {
      rpc->resize_msg_buffer(&op->req_handle->pre_resp_msgbuf,
                             sizeof(get_children_t));
      auto *gc = reinterpret_cast<get_children_t *>(
          op->req_handle->pre_resp_msgbuf.buf);
      gc->nchildren = 0;
      strcpy(gc->name, op->get_children.name);
      gc->version = -1;
      rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
      stat_resp_tx_tot++;

      LOG_SERVER("execute get children responding no node\n");
      return;
    }
    // address of reference is the address of original object
    auto msg_size = sizeof_get_children(znode->children.size());
    fmt_rt_assert(msg_size < kMaxGetChildrenSize,
      "Thread %zu: Size of get children response too large. "
      "%zu for %zu children. Max eRPC msg size is %zu.\n",
      thread_id, msg_size, znode->children.size(), kMaxMsgSize);

    erpc::MsgBuffer &resp_msgbuf = op->req_handle->dyn_resp_msgbuf;
    resp_msgbuf = rpc->alloc_msg_buffer_or_die(msg_size);
    rpc->resize_msg_buffer(&resp_msgbuf, msg_size);

    auto *gc = reinterpret_cast<get_children_t *>(resp_msgbuf.buf);
    gc->version = znode->version;
    strcpy(gc->name, znode->my_name.c_str());
    gc->seqnum = op->seqnum;
    gc->nchildren = znode->children.size();
    char *gc_child_ptr = gc->children;
    for (const std::string &child : znode->children) {
      strcpy(gc_child_ptr, child.c_str());
      gc_child_ptr += MAX_ZNODE_NAME_SIZE;
    }

    LOG_SERVER("Returning %u children\n", gc->nchildren);
    rpc->enqueue_response(op->req_handle, &resp_msgbuf);
    stat_resp_tx_tot++;

    // register watch
    if (op->get_children.watch) {
      get_children_watches[op->get_children.name].push_back(
          op->get_children.client_id);
    }
  }

  __inline__ void execute_add_child(Operation *op) {
    LOG_SERVER("Thread %zu: executing add child sn %zu\n",
           thread_id, op->seqnum);
    auto *znode = in_znodes(op->add_child.name);
    if (!znode) {
      std::string name(op->add_child.name);
      znode = znode_pool.alloc();
      znode->reset(&name, nullptr);
      znodes[znode->my_name] = znode;
    }
    znode->add_child(op->add_child.child);

    check_watches_map_and_notify(znode->my_name,
                                 WatchType::kNodeChildrenChanged,
                                 get_children_watches);
    assert(get_children_watches[znode->my_name].empty());
  }

  // responds false if no znode of name
  __inline__ void execute_remove_child(Operation *op) {
    LOG_SERVER("Thread %zu: executing remove child sn %zu\n",
           thread_id, op->seqnum);
    auto *znode = in_znodes(op->remove_child.name);
    if (!znode) {
      return;
    }
    znode->remove_child(op->remove_child.child);

    check_watches_map_and_notify(znode->my_name,
                                 WatchType::kNodeChildrenChanged,
                                 get_children_watches);
    assert(get_children_watches[znode->my_name].empty());
  }

  // returns false if node or child did not exist
  __inline__ void execute_rename_child(Operation *op) {
    LOG_SERVER("Thread %zu: executing rename child sn %zu name %s\n",
           thread_id, op->seqnum, op->rename_child.from);
    auto *znode = in_znodes(get_parent(op->rename_child.from));
    if (!znode) {
      std::string name(op->rename_child.from);
      znode = znode_pool.alloc();
      znode->reset(&name, nullptr);
      znodes[znode->my_name] = znode;
    }
    znode->rename_child(op->rename_child.from, op->rename_child.to);

    check_watches_map_and_notify(znode->my_name,
                                 WatchType::kNodeChildrenChanged,
                                 get_children_watches);
    assert(get_children_watches[znode->my_name].empty());
  }

  __inline__ void execute_exists(Operation *op) {
    LOG_SERVER("Thread %zu: executing exists %s\n", thread_id, op->exists.name);
    rpc->resize_msg_buffer(
        &(op->req_handle->pre_resp_msgbuf), sizeof(exists_t));
    auto *exists =
        reinterpret_cast<exists_t *>(op->req_handle->pre_resp_msgbuf.buf);
    exists->seqnum = op->seqnum;
    strcpy(exists->name, op->exists.name);
    exists->exists = in_znodes(op->exists.name);

    // register watches
    if (op->exists.watch) {
      if (exists->exists) {
        exists_true_watches[op->exists.name].push_back(op->exists.client_id);
      } else {
        LOG_SERVER("exists returning false %s\n", op->exists.name);
        exists_false_watches[op->exists.name].push_back(op->exists.client_id);
      }
    }

    rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
    stat_resp_tx_tot++;
  }

  __inline__ void execute_and_delete_operation(Operation *op) {
    switch (op->op_type) {
      case OpType::kCreateNode: { execute_create_node(op); break; }
      case OpType::kCreateNodeWithChildren: {
        execute_create_node_with_children(op); break; }
      case OpType::kWriteNode:              { execute_write_node(op); break; }
      case OpType::kRenameNode:             { execute_rename_node(op); break; }
      case OpType::kDeleteAndReadNode:      {
        execute_delete_and_read_node(op); break; }
      case OpType::kDeleteNode:             { execute_delete_node(op); break; }
      case OpType::kReadNode:               { execute_read_node(op); break; }
      case OpType::kAddChild:               { execute_add_child(op); break; }
      case OpType::kRemoveChild:            { execute_remove_child(op); break; }
      case OpType::kRenameChild:            { execute_rename_child(op); break; }
      case OpType::kExists:                 { execute_exists(op); break; }
      case OpType::kGetChildren:            { execute_get_children(op); break; }
      case OpType::kDeleteClientConnection: {
        execute_delete_client_connection(op); break; }
      default: { // for compiler
        break;
      }
    }
    op_pool.free(op);
  }

  // function to continuously pull from queue and execute until we can't
  // execute the next op
  __inline__ void pull_and_execute() {
    while (!op_queue.empty() && op_queue.top()->seqnum == cur_seqnum) {
      auto *op = op_queue.top();
      op_queue.pop();
      cur_seqnum++;
      execute_and_delete_operation(op);
    }
  }

  // function to add to pri queue
  __inline__ void push_and_pull_from_queue(Operation *op) {
    if (unlikely(op->seqnum < cur_seqnum)) {
      if (op->op_type == OpType::kDeleteAndReadNode) {
        fmt_rt_assert(false,
                      "Thread %zu: got old delete request for an old sequence"
                      " number already processed %zu\n", thread_id, op->seqnum);
      }
      return;
    }
    op_queue.push(op);
    pull_and_execute();
  }

  void print_stats();
};

void delete_eph_nodes_cont_func(void *_c, void *_t) {
  (void) _c; (void) _t;
  delete reinterpret_cast<basic_tag_t *>(_t);
  return;
}

// submit the explicit delete to a proxy
void client_connection_cb(void *_arg) {
  ZKContext *c = reinterpret_cast<connection_cb_arg_t *>(_arg)->c;
  ClientConnection *cc = reinterpret_cast<connection_cb_arg_t *>(_arg)->cc;
  // this node could have many ephemeral nodes, but we only need to send the
  // id because each server will have the same state
  LOG_INFO("IN CLIENT CONNECTION CB for cid %d addr %p cxn num %zu\n",
           cc->client_id, reinterpret_cast<void *>(cc),
           cc->client_connection_id);

  // need to submit a multi-op delete for all the nodes in this connection
  if (cc->znodes.empty()) {
    LOG_INFO("Client connection for %d had no znodes\n", cc->client_id);
    erpc::rt_assert(false, "no znodes???\n");
    return;
  }
  LOG_INFO("There are %zu eph nodes\n", cc->znodes.size());
  for (auto znode : cc->znodes) {
    LOG_INFO("\t%s\n", znode->my_name.c_str());
  }
  auto *bt = new basic_tag_t;
  bt->req_msgbuf = c->rpc->alloc_msg_buffer_or_die(sizeof(client_payload_t));
  bt->resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(sizeof(client_payload_t));
  auto *payload = reinterpret_cast<client_payload_t *>(bt->req_msgbuf.buf);

  // client metadata
  payload->client_id = c->my_client_id;
  payload->client_reqid = c->cur_req_id++;
  payload->proxy_id = FLAGS_proxy_id;
  payload->highest_recvd_reqid = c->highest_cons_reqid;

  // setup operation
  payload->reqtype = ReqType::kDeleteClientConnection;
  payload->zk_payload.delete_client_connection.client_connection_id = cc->client_connection_id;
  payload->zk_payload.delete_client_connection.client_id = cc->client_id;
  payload->zk_payload.delete_client_connection.nshards = cc->shards.size();
  set_shards(cc->shards, payload->zk_payload.delete_client_connection.shards);
  c->rpc->enqueue_request(c->proxy_session_num,
                          static_cast<uint8_t>(payload->reqtype),
                          &bt->req_msgbuf, &bt->resp_msgbuf,
                          delete_eph_nodes_cont_func,
                          reinterpret_cast<void *>(bt));
  LOG_INFO("1\n");
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
__inline__ void notify_cont_func(void *_c, void *_tag) {
  LOG_SERVER("got notification ack\n");
  auto *c = reinterpret_cast<ZKContext *>(_c);
  auto *tag = reinterpret_cast<WatchTag *>(_tag);
  c->watch_tag_pool.free(tag);
}

inline void respond_to_request(ZKContext *c,
                               erpc::ReqHandle *req_handle,
                               void *buf,
                               size_t size) {
  if (size != 0) {
    c->rpc->resize_msg_buffer(&(req_handle->pre_resp_msgbuf), size);
  } else {
    c->rpc->resize_msg_buffer(&(req_handle->pre_resp_msgbuf), 1);
  }
  if (unlikely(size != 0)) {
    memcpy(req_handle->pre_resp_msgbuf.buf, buf, size);
  }
  c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
  c->stat_resp_tx_tot++;
}

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *);
void corfu_append_req_handler(erpc::ReqHandle *, void *);
void corfu_read_req_handler(erpc::ReqHandle *, void *);
void launch_threads(int, erpc::Nexus *);

#endif //ZK_SERVER_H
