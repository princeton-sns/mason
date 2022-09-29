#ifndef ZK_SERVER_H
#define ZK_SERVER_H

#define IDENT(x) x
#define XSTR(x) #x
#define STR(x) XSTR(x)
#define PATH(x, y) STR(IDENT(x)IDENT(y))

#define Fname /common.h

#include PATH(COMMON_DIR, Fname)

#define DEBUG 0

#include "common.h"
extern size_t numa_node;
extern volatile bool force_quit;

DEFINE_string(my_ip, "",
              "This Corfu Server's IP address for eRPC setup");

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
                        char *buf,
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
  };

  // required for AppMemPool
  Operation() {};

  // could be simplified like above
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
};

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
    if (!children.erase(from)) return false;
    add_child(to);
    return true;
  }

};

struct CmpOperation {
  bool operator()(const Operation *a, const Operation *b) const {
    return a->seqnum > b->seqnum;
  }
};

class ZKContext : public ThreadContext {
 public:
  struct timespec tput_t0;
  app_stats_t *app_stats;
  uint64_t stat_resp_tx_tot = 0;

  AppMemPool<Operation> op_pool;
  AppMemPool<ZNode> znode_pool;

  // the main struct of filenames to data and its children
  // should be pointer to ZNode?
  std::unordered_map<std::string, ZNode *> znodes;

  // the queue of operations ordered by seqnum
  uint64_t cur_seqnum = 0;
  std::priority_queue<Operation *, std::vector<Operation *>, CmpOperation>
      op_queue;

  ZNode *in_znodes(std::string name) {
    return znodes.find(name) != znodes.end() ? znodes[name] : nullptr;
  }

  __inline__ void execute_create_node(Operation *op) {
    std::string name(op->node.name);
    auto *znode = znode_pool.alloc();
    znode->reset(&name, op->node.data);
    auto *old = in_znodes(name);
    if (old) znode_pool.free(old); // if there was one there update it

    // this allows it to be used for renames, proxy sends previous version
    znode->version = op->node.version;
    znodes[znode->my_name] = znode;
  }

  __inline__ void execute_write_node(Operation *op) {
    std::string name(op->write_node.name);
    ZNode *znode = in_znodes(name);
    // write does not create the node if it doesn't exist
    if (!znode) return;
    if (op->write_node.version == -1 ||
        op->write_node.version == znode->version) {
      memcpy(znode->data, op->write_node.data, MAX_ZNODE_DATA);
      znode->version++;
    } else {
//      printf("version wrong not writing zvsn %d opvsn %d\n",
//             znode->version, op->write_node.version);
    }
  }

  // never used.
  __inline__ void execute_create_node_with_children(Operation *op) {
    erpc::rt_assert(false, "Should not be in create with children\n");
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
  // I don't actually want this op.
  // Rename is a delete followed by a create and rename child.
  __inline__ void execute_rename_node(Operation *op) {
    if (!in_znodes(op->rename_node.from)) {
      return;
    }
    // for now rename ONLY changes the name, it does not change data
    in_znodes(op->rename_node.from)->my_name = std::string(op->rename_node.to);

  }

  // always succeeds
  __inline__ void execute_delete_and_read_node(Operation *op) {
    auto *znode = in_znodes(op->delete_node.name);
    if (!znode) {
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

    // delete the znode
    znodes.erase(znode->my_name);
    znode_pool.free(znode);
  }

  // always succeeds
  __inline__ void execute_delete_node(Operation *op) {
    // delete ignores nullptr, doesn't matter if in map or not
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
  }

  __inline__ void execute_read_node(Operation *op) {
    // delete ignores nullptr, doesn't matter if in map or not
    ZNode *znode = in_znodes(op->read_node.name);
    if (!znode) {
      char data[MAX_ZNODE_DATA + sizeof(int32_t)];
      memset(data, 0, MAX_ZNODE_DATA + sizeof(int32_t));

      respond_to_request(this, op->req_handle, data,
                         MAX_ZNODE_DATA + sizeof(int32_t));
      return;
    }
    // reply with the data that was deleted. Necessary for rename.
    // atomicity is given to us by multi-sequencing
    size_t total_size = MAX_ZNODE_DATA + sizeof(int32_t);

    rpc->resize_msg_buffer(&(op->req_handle->pre_resp_msgbuf), total_size);
    uint8_t *data = op->req_handle->pre_resp_msgbuf.buf;
    *reinterpret_cast<int32_t *>(data) = znode->version;
    data += sizeof(int32_t);

    memcpy(data, znode->data, MAX_ZNODE_DATA);
    rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
    stat_resp_tx_tot++;
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
      gc->version = -1; // used to return null node
      rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
      stat_resp_tx_tot++;
      return;
    }
    auto msg_size = sizeof_get_children(znode->children.size());
    fmt_rt_assert(msg_size < kMaxMsgSize,
                  "Size of get children response too large. %zu for %zu children. "
                  "Max eRPC msg size is %zu.\n", msg_size, znode->children.size(),
                  kMaxMsgSize);

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

    rpc->enqueue_response(op->req_handle, &resp_msgbuf);
    stat_resp_tx_tot++;
  }

  __inline__ void execute_add_child(Operation *op) {
    auto *znode = in_znodes(op->add_child.name);
    if (!znode) {
      // creates the parent if the child doesn't exist?
      // perhaps should just fail? this should never happen...
      std::string name(op->add_child.name);
      znode = znode_pool.alloc();
      znode->reset(&name, nullptr);
      znodes[znode->my_name] = znode;
    }
    znode->add_child(op->add_child.child);
  }

  // responds false if no znode of name
  __inline__ void execute_remove_child(Operation *op) {
    // actually just needs child name, it includes full path.
    auto *znode = in_znodes(op->remove_child.name);
    if (!znode) {
      return;
    }
    znode->remove_child(op->remove_child.child);
  }

  // returns false if node or child did not exist
  __inline__ void execute_rename_child(Operation *op) {
    // if parent doesn't exist create it with no data
    auto *znode = in_znodes(get_parent(op->rename_child.from));
    if (!znode) {
      std::string name(op->rename_child.from);
      znode = znode_pool.alloc();
      znode->reset(&name, nullptr);
      znodes[znode->my_name] = znode;
    }
    znode->rename_child(op->rename_child.from, op->rename_child.to);
  }

  __inline__ void execute_exists(Operation *op) {
    rpc->resize_msg_buffer(
        &(op->req_handle->pre_resp_msgbuf), sizeof(exists_t));
    auto *exists =
        reinterpret_cast<exists_t *>(op->req_handle->pre_resp_msgbuf.buf);
    exists->seqnum = op->seqnum;
    strcpy(exists->name, op->exists.name);
    exists->exists = in_znodes(op->exists.name);
    rpc->enqueue_response(op->req_handle, &op->req_handle->pre_resp_msgbuf);
    stat_resp_tx_tot++;
  }

  __inline__ void execute_and_delete_operation(Operation *op) {
    switch (op->op_type) {
      case OpType::kCreateNode: {
        execute_create_node(op);
        break;
      }
      case OpType::kCreateNodeWithChildren: {
        execute_create_node_with_children(op);
        break;
      }
      case OpType::kWriteNode: {
        execute_write_node(op);
        break;
      }
      case OpType::kRenameNode: {
        execute_rename_node(op);
        break;
      }
      case OpType::kDeleteAndReadNode: {
        execute_delete_and_read_node(op);
        break;
      }
      case OpType::kDeleteNode: {
        execute_delete_node(op);
        break;
      }
      case OpType::kReadNode: {
        execute_read_node(op);
        break;
      }
      case OpType::kAddChild: {
        execute_add_child(op);
        break;
      }
      case OpType::kRemoveChild: {
        execute_remove_child(op);
        break;
      }
      case OpType::kRenameChild: {
        execute_rename_child(op);
        break;
      }
      case OpType::kExists: {
        execute_exists(op);
        break;
      }
      case OpType::kGetChildren: {
        execute_get_children(op);
        break;
      }
      default: { // for compiler
        break; // will be stuck here
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
      // the only operation that returns something
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

inline void respond_to_request(ZKContext *c,
                               erpc::ReqHandle *req_handle,
                               char *buf,
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