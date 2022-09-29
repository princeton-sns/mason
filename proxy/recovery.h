#ifndef PROXY_RECOVERY_H
#define PROXY_RECOVERY_H

#include <cstdint>
#include <stdint-gcc.h>
#include "proxy.h"
#include "gc.h"

int connect_and_store_session(WorkerContext *, std::string, int, MachineIdx);
int connect_and_store_session(WorkerContext *,
                              std::string,
                              int,
                              MachineIdx,
                              Proxy *);
void process_received_seqnum(Batch *, uint64_t);
void process_received_seqnums(Batch *, seq_req_t[]);

// Sever connection to original sequencer and connect to the backup
void
replace_seq_connection(WorkerContext *c, int remote_tid) {
  debug_print(DEBUG_RECOVERY, "[%zu] Replacing seq connection\n", c->thread_id);
  c->stop_heartbeat_timer();
  connect_and_store_session(c, backupseq_ip, remote_tid, MachineIdx::SEQ);
}

// Destroy the connection to the backup sequencer; we will be receiving
// requests from it until we are done with recovery, and then we can initiate
// a new connection on the appropriate thread in the SEQ position of the
// session_num_vec
void
initiate_recovery_cont_func(void *_c, void *_tag) {
  debug_print(DEBUG_RECOVERY, "heard back from backup!\n");
  auto *c = reinterpret_cast<WorkerContext *>(_c);

  Tag *tag = reinterpret_cast<Tag *>(_tag);
  Batch *b = tag->batch;

  b->proxy->tag_pool.free(tag);
  b->proxy->batch_pool.free(b);

  // Disconnect this session. Will reconnect later with appropriate thread.
  int ret = c->rpc->destroy_session(
      c->session_num_vec[static_cast<uint8_t>(MachineIdx::BACKUP_SEQ)]);
  erpc::rt_assert(ret >= 0, "Couldn't disconnect session to backup sequencer!");
  c->session_num_vec[static_cast<uint8_t>(MachineIdx::BACKUP_SEQ)] = -1;
}

// The sequencer has missed enough heartbeats.
// Connect to the backup sequencer and tell it that the original
// sequencer has died
void
initiate_recovery(void *_c) {
  WorkerContext *c = reinterpret_cast<WorkerContext *>(_c);

  debug_print(DEBUG_RECOVERY, "[%zu] Initiating recovery!\n",
              c->thread_id);

  for (auto context : *context_vector) {
    context->in_recovery = true;
    context->stop_heartbeat_timer();
  }

  // Reset and destroy session to sequencer (primary)
  debug_print(DEBUG_RECOVERY, "[%zu] Resetting session!\n",
              c->thread_id);
  debug_print(DEBUG_RECOVERY, "snv %d\n",
              c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)]);

  bool ret = c->rpc->reset_session(
      c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)]);
  erpc::rt_assert(ret, "Session reset failed!");

  c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)] = -1;

  Proxy *proxy = c->proxies[FLAGS_proxy_id_start + c->thread_id];
  Batch *batch = proxy->batch_pool.alloc();
  batch->reset(c, proxy, 0, 0);

  debug_print(DEBUG_RECOVERY, "[%zu] Connecting to backup!\n",
              c->thread_id);
  connect_and_store_session(c, backupseq_ip, RECOVERY_RPCID,
                            MachineIdx::BACKUP_SEQ, proxy);
  debug_print(DEBUG_RECOVERY, "[%zu] ... connected\n",
              c->thread_id);

  Tag *tag = proxy->tag_pool.alloc();

  tag->alloc_msgbufs(c, batch, false);
  c->rpc->resize_msg_buffer(&tag->req_msgbuf, 1);

  debug_print(DEBUG_RECOVERY, "Getting ready to enqueue recovery request\n");
  uint8_t session_num = c->session_num_vec[static_cast<uint8_t>(
      MachineIdx::BACKUP_SEQ)];

  c->rpc->enqueue_request(session_num,
                          static_cast<uint8_t>(ReqType::kInitiateRecovery),
                          &tag->req_msgbuf, &tag->resp_msgbuf,
                          initiate_recovery_cont_func,
                          reinterpret_cast<void *>(tag));

  while (c->session_num_vec[static_cast<uint8_t>(MachineIdx::BACKUP_SEQ)] > 0) {
    debug_print(DEBUG_RECOVERY,
                "Waiting in event loop until session is dead\n");
    c->rpc->run_event_loop_once();
    proxy->call_raft_periodic(1);
  }
}

// Collect bitmaps from other threads
void
collect_bitmaps(WorkerContext *c, uint64_t sequence_space) {
  c->recovery_context->agg = new Bitmap;
  Bitmap *agg = c->recovery_context->agg;
  int first = 1;

  for (auto &context : *context_vector) {
    LOG_RECOVERY("On context %zu sequence space %zu\n",
                 context->thread_id,
                 sequence_space);
    Bitmap *bitmap = context->received_ms_seqnums[sequence_space];

    if (first) {
      LOG_RECOVERY("Starting to collect the bitmaps. Creating agg bitmap...\n");
      // Starting bitmap
      delete[] agg->bitmap;

      agg->base_seqnum = bitmap->base_seqnum;
      agg->nblocks = bitmap->nblocks;
      agg->head_block = bitmap->head_block;
      agg->tail_block = bitmap->tail_block;
      agg->bitmap = new uint8_t[agg->nblocks * BYTES_PER_BLOCK]();
      memcpy(agg->bitmap, bitmap->bitmap,
             agg->nblocks * BYTES_PER_BLOCK);

      first = 0;
      LOG_RECOVERY("... created agg bitmap\n");
    } else {
      // Next bitmap
      auto *next = new Bitmap;
      delete[] next->bitmap;

      next->base_seqnum = bitmap->base_seqnum;
      next->nblocks = bitmap->nblocks;
      next->head_block = bitmap->head_block;
      next->tail_block = bitmap->tail_block;
      next->bitmap = new uint8_t[next->nblocks * BYTES_PER_BLOCK]();

      memcpy(next->bitmap, bitmap->bitmap,
             next->nblocks * BYTES_PER_BLOCK);

      // Truncate original bitmap
      while (agg->base_seqnum < next->base_seqnum) {
        agg->truncate();
      }

      // Truncate new bitmap
      while (agg->base_seqnum > next->base_seqnum) {
        next->truncate();
      }

      // Keep the larger bitmap
      if (agg->nblocks >= next->nblocks) {
        for (size_t i = 0; i < next->nblocks * BYTES_PER_BLOCK; i++) {
          agg->bitmap[(
              agg->head_block * BYTES_PER_BLOCK + i) % (
              agg->nblocks * BYTES_PER_BLOCK)] |=
              next->bitmap[(
                  next->head_block * BYTES_PER_BLOCK + i) % (
                  next->nblocks * BYTES_PER_BLOCK)];
        }
        delete next;

      } else {
        for (size_t i = 0; i < agg->nblocks * BYTES_PER_BLOCK; i++) {
          next->bitmap[(
              next->head_block * BYTES_PER_BLOCK + i) % (
              next->nblocks * BYTES_PER_BLOCK)] |=
              agg->bitmap[(
                  agg->head_block * BYTES_PER_BLOCK + i) % (
                  agg->nblocks * BYTES_PER_BLOCK)];
        }

        delete agg;
        agg = next;
      }
    }
  }
}

void send_bitmap(erpc::ReqHandle *req_handle, WorkerContext *c,
                 uint64_t offset) {
  LOG_RECOVERY("in send_bitmap\n");
  RecoveryContext *rc = c->recovery_context;
  Bitmap *agg = rc->agg;

  size_t block_offset = offset / BYTES_PER_BLOCK;
  size_t nblocks_this_resp;

  // How big should the msgbuffer be?
  if (agg->nblocks - block_offset < MAX_BLOCKS_PER_MSG) {
    nblocks_this_resp = agg->nblocks - block_offset;
  } else {
    nblocks_this_resp = MAX_BLOCKS_PER_MSG;
  }

  erpc::MsgBuffer &resp_msgbuf = req_handle->dyn_resp_msgbuf;
  resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(
      sizeof(recovery_payload_t) - sizeof(uint8_t) +
          nblocks_this_resp * BYTES_PER_BLOCK);

  // Fill in the buffer
  auto *payload = reinterpret_cast<recovery_payload_t *>(
      resp_msgbuf.buf);
  payload->nblocks_this_resp = nblocks_this_resp;
  payload->base_seqnum = agg->base_seqnum;
  payload->nblocks_total = agg->nblocks;
  payload->head_block = agg->head_block;
  payload->tail_block = agg->tail_block;
  payload->nrequests = rc->dependencies.size();

  memcpy(payload->received_seqnums,
         agg->bitmap + offset,
         nblocks_this_resp * BYTES_PER_BLOCK);

  c->rpc->enqueue_response(req_handle, &resp_msgbuf);
  LOG_RECOVERY("leaving send_bitmap\n");
}

void
collect_dependencies(WorkerContext *c, uint64_t sequence_space) {
  RecoveryContext *rc = c->recovery_context;

  for (auto &context : *context_vector) {
    for (auto &el : context->proxies) {
      Proxy *proxy = el.second;
      if (!raft_is_leader(proxy->raft)) {
        continue;
      }

      // on followers:
      //    these are batches that have been log_offered, but not applied
      // on leader:
      //    these are batches that have been requested from the sequencer
      //    and not received a response. Or have received the response
      //    but not yet applied. Sealing the old sequencer and apply the
      //    recovery message should prevent the second case.
      for (auto const &x : proxy->appended_batch_map) {
        Batch *batch = x.second;

        dependency_t dep;
        dep.proxy_id = batch->proxy_id;
        dep.thread_id = context->thread_id;
        dep.batch_size = batch->seq_reqs[sequence_space].batch_size;

        rc->dependencies.push_back(dep);
      }
    }
  }

  debug_print(DEBUG_RECOVERY, "Found %zu batches missing responses\n",
              rc->dependencies.size());
  if (LOG_DEBUG_RECOVERY)
    for (auto dep : rc->dependencies) {
      (void) dep;
      LOG_RECOVERY("dep batch_size: %zu\n", dep.batch_size);
    }
}

// Tell the backup how many leader responses we got
// This is handled by thread0
void
WorkerContext::respond_backup_ready() {
  // Some sanity checks
  debug_print(DEBUG_RECOVERY, "Responding to backup_ready\n");
  erpc::rt_assert(thread_id == 0,
                  "Wrong thread trying to respond to sequencer!\n");
  erpc::rt_assert(backup_ready_handle != nullptr,
                  "Trying to respond to backup again!");

  erpc::MsgBuffer *msgbuf = &backup_ready_handle->pre_resp_msgbuf;
  rpc->resize_msg_buffer(&backup_ready_handle->pre_resp_msgbuf, 1);
  uint8_t *leader_count = reinterpret_cast<uint8_t *>(msgbuf->buf);
  *leader_count = total_leaders;
  rpc->enqueue_response(backup_ready_handle, msgbuf);
  backup_ready_handle = nullptr;
}

// All leaders on a thread will confirm recovery
// recovery_confirmations should get incremented on each confirmation.
// When it equals the number of leaders for this workercontext,
// send a response to the backup.
void
WorkerContext::confirm_recovery_replication(bool incr_confirmations) {
  WorkerContext *c0 = context_vector->at(0);
  if (incr_confirmations) {
    c0->recovery_confirmations++;
  }

  // if I am the designated recovery thread, trap here until all leaders have
  // replicated their recovery entry
  if (thread_id == 0) {
    while (recovery_confirmations != total_leaders) {
      debug_print(LOG_DEBUG_RECOVERY, "[%zu] Still waiting for recovery "
                                      "confirmations have %d need %d...\n",
                  thread_id, static_cast<uint8_t>(recovery_confirmations),
                  static_cast<uint8_t>(total_leaders));
      for (auto &el : proxies) {
        Proxy *proxy = el.second;
        proxy->call_raft_periodic(0);
        raft_apply_all(proxy->raft);
      }
      rpc->run_event_loop(50);
    }
    respond_backup_ready();
  }
}

// Find all proxy groups and replicate recovery if this is a leader.
// Track the number of leaders; proceed with recovery after all leaders have committed
// move to the backup seq.
static void
replicate_recovery() {
  WorkerContext *c0 = context_vector->at(0);
  for (auto c : *context_vector) {
    c->in_recovery = true;
    for (auto &el : c->proxies) {
      Proxy *proxy = el.second;
      if (!raft_is_leader(proxy->raft)) {
        continue;
      }

      c->leaders_this_thread++;
      c0->total_leaders++;

      if (c->thread_id != 0)
        continue;

      c->stop_heartbeat_timer();
      LOG_RECOVERY("[%zu] Calling replicate recovery\n", c->thread_id);
      proxy->replicate_recovery();
    }
    debug_print(DEBUG_RECOVERY, "[%zu] %zu leaders this thread\n",
                c->thread_id, c->leaders_this_thread);
  }

  // If there are no leaders on c0, we will never go into
  // confirm_recovery_replication on this thread. But c0 is the only thread that
  // can respond to the sequencer.
  if (c0->leaders_this_thread == 0) {
    c0->confirm_recovery_replication(false);
  }
}

// The backup has heard from someone that it is time to initiate recovery.
// We need to replicate this after we kill the connection to the 1* sequencer.
// Only thread 0 should get this.
static void
backup_ready_handler(erpc::ReqHandle *req_handle, void *_c) {
  WorkerContext *c = reinterpret_cast<WorkerContext *>(_c);
  debug_print(DEBUG_RECOVERY, "[%zu] In backup_ready_handler\n",
              c->thread_id);
  c->backup_ready_handle = req_handle;

  // Each proxy group records that it needs to switch over to the backup
  // sequencer. Respond to sequencer when the raft callback triggers.
  debug_print(DEBUG_RECOVERY, "Preparing to replicate recovery\n");
  replicate_recovery();
  debug_print(DEBUG_RECOVERY, "Leaving backup_ready_handler\n");
}

// Respond to request for bitmap. This will go to the 0th thread only;
// it will be responsible for compiling the bitmaps from every other
// thread, truncating, and sending to the sequencer
static void
request_bitmap_handler(erpc::ReqHandle *req_handle, void *_c) {
  auto *c = reinterpret_cast<WorkerContext *>(_c);

  debug_print(1, "in request_bitmap_handler\n");

  Proxy *proxy = nullptr;
  WorkerContext *context = nullptr;

  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() > 0, "bitmap msgbuf too small!");
  uint64_t offset = reinterpret_cast<offset_payload_t *>
  (req_msgbuf->buf)->offset;
  uint64_t sequence_space = reinterpret_cast<offset_payload_t *>
  (req_msgbuf->buf)->sequence_space;

  debug_print(DEBUG_RECOVERY,
              "Working on offset %lu sequence_space %zu\n",
              offset,
              sequence_space);

  static std::vector<Batch *> *recovery_batches = new std::vector<Batch *>();
  recovery_batches->resize(nsequence_spaces);

  if (!sequence_space) {
    printf("starting to receive requests for bitmaps\n");
  }

  // The first message
  if (offset == 0) {
    debug_print(DEBUG_RECOVERY,
                "Received first message for sequence_space %zu\n",
                sequence_space);
    c->recovery_context = new RecoveryContext(c);
    c->recovery_context->recovery_batches = recovery_batches;

    for (auto ctx : *context_vector) {
      for (auto &el : ctx->proxies) {
        Proxy *p = el.second;
        if (raft_is_leader(p->raft)) {
          proxy = p;
          context = ctx;
          break;
        }
      }
    }
    erpc::rt_assert(proxy != nullptr, "Couldn't find a recovery leader!\n");

    LOG_RECOVERY(
        "Allocating another batch for recovery recovery batches size %zu\n",
        (*(c->recovery_context->recovery_batches)).size());

    Batch *b = proxy->batch_pool.alloc();
    (*(c->recovery_context->recovery_batches))[sequence_space] =
        b;
    (*(c->recovery_context->recovery_batches))[sequence_space]->reset(
        context, proxy, proxy->proxy_id, proxy->batch_counter++);

    LOG_RECOVERY(
        "Allocated the batch for sequence space %zu, batch addr %p batch_id %zu\n",
        sequence_space,
        reinterpret_cast<void *>(b),
        b->batch_id);

    debug_print(1,
                "Recovery batchid: %zu\n",
                (*(c->recovery_context->recovery_batches))[sequence_space]->batch_id);

    size_t now = erpc::rdtsc(); (void) now;
    // Collect everyone else's bitmaps, dependencies
    collect_bitmaps(c, sequence_space);
    LOG_RECOVERY("Collecting bitmaps took %Lf us\n",
                 cycles_to_usec(erpc::rdtsc() - now));

    LOG_RECOVERY("Collected bitmaps\n");

    if (DEBUG_BITMAPS) {
      FILE *f;
      char fname[100];
      sprintf(fname, "temp-%s", FLAGS_my_ip.c_str());
      f = fopen(fname, "w");
      Bitmap *p_seqnums = c->recovery_context->agg;
      fprintf(f, "HEAD %zu, TAIL %zu, BASE %zu, NBLOCKS %zu\n",
              p_seqnums->head_block, p_seqnums->tail_block,
              p_seqnums->base_seqnum, p_seqnums->nblocks);
      for (size_t i = 0; i < p_seqnums->nblocks * BYTES_PER_BLOCK; i++) {
        fprintf(f, "%"
        PRIu8
        "\n", p_seqnums->bitmap[i]);
      }
      fprintf(f, "\n");
      fclose(f);
    }

    now = erpc::rdtsc();
    collect_dependencies(c, sequence_space);
    LOG_RECOVERY("Collecting dependencies took %Lf us\n",
                 cycles_to_usec(erpc::rdtsc() - now));
    LOG_RECOVERY("Collected dependencies\n");
  }
  erpc::rt_assert(offset / BYTES_PER_BLOCK < c->recovery_context->agg->nblocks,
                  "The offset exceeds the bitmap size!\n");

  LOG_RECOVERY("About to send bitmap\n");
  // Send to the recovering sequencer
  send_bitmap(req_handle, c, offset);
  LOG_RECOVERY("no longer in send_bitmap\n");
  debug_print(DEBUG_RECOVERY, "Leaving request bitmap handler\n");
  LOG_RECOVERY("after debug_print \"Leaving\"\n");

}

static void
request_dependencies_handler(erpc::ReqHandle *req_handle, void *_c) {
  debug_print(DEBUG_RECOVERY, "In request_dependencies handler\n");
  auto *c = reinterpret_cast<WorkerContext *>(_c);

  erpc::MsgBuffer &resp_msgbuf = req_handle->dyn_resp_msgbuf;
  resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(
      sizeof(dependency_t) * c->recovery_context->dependencies.size());

  // Fill in the buffer
  auto *payload = reinterpret_cast<dependency_t *>(
      resp_msgbuf.buf);
  memcpy(payload, c->recovery_context->dependencies.data(),
         sizeof(dependency_t) * c->recovery_context->dependencies.size());
  c->rpc->enqueue_response(req_handle, &resp_msgbuf);
  debug_print(DEBUG_RECOVERY, "Leaving request_dependencies handler\n");
}

static void
fill_hole_handler(erpc::ReqHandle *req_handle, void *_c) {
  debug_print(DEBUG_RECOVERY, "In fill hole handler\n");
  auto *c0 = reinterpret_cast<WorkerContext *>(_c);

  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  erpc::rt_assert(req_msgbuf->get_data_size() > 0,
                  "Message error!");

  auto *dep = reinterpret_cast<dependency_t *>(req_msgbuf->buf);
  Batch
      *batch = (*(c0->recovery_context->recovery_batches))[dep->sequence_space];
  Proxy *proxy = batch->proxy;
//    dep->

  LOG_RECOVERY("Recv'd dep tid %u pid %u midx %u ss %zu sn %zu bs %zu\n",
               dep->thread_id,
               dep->proxy_id,
               dep->machine_idx,
               dep->sequence_space,
               dep->seqnum,
               dep->batch_size);

  ClientOp *op = proxy->client_op_pool.alloc();
  op->populate(ReqType::kNoop, UINT16_MAX, INT64_MAX,
               proxy->proxy_id, nullptr, proxy->op_counter++, proxy);

  for (size_t i = 0; i < nsequence_spaces; i++) {
    if (dep->sequence_space == i) {
      op->seq_reqs[dep->sequence_space].seqnum = dep->seqnum;
      op->seq_reqs[dep->sequence_space].batch_size = 1;
    } else {
      op->seq_reqs[i].seqnum = 0;
      op->seq_reqs[i].batch_size = 0;
    }
  }

  proxy->add_op_to_batch(op, batch);

  debug_print(DEBUG_RECOVERY, "Batch %zu, op %zu got seqnum %zu bsize %zu\n",
              batch->batch_id, op->local_reqid, dep->seqnum, dep->batch_size);
  batch->seqnum = std::max(batch->seqnum, dep->seqnum);

  c0->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf, 1);
  c0->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);

  debug_print(DEBUG_RECOVERY, "Leaving fill hole handler\n");
}

static void
recovery_complete_handler(erpc::ReqHandle *req_handle, void *_c) {
  debug_print(DEBUG_RECOVERY, "Got recovery complete message from sequencer\n");
  WorkerContext *c0 = reinterpret_cast<WorkerContext *>(_c);

  // ACK the recovery confirmation message
  c0->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf, 1);
  c0->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);

  for (auto c : *context_vector) {
    c->in_recovery = false;
  }
  debug_print(DEBUG_RECOVERY, "Leaving recovery complete handler\n");
}

void
finish_recovery(WorkerContext *c) {
  debug_print(1, "In recovery!\n");
  debug_print(DEBUG_RECOVERY,
              "[%zu] Going into recovery event loop\n",
              c->thread_id);

  // thread 0 already did this
  if (c->thread_id != 0) {
    c->stop_heartbeat_timer();
    for (auto &el : c->proxies) {
      Proxy *p = el.second;
      if (raft_is_leader(p->raft)) {
        LOG_RECOVERY("[%zu] Calling replicate recovery in finish_recovery()\n",
                     c->thread_id);
        p->replicate_recovery();
      }
    }
  }

  // main waiting loop for recovery
  while (c->in_recovery) {
    for (auto &el : c->proxies) {
      LOG_RECOVERY("[%zu] Waiting in recovery event loop.\n", c->thread_id);
      Proxy *proxy = el.second;
      proxy->call_raft_periodic(0);
      raft_apply_all(proxy->raft);
    }
    c->rpc->run_event_loop(10);
  }
  debug_print(DEBUG_RECOVERY, "[%zu] Done with recovery event loop\n",
              c->thread_id);

  // Re-start client requests that were outstanding
  // Also reset all timers
  for (auto &el : c->proxies) {
    Proxy *proxy = el.second;
    debug_print(DEBUG_RECOVERY, "[%zu] Checking proxy %zu\n",
                c->thread_id, proxy->proxy_id);
    if (!raft_is_leader(proxy->raft)) {
      debug_print(DEBUG_RECOVERY, "[%zu] Continuing!\n", c->thread_id);
      continue;
    }

    debug_print(DEBUG_RECOVERY, "Recovering proxy %zu on thread %zu...\n",
                proxy->proxy_id, c->thread_id);

    for (auto const &x : proxy->appended_batch_map) {
      // Populate dep struct with batch information
      Batch *b = x.second;
      debug_print(DEBUG_RECOVERY, "Freeing recovery tag\n",
                  b->batch_id);
      proxy->tag_pool.free(b->recovery_tag);

      debug_print(DEBUG_RECOVERY,
                  "Outstanding batch %zu being processed, size %zu\n",
                  b->batch_id, b->batch_size());

      b->request_seqnum(false);
    }

    debug_print(DEBUG_RECOVERY, "done!\n");
  }

  // Process the recovery batch
  WorkerContext *c0 = context_vector->at(0);
  if (c0->total_leaders > 0) {
    for (auto recovery_batch : (*(c0->recovery_context->recovery_batches))) {
      if (c->thread_id == recovery_batch->c->thread_id) {
        recovery_batch->proxy->appended_batch_map[
            recovery_batch->batch_id] = recovery_batch;

        debug_print(DEBUG_RECOVERY, "Processing recovery batch, size %zu\n",
                    recovery_batch->batch_size());
        recovery_batch->replicate_seqnums();
      }
    }
  }

  c->init_util_timers();

  delete c->recovery_context;
  debug_print(1, "Done with recovery!\n");
}

#endif
