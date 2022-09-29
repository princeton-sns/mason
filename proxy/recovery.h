#ifndef PROXY_RECOVERY_H
#define PROXY_RECOVERY_H

#include "proxy.h"
#include "gc.h"


int connect_and_store_session(WorkerContext *, std::string, int, MachineIdx);
int connect_and_store_session(WorkerContext *, std::string, int, MachineIdx, Proxy *);
void process_received_seqnum(Batch *, uint64_t);


// Sever connection to original sequencer and connect to the backup
void
replace_seq_connection(WorkerContext *c, int remote_tid) 
{
    debug_print(DEBUG_RECOVERY, "[%zu] Replacing seq connection\n", c->thread_id);
    c->stop_heartbeat_timer();
    connect_and_store_session(c, backupseq_ip, remote_tid, MachineIdx::SEQ);
}


// Destroy the connection to the backup sequencer; we will be receiving 
// requests from it until we are done with recovery, and then we can initiate
// a new connection on the appropriate thread in the SEQ position of the 
// session_num_vec
void
initiate_recovery_cont_func(void *_c, void *_tag)
{
    debug_print(DEBUG_RECOVERY, "heard back from backup!\n");
    WorkerContext *c = reinterpret_cast<WorkerContext *>(_c);

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
initiate_recovery(void *_c)
{
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

    debug_print(DEBUG_RECOVERY, "snv %d\n", c->session_num_vec[static_cast<uint8_t>(MachineIdx ::SEQ)]);

    debug_print(DEBUG_RECOVERY, "[%zu] 1\n", c->thread_id);

    bool ret = c->rpc->reset_session(
            c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)]);

    debug_print(DEBUG_RECOVERY, "[%zu] 1\n", c->thread_id);

    erpc::rt_assert(ret, "Session reset failed!");

    debug_print(DEBUG_RECOVERY, "[%zu] 1\n", c->thread_id);

    c->session_num_vec[static_cast<uint8_t>(MachineIdx::SEQ)] = -1;

    // Pick a proxy and create a batch to associate tags to; 
    // The payload will be ignored, don't bother to update
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

    tag->alloc_msgbufs(c, batch, 0);

    debug_print(DEBUG_RECOVERY, "Getting ready to enqueue recovery request\n");
    uint8_t session_num = c->session_num_vec[static_cast<uint8_t>(
            MachineIdx::BACKUP_SEQ)];

    c->rpc->enqueue_request(session_num,
            static_cast<uint8_t>(ReqType::kInitiateRecovery),
            &tag->req_msgbuf, &tag->resp_msgbuf, 
            initiate_recovery_cont_func,
            reinterpret_cast<void *>(tag));

    while (c->session_num_vec[static_cast<uint8_t>(MachineIdx::BACKUP_SEQ)] > 0) {
        debug_print(DEBUG_RECOVERY, "Waiting in event loop until session is dead\n");
        c->rpc->run_event_loop_once();
        proxy->call_raft_periodic(1);
    }
}


// Collect bitmaps from other threads
void
collect_bitmaps(WorkerContext *c) 
{
    c->recovery_context->agg = new Bitmap;
    Bitmap *agg = c->recovery_context->agg;
    int first = 1;

    for (auto &context : *context_vector) {
        Bitmap *bitmap = context->received_seqnums;

        if (first) {
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
        } else {
            // Next bitmap 
            Bitmap *next = new Bitmap;
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
                for (size_t i = 0; i < next->nblocks * BYTES_PER_BLOCK; i++ ) {
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


void
send_bitmap(erpc::ReqHandle *req_handle, WorkerContext *c, uint64_t offset)
{
    RecoveryContext *rc = c->recovery_context;
    Bitmap *agg = rc->agg;

    size_t block_offset = offset/BYTES_PER_BLOCK;
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
    recovery_payload_t *payload = reinterpret_cast<recovery_payload_t *>(
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
}


void
collect_dependencies(WorkerContext *c) {
    RecoveryContext *rc = c->recovery_context;

    for (auto &context : *context_vector) {
        for (auto &el : context->proxies) {
            Proxy *proxy = el.second;
            if (!raft_is_leader(proxy->raft)) {
                continue;
            }

            for (auto const &x : proxy->appended_batch_map) {
                // Populate dep struct with batch information
                Batch *batch = x.second;
                dependency_t dep;
                dep.proxy_id = batch->proxy_id; 
                dep.thread_id = context->thread_id;
                dep.batch_size = batch->batch_size();

                rc->dependencies.push_back(dep);
            }
        }
    }

    debug_print(DEBUG_RECOVERY, "Found %zu holes\n", 
            rc->dependencies.size());
}


// Tell the backup how many leader responses we got
// This is handled by thread0
void
WorkerContext::respond_backup_ready()
{
    // Some sanity checks
    debug_print(DEBUG_RECOVERY, "Responding to backup_ready\n");
    erpc::rt_assert(thread_id == 0, 
            "Wrong thread trying to respond to sequencer!\n");
    erpc::rt_assert(backup_ready_handle != nullptr, "Trying to respond to backup again!");

    erpc::MsgBuffer *msgbuf = &backup_ready_handle->pre_resp_msgbuf;
    rpc->resize_msg_buffer(&backup_ready_handle->pre_resp_msgbuf, 1);
    uint8_t *leader_count = reinterpret_cast<uint8_t *>(msgbuf->buf);
    *leader_count = total_leaders;
    rpc->enqueue_response(backup_ready_handle, msgbuf);
    backup_ready_handle = nullptr;  // make sure we don't do this again
}


// All leaders on a thread will confirm recovery
// recovery_confirmations should get incremented on each confirmation. 
// When it equals the number of leaders for this workercontext, 
// send a response to the backup. 
void
WorkerContext::confirm_recovery_replication(bool incr_confirmations)
{
    WorkerContext *c0 = context_vector->at(0);
    if (incr_confirmations) {
        c0->recovery_confirmations++;
    }

    if (thread_id == 0) {
        while (recovery_confirmations != total_leaders) {
            for (auto &el : proxies){
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
// Track the number of leaders; proceed with recovery after al leaders have committed 
// move to the backup seq. 
static void
replicate_recovery() 
{
    WorkerContext *c0 = context_vector->at(0);
    for (auto c : *context_vector) {
        c->in_recovery = true;
        c->stop_heartbeat_timer();

        for (auto &el : c->proxies) {
            Proxy *proxy = el.second;

            if (!raft_is_leader(proxy->raft)) {
                continue;
            }

            c->leaders_this_thread++;
            c0->total_leaders++;

            proxy->replicate_recovery();
        }
        debug_print(DEBUG_RECOVERY, "%zu leaders this thread\n", 
                c->leaders_this_thread);
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
backup_ready_handler(erpc::ReqHandle *req_handle, void *_c)
{
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
request_bitmap_handler(erpc::ReqHandle *req_handle, void *_c)
{
    WorkerContext *c = reinterpret_cast<WorkerContext *>(_c);

    debug_print(1, "in request_bitmap_handler\n");

    Proxy *proxy = nullptr;  // Need to find a leader/context to handle recovery batch
    WorkerContext *context = nullptr;

    const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
    erpc::rt_assert(req_msgbuf->get_data_size() > 0, "bitmap msgbuf too small!");
    uint64_t offset = reinterpret_cast<offset_payload_t *>(req_msgbuf->buf)->offset;
    debug_print(DEBUG_RECOVERY, "Working on offset %lu\n", offset);

    // The first message
    if (offset == 0) {
        c->recovery_context = new RecoveryContext(c);

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
        Batch *batch = proxy->batch_pool.alloc();
        c->recovery_context->recovery_batch = batch;

        batch->reset(context, proxy, proxy->proxy_id, proxy->batch_counter++);
        debug_print(1, "Recovery batchid: %zu\n", batch->batch_id);

        // Collect everyone else's bitmaps, dependencies
        collect_bitmaps(c);

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
                fprintf(f, "%" PRIu8 "\n", p_seqnums->bitmap[i]);
            }
            fprintf(f, "\n");
            fclose(f);
        }

        collect_dependencies(c);
    }
    erpc::rt_assert(offset/BYTES_PER_BLOCK < c->recovery_context->agg->nblocks, 
            "The offset exceeds the bitmap size!\n");

    // Send to the recovering sequencer
    send_bitmap(req_handle, c, offset);
    debug_print(DEBUG_RECOVERY, "Leaving request bitmap handler\n");
}


static void
request_dependencies_handler(erpc::ReqHandle *req_handle, void *_c)
{
    debug_print(DEBUG_RECOVERY, "In request_dependencies handler\n");
    WorkerContext *c = reinterpret_cast<WorkerContext *>(_c);

    erpc::MsgBuffer &resp_msgbuf = req_handle->dyn_resp_msgbuf; 
    resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(
            sizeof(dependency_t) * c->recovery_context->dependencies.size());

    // Fill in the buffer
    dependency_t *payload = reinterpret_cast<dependency_t *>(
            resp_msgbuf.buf);
    memcpy(payload, c->recovery_context->dependencies.data(), 
            sizeof(dependency_t) * c->recovery_context->dependencies.size());
    c->rpc->enqueue_response(req_handle, &resp_msgbuf);
    debug_print(DEBUG_RECOVERY, "Leaving request_dependencies handler\n");
}


static void
fill_hole_handler(erpc::ReqHandle *req_handle, void *_c)
{
    debug_print(DEBUG_RECOVERY, "In fill hole handler\n");
    WorkerContext *c0 = reinterpret_cast<WorkerContext *>(_c);
    Batch *batch = c0->recovery_context->recovery_batch;
    Proxy *proxy = batch->proxy; 

    const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
    erpc::rt_assert(req_msgbuf->get_data_size() > 0, 
            "Message error!");

    dependency_t *dep = reinterpret_cast<dependency_t *>(req_msgbuf->buf);

    ClientOp *op = proxy->client_op_pool.alloc();
    op->populate(ReqType::kNoop, UINT16_MAX, INT64_MAX,
                 proxy->proxy_id, nullptr, proxy->op_counter++, proxy, proxy->c);
    op->seqnum = dep->seqnum;

    proxy->add_op_to_batch(op, batch);

    debug_print(DEBUG_RECOVERY, "Batch %zu, op %zu got seqnum %zu\n", 
            batch->batch_id, op->local_reqid, dep->seqnum);
    batch->seqnum = std::max(batch->seqnum, dep->seqnum);

    c0->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf, 1);
    c0->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);

    debug_print(DEBUG_RECOVERY, "Leaving fill hole handler\n");
}


static void
recovery_complete_handler(erpc::ReqHandle *req_handle, void *_c)
{
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
    debug_print(DEBUG_RECOVERY, "[%zu] Going into recovery event loop\n", c->thread_id);
    while (c->in_recovery) {
        for (auto &el : c->proxies){
            debug_print(DEBUG_RECOVERY, "[%zu] in for loop\n", c->thread_id);
            Proxy *proxy = el.second;
            proxy->call_raft_periodic(0);
            debug_print(DEBUG_RECOVERY, "[%zu] apply_all\n", c->thread_id);
            raft_apply_all(proxy->raft);
        }
        debug_print(DEBUG_RECOVERY, "[%zu] before run event loop\n", c->thread_id);
        c->rpc->run_event_loop(10);
        debug_print(DEBUG_RECOVERY, "[%zu] after run event loop\n", c->thread_id);
    }
    debug_print(DEBUG_RECOVERY, "[%zu] Done with recovery event loop\n", c->thread_id);


    // Re-start client requests that were outstanding
    // Also reset all timers
    for (auto &el : c->proxies) {
        Proxy *proxy = el.second;
        debug_print(DEBUG_RECOVERY, "[%zu] Checking proxy %zu\n", c->thread_id, proxy->proxy_id);
        if (!raft_is_leader(proxy->raft)) {
            debug_print(DEBUG_RECOVERY, "[%zu] Continuing!\n", c->thread_id);
            continue;
        }

        debug_print(DEBUG_RECOVERY, "Recovering proxy %zu on thread %zu...\n", 
                proxy->proxy_id, c->thread_id);

        for (auto const& x : proxy->appended_batch_map) {
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
        Batch *recovery_batch = c0->recovery_context->recovery_batch; 
        if (c->thread_id == recovery_batch->c->thread_id) {
            recovery_batch->proxy->appended_batch_map[
                recovery_batch->batch_id] = recovery_batch;

            debug_print(DEBUG_RECOVERY, "Processing recovery batch, size %zu\n",
                    recovery_batch->batch_size());
            recovery_batch->replicate_seqnums_noncontig();
        }
    }

    c->init_util_timers();

    delete c->recovery_context; 
    debug_print(1, "Done with recovery!\n");
}

#endif
