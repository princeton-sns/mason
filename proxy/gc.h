#ifndef PROXY_GC_H
#define PROXY_GC_H

#include "proxy.h"

extern std::vector<WorkerContext *> *context_vector;

// Fwd decls
uint64_t accumulate_bitmap_counts(uint64_t, uint64_t);
static void garbage_collection_cont_func(void *, void *);


// Initialize garbage collection: send neighbor the seqnum count
static void
initiate_garbage_collection(void *_c)
{
    WorkerContext *c = reinterpret_cast<WorkerContext *>(_c);
    debug_print(DEBUG_GC, "[%zu] Initiating a round of GC...\n", c->thread_id);

    if (!c->am_gc_leader()) {
        while (c->received_seqnums->pending_truncates > 0) {
            c->received_seqnums->mutex.lock();
            // it could have changed since we grabbed the lock
            if (c->received_seqnums->pending_truncates <= 0) {
                c->received_seqnums->mutex.unlock();
                continue;
            }
            c->received_seqnums->pending_truncates--;
            c->received_seqnums->truncate();
            c->received_seqnums->mutex.unlock();
        }
        debug_print(DEBUG_GC, "[%zu] Not GC leader. Returning...\n", c->thread_id);
        // will never start again if 00 didn't have leadership when this was called
        c->util_timers[GC_TIMER_IDX].start();
        return;
    }

    Bitmap *bitmap_zero = c->received_seqnums;
    while (bitmap_zero->counts[bitmap_zero->head_block] >= SEQNUMS_PER_BLOCK) {
        debug_print(DEBUG_GC, "[%zu] truncating in initialization loop\n", c->thread_id);
        bitmap_zero->truncate();
    }

    uint64_t base_seqnum = bitmap_zero->base_seqnum;
    uint64_t count = 0;
    count = accumulate_bitmap_counts(base_seqnum, count);

    if (count >= SEQNUMS_PER_BLOCK) {
        debug_print(DEBUG_GC, "[%zu] Truncating when count >= SEQNUMS_PER_BLOCK on gc leader "
                              "count %zu seqnums per block %zu\n",
                    c->thread_id, count, SEQNUMS_PER_BLOCK);
        bitmap_zero->truncate();
    }

    // accumulate_bitmap increases pending truncates,
    // they each individually truncate when initiate() is called...
    if (c->only_proxy || FLAGS_no_gc) {
        c->util_timers[GC_TIMER_IDX].start();
        return;
    }

    // Enqueue request to next proxy in the ring
    auto *req_payload = reinterpret_cast<gc_payload_t *>(
            c->req_msgbuf.buf);
    req_payload->base_seqnum = base_seqnum;
    req_payload->count = count;

    if (c->lt_tts == 0) c->lt_tts = erpc::rdtsc();
    c->avg_time_bw_trying_to_send =
            update_avg(erpc::rdtsc() - c->lt_tts, c->avg_time_bw_trying_to_send, c->tts_c);
    c->lt_tts = erpc::rdtsc();
    c->tts_c++;

    if (!c->received_gc_response) {
        c->util_timers[GC_TIMER_IDX].start();
        return;
    }

    debug_print(DEBUG_GC, "[%zu] Initiating sending GC message to next proxy: "
            "base_seqnum %lu, count %lu sess_num %d\n",
            c->thread_id, base_seqnum, count, c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX)]);
    erpc::rt_assert(c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX)] != 0,
            " nextpx session_num is 0\n");
    c->received_gc_response = false;

    if (c->lt_s == 0) c->lt_s = erpc::rdtsc();
    c->avg_time_bw_sending =
            update_avg(erpc::rdtsc() - c->lt_s, c->avg_time_bw_sending, c->s_c);
    c->lt_s = erpc::rdtsc();
    c->s_c++;

    c->rpc->enqueue_request(
            c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX)],
            static_cast<uint8_t>(ReqType::kDoGarbageCollection),
            &c->req_msgbuf, &c->resp_msgbuf,
            garbage_collection_cont_func,
            nullptr);

    c->util_timers[GC_TIMER_IDX].start();
}


// Just an ACK to the sender; also enqueue request to the next proxy 
// after accumulating counts
void
garbage_collection_handler(erpc::ReqHandle *req_handle, void *_context) 
{
    auto *c = static_cast<WorkerContext *>(_context);

    const erpc::MsgBuffer *handler_msgbuf = req_handle->get_req_msgbuf();

    // Collect counts from every other thread
    gc_payload_t *payload = reinterpret_cast<gc_payload_t *>(
            handler_msgbuf->buf);

    if (!c->am_gc_leader()) {
        // added to replace above
        uint64_t count = accumulate_bitmap_counts(
                payload->base_seqnum, payload->count);
        debug_print(DEBUG_GC, "[%zu] Not GC leader got GC message! seqnum %lu, count %lu\n",
                    c->thread_id, payload->base_seqnum, count);

        // Enqueue the request; use the batch object as the tag
        gc_payload_t *req_payload = reinterpret_cast<gc_payload_t *>(
                c->req_msgbuf.buf);
        req_payload->base_seqnum = payload->base_seqnum;
        req_payload->count = count;

        debug_print(DEBUG_GC, "[%zu] Sending GC message to next proxy:\n "
                              "\tbase_seqnum %lu, count %lu sess_num %d\n",
                    c->thread_id, req_payload->base_seqnum, req_payload->count,
                    c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX)]);
        if (c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX)] == 0) {
            // we probably just haven't set up the connection yet wait to forward until we have the connection
            // just drop out and respond to sender
            LOG_INFO("%zu Not connected to next_proxy yet\n", c->thread_id);
        } else {

            debug_print(DEBUG_GC, "[%zu] Sending GC message to next proxy:\n "
                                  "\tbase_seqnum %lu, count %lu sess_num %d\n",
                        c->thread_id, req_payload->base_seqnum, req_payload->count,
                        c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX)]);

            if (!c->received_gc_response) {
                // don't forward unless I've received the ack
            } else {
                c->received_gc_response = false;
                c->rpc->enqueue_request(
                        c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX)],
                        static_cast<uint8_t>(ReqType::kDoGarbageCollection),
                        &c->req_msgbuf, &c->resp_msgbuf,
                        garbage_collection_cont_func,
                        nullptr);
            }
        }
    } else {
        debug_print(DEBUG_GC, "Leader completed around the GC ring\n");
        Bitmap *bitmap_zero = c->received_seqnums;

        erpc::rt_assert(c->lt_s != 0, "last time sent was 0 in ring completion!?\n");
        c->avg_ring_duration =
                update_avg(erpc::rdtsc() - c->lt_s, c->avg_ring_duration, c->rd_c);
        c->rd_c++;

        if (payload->base_seqnum == bitmap_zero->base_seqnum) {
            if (payload->count >= SEQNUMS_PER_BLOCK) {
                debug_print(DEBUG_GC, "[%zu] Truncating at the end of a GC round\n",
                        c->thread_id);
                bitmap_zero->truncate();
            }
        }
    }

    // Respond to sender with an ACK
    erpc::Rpc<erpc::CTransport>::resize_msg_buffer(
            &req_handle->pre_resp_msgbuf, 1); 
    c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}


void
garbage_collection_cont_func(void *_c, void *null)
{
    // Compiler complaints silenced
    (void)_c;
    auto *c = reinterpret_cast<WorkerContext *>(_c);
    debug_print(DEBUG_GC, "received ack for GC\n");
    c->received_gc_response = true;
    (void)null;
}


uint64_t 
accumulate_bitmap_counts(uint64_t base_seqnum, uint64_t count)
{
    for (auto &context : *context_vector) {
        Bitmap *bitmap = context->received_seqnums;
        bitmap->mutex.lock();
        if (base_seqnum == bitmap->base_seqnum) {
            // Accumulate the count...
            count += bitmap->counts[bitmap->head_block];
            
            // ...and mark for truncation if the count is sufficient
            if (count >= SEQNUMS_PER_BLOCK) {
                debug_print(DEBUG_GC, "Context %d Truncating when base_seqnum "
                        "is equal, local count is %zu total count is %zu\n",
                        context->thread_id, bitmap->counts[bitmap->head_block], count);
                bitmap->pending_truncates=1;//++;
            }
        } else if (base_seqnum < bitmap->base_seqnum) {
            // This context's base_seqnum is higher than the "leader's"; 
            // tell everyone else it is safe to truncate
            debug_print(DEBUG_GC, "Context %d This context's base_seqnum is higher than the"
                           "leader's tell everyone else it is safe to truncate\n", context->thread_id);
            count += SEQNUMS_PER_BLOCK;
        } else if (base_seqnum > bitmap->base_seqnum) {
            // Safe to truncate
            debug_print(DEBUG_GC, "Context %d Truncating when base_seqnums "
                    "aren't equal, (%zu %zu)local count is %zu total count is %zu\n",
                    context->thread_id,  base_seqnum, bitmap->base_seqnum, bitmap->counts[bitmap->head_block], count);
            bitmap->pending_truncates=1;
        }
        bitmap->mutex.unlock();
    }

    return count;
}

#endif
