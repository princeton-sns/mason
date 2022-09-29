#ifndef PROXY_GC_H
#define PROXY_GC_H

#include "proxy.h"

extern std::vector<WorkerContext *> *context_vector;

// Fwd decls
uint64_t accumulate_bitmap_counts(uint64_t base_seqnum, uint64_t count, uint64_t sequence_space);
static void garbage_collection_cont_func(void *, void *);

// GC_PAYLOAD_SIZE bytes to the 3 msgbufs and enqueues them
void send_to_next_group(gc_payload_t *gc_payload, WorkerContext *c) {
  // todo connect_and_store_session + MachineIdx is forcing this rather than
  //  a loop
  //  make another vector for gc? this is annoying to do because of the
  //  machineidx and don't want to take advantage of +1
  if (c->received_gc_response0) {
    LOG_GC("About to send to the next idx 0.\n");
    memcpy(c->gc_req_msgbufs[0].buf, gc_payload,
           GC_PAYLOAD_SIZE(nsequence_spaces));
    auto *idx = new size_t;
    *idx = 0;
    c->rpc->enqueue_request(
        c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX0)],
        static_cast<uint8_t>(ReqType::kDoGarbageCollection),
        &c->gc_req_msgbufs[0], &c->gc_resp_msgbufs[0],
        garbage_collection_cont_func, idx);
    c->received_gc_response0 = false;
  } else {
    LOG_GC("Have not received a response from idx 0 not sending.\n");
  }

  if (c->received_gc_response1) {
    LOG_GC("About to send to the next idx 1.\n");
    memcpy(c->gc_req_msgbufs[1].buf, gc_payload,
           GC_PAYLOAD_SIZE(nsequence_spaces));
    auto *idx = new size_t;
    *idx = 1;
    c->rpc->enqueue_request(
        c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX1)],
        static_cast<uint8_t>(ReqType::kDoGarbageCollection),
        &c->gc_req_msgbufs[1], &c->gc_resp_msgbufs[1],
        garbage_collection_cont_func, idx);
    c->received_gc_response1 = false;
  } else {
    LOG_GC("Have not received a response from idx 1 not sending.\n");
  }

  if (c->received_gc_response2) {
    LOG_GC("About to send to the next idx 2.\n");
    memcpy(c->gc_req_msgbufs[2].buf, gc_payload,
           GC_PAYLOAD_SIZE(nsequence_spaces));
    auto *idx = new size_t;
    *idx = 2;
    c->rpc->enqueue_request(
        c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX2)],
        static_cast<uint8_t>(ReqType::kDoGarbageCollection),
        &c->gc_req_msgbufs[2], &c->gc_resp_msgbufs[2],
        garbage_collection_cont_func, idx);
    c->received_gc_response2 = false;
  } else {
    LOG_GC("Have not received a response from idx 2 not sending.\n");
  }
}

// Initialize garbage collection: send neighbor the seqnum count
static void
initiate_garbage_collection(void *_c)
{
    auto *c = reinterpret_cast<WorkerContext *>(_c);

    // Enqueue request to next proxy in the ring
    c->rpc->resize_msg_buffer(&(c->req_msgbuf), GC_PAYLOAD_SIZE(nsequence_spaces));
    auto *req_payload = reinterpret_cast<gc_payload_t *>(c->req_msgbuf.buf);

    for (size_t i = 0; i < nsequence_spaces; i++) {
        while (c->received_ms_seqnums[i]->pending_truncates > 0) {
            c->received_ms_seqnums[i]->mutex.lock();
            // it could have changed since we grabbed the lock
            if (c->received_ms_seqnums[i]->pending_truncates <= 0) {
                c->received_ms_seqnums[i]->mutex.unlock();
                continue;
            }
            c->received_ms_seqnums[i]->pending_truncates--;
            c->received_ms_seqnums[i]->truncate();
            c->received_ms_seqnums[i]->mutex.unlock();
        }
    }

    if (!c->am_gc_leader()) {
        debug_print(DEBUG_GC, "[%zu] Not GC leader. Returning...\n", c->thread_id);
        // will never start again if 00 didn't have leadership when this was called
        c->util_timers[GC_TIMER_IDX].start();
        return;
    }

    for (size_t i = 0; i < nsequence_spaces; i++) {
        Bitmap *bitmap_zero = c->received_ms_seqnums[i];
        while (bitmap_zero->counts[bitmap_zero->head_block] >= SEQNUMS_PER_BLOCK) {
            debug_print(DEBUG_GC, "[%zu] truncating in initialization loop\n", c->thread_id);
            bitmap_zero->truncate();
        }

        uint64_t base_seqnum = bitmap_zero->base_seqnum;
        uint64_t count = 0;
        count = accumulate_bitmap_counts(base_seqnum, count, i);

        if (count >= SEQNUMS_PER_BLOCK) {
            debug_print(DEBUG_GC, "[%zu] Truncating when count >= SEQNUMS_PER_BLOCK on gc leader "
                                  "count %zu seqnums per block %zu\n",
                        c->thread_id, count, SEQNUMS_PER_BLOCK);
            bitmap_zero->truncate();
        }

        req_payload->pairs[i].base_seqnum = base_seqnum;
        req_payload->pairs[i].count = count;
    }

    if (c->only_proxy || FLAGS_no_gc) {
        LOG_GC("not sending GC only_proxy %d FLAGS_no_gc %d\n",
                c->only_proxy, FLAGS_no_gc);
        c->util_timers[GC_TIMER_IDX].start();
        return;
    }

    // rough stats
    if (c->lt_tts == 0) c->lt_tts = erpc::rdtsc();
    c->avg_time_bw_trying_to_send =
            update_avg(erpc::rdtsc() - c->lt_tts, c->avg_time_bw_trying_to_send, c->tts_c);
    c->lt_tts = erpc::rdtsc();
    c->tts_c++;

    LOG_GC("[%zu] Initiating sending GC message to next proxy: "
           "base_seqnum %lu, count %lu sess_num %d\n",
           c->thread_id, req_payload->pairs[0].base_seqnum,
           req_payload->pairs[0].count,
           c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX0)]);
    erpc::rt_assert(c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX0)] != 0,
                    " nextpx session_num is 0\n");

    // rough stats
    if (c->lt_s == 0) c->lt_s = erpc::rdtsc();
    c->avg_time_bw_sending =
            update_avg(erpc::rdtsc() - c->lt_s, c->avg_time_bw_sending, c->s_c);
    c->lt_s = erpc::rdtsc();
    c->s_c++;

    send_to_next_group(req_payload, c);
    c->util_timers[GC_TIMER_IDX].start();
}

// Just an ACK to the sender; also enqueue request to the next proxy
// after accumulating counts
// todo this function can be simplified
void
garbage_collection_handler(erpc::ReqHandle *req_handle, void *_context) 
{
    auto *c = static_cast<WorkerContext *>(_context);

    const erpc::MsgBuffer *handler_msgbuf = req_handle->get_req_msgbuf();

    // Collect counts from every other thread
    auto *payload = reinterpret_cast<gc_payload_t *>(handler_msgbuf->buf);
    erpc::rt_assert(handler_msgbuf->get_data_size() == GC_PAYLOAD_SIZE(nsequence_spaces),
            "GC received wrong size message\n");

    if (!c->am_gc_leader()) {
      // there is only one proxy per thread... I don't think this is the only
      // place that takes advantage of this fact
      for (auto pair : c->proxies) {
        // if this thread isn't any leader, just ack and exit
        if (!raft_is_leader(pair.second->raft)) {
          erpc::Rpc<erpc::CTransport>::resize_msg_buffer(
              &req_handle->pre_resp_msgbuf, 1);
          c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
          return;
        }
      }

      auto *req_payload = reinterpret_cast<gc_payload_t *>(
          c->req_msgbuf.buf);
      for (size_t i = 0; i < nsequence_spaces; i++) {
            // added to replace above
            uint64_t count = accumulate_bitmap_counts(
                    payload->pairs[i].base_seqnum, payload->pairs[i].count, i);
            LOG_GC("[%zu] Not GC leader got GC message! seqnum %lu, count %lu\n",
                        c->thread_id, payload->pairs[i].base_seqnum, count);

            // Enqueue the request; use the batch object as the tag
            c->rpc->resize_msg_buffer(&(c->req_msgbuf), GC_PAYLOAD_SIZE(nsequence_spaces));
            req_payload->pairs[i].base_seqnum = payload->pairs[i].base_seqnum;
            req_payload->pairs[i].count = count;
        }

        if (c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX0)] == 0
        || c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX1)] == 0
        || c->session_num_vec[static_cast<uint8_t>(MachineIdx::NEXTPX2)] == 0) {
            // we probably just haven't set up the connection yet wait to forward until we have the connection
            // just drop out and respond to sender
            LOG_INFO("[%zu] Not connected to next_proxy yet\n", c->thread_id);
        } else {
              LOG_GC("[%zu] Sending GC message to next proxy:\n "
                     "\tbase_seqnum (for 0) %lu, count %lu sess_num %d\n",
                     c->thread_id, req_payload->pairs[0].base_seqnum,
                     req_payload->pairs[0].count,
                     c->session_num_vec[
                         static_cast<uint8_t>(MachineIdx::NEXTPX0)]);
                send_to_next_group(req_payload, c);
        }
    } else { // I am the gc leader
        LOG_GC("Leader completed around the GC ring\n");

        for (size_t i = 0; i < nsequence_spaces; i++) {
            Bitmap *bitmap_zero = c->received_ms_seqnums[i];

            bitmap_zero->mutex.lock();
            if (payload->pairs[i].base_seqnum == bitmap_zero->base_seqnum) {
                if (payload->pairs[i].count >= SEQNUMS_PER_BLOCK) {
                    debug_print(DEBUG_GC, "[%zu] Truncating at the end of a GC round\n",
                                c->thread_id);
                    bitmap_zero->pending_truncates = 1;
                }
            }
            bitmap_zero->mutex.unlock();

        }

        // rough stats
        erpc::rt_assert(c->lt_s != 0, "last time sent was 0 in ring completion!?\n");
        c->avg_ring_duration =
                update_avg(erpc::rdtsc() - c->lt_s, c->avg_ring_duration, c->rd_c);
        c->rd_c++;
    }

    // Respond to sender with an ACK
    erpc::Rpc<erpc::CTransport>::resize_msg_buffer(
            &req_handle->pre_resp_msgbuf, 1); 
    c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}


void
garbage_collection_cont_func(void *_c, void *_idx)
{
  // Compiler complaints silenced
  (void)_c;
  auto *c = reinterpret_cast<WorkerContext *>(_c);
  debug_print(DEBUG_GC, "received ack for GC\n");
  auto *idx = static_cast<size_t *>(_idx);
  switch (*idx) {
    case 0:
      c->received_gc_response0 = true;
      break;
    case 1:
      c->received_gc_response1 = true;
      break;
    case 2:
      c->received_gc_response2 = true;
      break;
  }
}


uint64_t 
accumulate_bitmap_counts(uint64_t base_seqnum, uint64_t count, uint64_t sequence_space)
{
    for (auto &context : *context_vector) {
        Bitmap *bitmap = context->received_ms_seqnums[sequence_space];
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
