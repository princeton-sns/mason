#include "proxy.h"

static void request_seqnum_cont_func(void *, void *);

// Generate load
void
Proxy::add_dummy_client_ops()
{
    for (int i = 0; i < 10; i++) {
        ClientOp *op = client_op_pool.alloc();

        op_counter++;
        op->populate(static_cast<ReqType>(1), i, this->op_counter, 
                proxy_id, nullptr, op_counter, this);

        add_op_to_batch(op);
    }
}


// Contact sequencer for a batch of seqnums
void
Batch::mock_request_seqnum(bool failover)
{
    (void)failover;
    debug_print(DEBUG_THREAD, "Requesting seqnum for batch %lu...\n", batch_id);

    // Enqueue the request; use the batch object as the tag
    Tag *tag = proxy->tag_pool.alloc();
    tag->alloc_msgbufs(c, this, false);
    populate_seqnum_request_buffer(tag);

#if !NO_ERPC
    memcpy(tag->resp_msgbuf.buf, tag->req_msgbuf.buf, sizeof(payload_t));
#else
    memcpy(tag->resp_msgbuf, tag->req_msgbuf, sizeof(payload_t));
    reinterpret_cast<payload_t *>(tag->resp_msgbuf)->seqnum = batch_size();
#endif

    request_seqnum_cont_func(reinterpret_cast<void *>(c),
            reinterpret_cast<void *>(tag));
}


// Respond to the client
void
ClientOp::mock_populate_client_response(client_payload_t *payload)
{
    payload->proxy_id = proxy_id;
    payload->client_id = client_id;
    payload->client_reqid = client_reqid;
}


void
ClientOp::mock_respond_to_client()
{
    client_payload_t *payload = new client_payload_t;
    mock_populate_client_response(payload);
    batch->completed_ops++;
    
    // If this is the last client op, free the entire batch
    if (batch->completed_ops == batch->batch_size()) {
        debug_print(DEBUG_THREAD, "Thread %lu: Batch %lu is finished, "
                "freeing eventually...\n", batch->c->thread_id, batch->batch_id);
        batch->free();
    }

    batch->c->stat_resp_tx_tot++;
    delete payload;
}