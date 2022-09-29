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
                proxy_id, nullptr, op_counter, this, c);

        add_op_to_batch(op);
    }
}


// Contact sequencer for a batch of seqnums
void
Batch::mock_request_seqnum(bool failover)
{
    (void)failover; // TODO fix!
    debug_print(DEBUG_THREAD, "Requesting seqnum for batch %lu...\n", batch_id);

    // Enqueue the request; use the batch object as the tag
    Tag *tag = proxy->tag_pool.alloc();
    tag->alloc_msgbufs(c, this, 0);
    populate_seqnum_request_buffer(tag);

#if !NO_ERPC
    memcpy(tag->resp_msgbuf.buf, tag->req_msgbuf.buf, sizeof(payload_t));
    reinterpret_cast<payload_t *>(tag->resp_msgbuf.buf)->seqnum = batch_size();
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
    payload->seqnum = seqnum;
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


void
test_bitmaps(WorkerContext *c)
{
    // Test inserting multiple sequence numbers, without growing
    Bitmap *b = c->received_seqnums;
    for (uint64_t i = 0; i < 256; i++) {
        b->insert_seqnum(i);
    }
    for (size_t i = 0; i < 256/8; i++) {
        erpc::rt_assert(b->bitmap[i] == 0xff,
                "Inserting multiple sequence numbers failed!\n");
    }
    debug_print(1, "Sequence numbers inserted successfully\n");
    b->print();

    // Grow until we can truncate a block
    debug_print(1, "Attempting to truncate...\n");
    for (uint64_t i = 256; i < SEQNUMS_PER_BLOCK; i++) {
        b->insert_seqnum(i); 
    }
    erpc::rt_assert(b->counts[b->head_block] == SEQNUMS_PER_BLOCK, 
            "Not enough seqnums to truncate!\n");
    b->truncate();
    debug_print(1, "Finished truncating!\n");
    b->print();

    // Inserting multiple sequence numbers, with growing
    debug_print(1, "Attempting to insert some high sequence numbers...\n");
    for (uint64_t i = 106512; i < 200000; i++) {
        b->insert_seqnum(i);
    }
    debug_print(1, "Inserted the numbers!\n");
    b->print();

    uint64_t base_seqnum = b->base_seqnum;
    debug_print(1, "Checking the indexing functions...\n");
    erpc::rt_assert(b->get_seqnum_index(base_seqnum) == 0, "Failed index!\n");
    erpc::rt_assert(b->get_seqnum_bitmask(base_seqnum) == 1, "Failed bitmask!\n");
    erpc::rt_assert(b->get_seqnum_block(base_seqnum) == 0, "Failed block!\n");
    uint64_t test_seqnum = 106512;
    uint64_t index = b->get_seqnum_index(test_seqnum);
    uint8_t bit_idx = 106512 % 8; 
    
    erpc::rt_assert(b->get_seqnum_from_loc(index, bit_idx) == test_seqnum, 
            "Failed seqnum_from_loc!\n");
    erpc::rt_assert(b->get_seqnum_bitmask(test_seqnum) & b->bitmap[index], 
            "Failed check of bitmap!\n");
    debug_print(1, "Passed the indexing functions...\n");

}
