// Note you need to rename main() in proxy.cc or else the compiler won't 
// know what to do
#include "proxy.cc"

int
main()
{
    Proxy p = Proxy(); 
    Batch b = Batch();
    erpc::ReqHandle *handle = static_cast<erpc::ReqHandle *>(nullptr);

    b.reset(static_cast<WorkerContext *>(nullptr), &p, 0, 0);
    p.current_batch = &b;

    // Add the first op to the batch
    printf("Creating op0\n");
    ClientOp *op0 = p.client_op_pool.alloc();
    op0->populate(ReqType::kExecuteOpA, 0, 0, 0, 
            handle,
            p.op_counter++, &p);

	p.enqueue_or_add_to_batch(op0); 
    erpc::rt_assert(b.batch_client_ops.size() != 0,
            "First op not in batch!\n");
    erpc::rt_assert(b.batch_client_ops[0]->client_reqid == 0,
            "First op not in batch!\n");
    erpc::rt_assert(p.op_queues[0].empty(), 
            "First op ended up in op_queue!\n");

    // Add another (non-contiguous) element to the batch
    printf("Creating op2\n");
    ClientOp *op2 = p.client_op_pool.alloc();
    op2->populate(ReqType::kExecuteOpA, 0, 2, 0, 
            handle,
            p.op_counter++, &p);

	p.enqueue_or_add_to_batch(op2); 
    erpc::rt_assert(b.batch_client_ops.size() == 1,
            "Batch size is wrong after second op!\n");
    erpc::rt_assert(p.op_queues[0].size() == 1, 
            "Second op didn't get inserted into op_queue!\n");

    // Add a third element to the batch; this should also add
    // client_reqid 2, since it makes the batches contiguous.
    printf("Creating op1\n");
    ClientOp *op1 = p.client_op_pool.alloc();
    op1->populate(ReqType::kExecuteOpA, 0, 1, 0, 
            handle,
            p.op_counter++, &p);

	p.enqueue_or_add_to_batch(op1); 
    p.release_queued_ops(&b.highest_crid_this_batch);
    erpc::rt_assert(b.batch_client_ops.size() == 3,
            "Batch size is wrong after third op!\n");
    erpc::rt_assert(p.op_queues[0].size() == 0, 
            "op_queue isn't empty after adding third op!\n");
    erpc::rt_assert(b.highest_crid_this_batch[0] == 2, 
            "Wrong crid in batch's highest_crid_this_batch map!\n");
    erpc::rt_assert(p.highest_sequenced_crid[0] == -1, 
            "Highest sequenced crid for this proxy should be -1!\n");

    // Create a new batch and "sequence" the old one, try to add a new
    // op to the new batch. It should be queued until the old batch 
    // gets sequenced. 
    Batch b_new = Batch(); 
    b_new.reset(static_cast<WorkerContext *>(nullptr), &p, 0, 0);
    p.current_batch = &b_new;

    printf("Creating op3\n");
    ClientOp *op3 = p.client_op_pool.alloc();
    op3->populate(ReqType::kExecuteOpA, 0, 3, 0, 
            handle,
            p.op_counter++, &p);
    p.enqueue_or_add_to_batch(op3);
    erpc::rt_assert(b_new.batch_client_ops.empty(),
           "Batch should be empty after trying to add op3!\n");
    erpc::rt_assert(p.op_queues[0].size() == 1, 
           "op_queue should have an op in it!\n");
    
    printf("Processing received seqnum\n");
    process_received_seqnum(&b, 10);
    erpc::rt_assert(b_new.batch_client_ops[0]->client_reqid == 3, 
            "New batch's op has the wrong client_reqid\n");
    erpc::rt_assert(p.op_queues[0].empty(), 
            "Proxy's op_queue still has stuff in it\n");

    return 0;
}
