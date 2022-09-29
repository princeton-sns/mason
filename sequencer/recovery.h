#include "sequencer.h"
#include <inttypes.h>
#include "common.h"


Bitmap *agg_seqnums; 

// The recovery data for a particular proxy machine
typedef struct recovery_data {
    size_t session_num;
    uint64_t nblocks_total;
    uint64_t nblocks_received;
    uint64_t nrequests;  // Number of outstanding requests

    Bitmap *received_seqnums; 
} recovery_data_t;

// Data structures needed for recovery 
class RecoveryContext : public ThreadContext {
    public: 
        volatile bool in_recovery = false;
        uint64_t nproxies;
        std::vector<bool> active_proxies; 

        // Count how many bitmap/dep responses we've gotten
        size_t proxy_responses = 0;  
        size_t hole_acks = SIZE_MAX;  // Got ACK of assigned seqnum in hole-filling
        size_t nleaders = SIZE_MAX;
        size_t nleaders_confirmed = 0;

        size_t tags[MAX_PROXIES];
        erpc::MsgBuffer req_msgbufs[MAX_PROXIES];
        erpc::MsgBuffer resp_msgbufs[MAX_PROXIES];

        // These need to be allocated at runtime, since we don't know ahead
        // of time how many holes there will be
        erpc::MsgBuffer *dep_req_msgbufs;
        erpc::MsgBuffer *dep_resp_msgbufs;

        // These will be allocated as the first recovery responses come in
        recovery_data_t *recovery_data[MAX_PROXIES] = { nullptr };
        std::vector<dependency_t> dependencies;
        size_t batch_requests;  // Requests for batches
        size_t total_holes;  // Total *client* requests that need seqnums


        ~RecoveryContext() {
            for (size_t i = 0; i < other_ip_list.size(); i++) {
                if (active_proxies[i]) {
                    debug_print(1, "Deleting recovery data for %zu\n", i);
                    delete recovery_data[i];
                }
            }
            delete agg_seqnums;

            for (size_t i = 0; i < other_ip_list.size(); i++) {
                if (active_proxies[i]) {
                    rpc->free_msg_buffer(req_msgbufs[i]);
                    rpc->free_msg_buffer(resp_msgbufs[i]);
                }
            }

            for (size_t i = 0; i < total_holes; i++) {
                rpc->free_msg_buffer(dep_req_msgbufs[i]);
                rpc->free_msg_buffer(dep_resp_msgbufs[i]);
            }

            debug_print(1, "Done deleting arrays\n");
        }
};


// Fwd decls
void recovery_handler(erpc::ReqHandle *, void *);
void recover(RecoveryContext *);
void recovery_cleanup(RecoveryContext *);
void process_proxy_bitmap(RecoveryContext *, size_t);
static void notify_proxy_of_backup_cont_func(void *, void*);
static void request_bitmap_cont_func(void *, void *);
static void request_dependencies_cont_func(void *, void *);
static void assign_seqnum_to_hole_cont_func(void *, void *);
static void confirm_recovery_cont_func(void *, void *);


// Initialize recovery context and Rpc endpoint
void
run_recovery_loop(erpc::Nexus *nexus, size_t nleaders)
{
    RecoveryContext *c = new RecoveryContext;

    c->rpc = new erpc::Rpc<erpc::CTransport>(nexus, 
            static_cast<void *>(c), 
            static_cast<uint8_t>(RECOVERY_RPCID),  // RpcID
            sm_handler, 0);

    c->thread_id = RECOVERY_RPCID;
    c->nproxies = 0; 
    c->nleaders = nleaders;

    for (size_t i = 0; i < other_ip_list.size(); i++) {
        c->active_proxies.push_back(false); 
    }

    printf("Handling recovery for %zu leaders\n", nleaders);
    
    // Wait for incoming recovery requests
    while ((!c->in_recovery) && !force_quit) {
        debug_print(1, "Looping waiting to recover\n");
        c->rpc->run_event_loop(5);
    }
    erpc::rt_assert(!force_quit, "Force quitting!");

    recover(c);

    debug_print(DEBUG_RECOVERY, "Done with recovery! Deleting context.\n");
    delete c;
    debug_print(DEBUG_RECOVERY, "Done deleting context\n");
}


// Handler for proxy notifying the backup that it is
// the new sequencer
void
recovery_handler(erpc::ReqHandle *req_handle, void *_context)
{
    auto *c = static_cast<RecoveryContext *>(_context);

    // Acknowledge receipt
    debug_print(DEBUG_RECOVERY, "Heard from a proxy!\n");
    c->rpc->resize_msg_buffer(
            &req_handle->pre_resp_msgbuf, 8);
    c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);

    // Need to do this in case multiple proxies initiate recovery
	if (c->in_recovery == true) {
        debug_print(DEBUG_RECOVERY, "Already in recovery!\n");
		return;
	}

    debug_print(DEBUG_RECOVERY, "Now in recovery!\n");
    c->in_recovery = true;
}

uint64_t 
find_next_hole() {
    static uint64_t next_seqnum = agg_seqnums->nblocks * SEQNUMS_PER_BLOCK + 
        agg_seqnums->base_seqnum;
    static uint64_t last_assigned_seqnum = 0;

    debug_print(DEBUG_RECOVERY, "Base seqnum %lu, nblocks %lu, "
            "seqnums per block %lu, end of bitmap %lu, min %lu\n",
            agg_seqnums->base_seqnum, agg_seqnums->nblocks, 
            SEQNUMS_PER_BLOCK, next_seqnum, last_assigned_seqnum);
    uint8_t *bitmap = agg_seqnums->bitmap;

    // what if base_seqnum is a hole?
    if (last_assigned_seqnum < agg_seqnums->base_seqnum) {
        last_assigned_seqnum = agg_seqnums->base_seqnum; 
    }

    // idx into the byte array
    uint64_t idx = agg_seqnums->get_seqnum_index(last_assigned_seqnum);
    uint8_t bit = static_cast<uint8_t>(last_assigned_seqnum % 8);
    uint64_t temp = idx;
    debug_print(DEBUG_RECOVERY, "Starting at idx %lu, bit %d\n", idx, bit);

    // Start looking at min + 1, since seqnum must be higher than min
    bit++;
    if (bit > 7) {  // we overflowed
        bit = 0;
        idx = (idx + 1) % (agg_seqnums->nblocks * BYTES_PER_BLOCK);
        if (idx == temp) {
            debug_print(DEBUG_RECOVERY, "we are beyond the size of the bitmap\n");
            last_assigned_seqnum = next_seqnum++;
            return last_assigned_seqnum; 
        }
    }

    while (bitmap[idx] == 0xff) {
        bit = 0;
        idx = (idx + 1) % (agg_seqnums->nblocks * BYTES_PER_BLOCK);
        if (idx == temp) {
            debug_print(DEBUG_RECOVERY, "we are beyond the size of the bitmap\n");
            last_assigned_seqnum = next_seqnum++;
            return last_assigned_seqnum; 
        }
    }

    debug_print(DEBUG_RECOVERY, "found byte: %" PRIu8 ", idx %zu\n", 
            bitmap[idx], idx);
    for (; bit < 8; bit++) {
        if ( !(bitmap[idx] & (1 << bit))) {
            uint64_t hole = agg_seqnums->get_seqnum_from_loc(idx, bit);
            bitmap[idx] |= (1 << bit);
            debug_print(DEBUG_RECOVERY, "found the hole: min %" PRIu8 " \
                    hole %" PRIu64 "\n", last_assigned_seqnum, hole);
            last_assigned_seqnum = hole;
            return last_assigned_seqnum; 
        }
    }

    // this should never execuate
    debug_print(DEBUG_RECOVERY, "find_next_hole returning default: byte was %" PRIu8 "\n", 
            bitmap[idx]);
    exit(1);
    return 0xffffffffffffffff;
}


// Connect to all proxy *machines* (not threads) on Rpc endpoint 0
void
connect_to_proxies(RecoveryContext *c)
{
    debug_print(DEBUG_RECOVERY, "Connecting to %d proxies\n", other_ip_list.size());
    for (size_t i = 0; i < other_ip_list.size(); i++) {
        std::string ip = other_ip_list.at(i);

        std::string uri = ip + ":31850";
        debug_print(DEBUG_RECOVERY, "Connecting to %s\n", uri.c_str());

        int session_num = c->rpc->create_session(uri, 0); 
        erpc::rt_assert(session_num >= 0, "Failed to create session");
        c->session_num_vec.push_back(session_num);
        c->tags[i] = i;

        c->req_msgbufs[i] = c->rpc->alloc_msg_buffer_or_die(
                sizeof(offset_payload_t));
        // Don't count the extra byte from the dangling array
        c->resp_msgbufs[i] = c->rpc->alloc_msg_buffer_or_die(
                sizeof(recovery_payload_t) - sizeof(uint8_t) + 
                MAX_BLOCKS_PER_MSG * BYTES_PER_BLOCK);

        debug_print(DEBUG_RECOVERY, "Connection to %s pending.\n", uri.c_str());
    }
}


// Notify proxy machine that this sequencer is the new primary
void
notify_proxy_of_backup(RecoveryContext *c, size_t pid)
{
    debug_print(DEBUG_RECOVERY, "Notifying proxy %zu of "
            "switch to new sequencer\n", pid);

    c->rpc->enqueue_request(c->session_num_vec.at(pid), 
            static_cast<uint8_t>(ReqType::kBackupReady),
            &c->req_msgbufs[pid], &c->resp_msgbufs[pid], 
            notify_proxy_of_backup_cont_func, 
            reinterpret_cast<void *>(&c->tags[pid]));
}


// Send request for bitmaps to all proxy machines
void
request_bitmap(RecoveryContext *c, size_t pid, uint64_t offset)
{
    debug_print(DEBUG_RECOVERY, "Requesting bitmap at offset %lu, pid %zu\n", 
            offset, pid);
    offset_payload_t *payload = reinterpret_cast<offset_payload_t *>(
            c->req_msgbufs[pid].buf);
    payload->offset = offset;

    c->rpc->enqueue_request(c->session_num_vec.at(pid), 
            static_cast<uint8_t>(ReqType::kGetBitmap),
            &c->req_msgbufs[pid], &c->resp_msgbufs[pid], 
            request_bitmap_cont_func, 
            reinterpret_cast<void *>(&c->tags[pid]));
}


// Request dependencies from all proxy machines
void
request_dependencies(RecoveryContext *c)
{
    c->batch_requests = 0;

    debug_print(DEBUG_RECOVERY, "Getting ready to request holes\n");
    for (size_t i = 0; i < other_ip_list.size(); i++) {
        if (c->active_proxies[i]) {
            size_t nrequests = c->recovery_data[i]->nrequests;
            debug_print(DEBUG_RECOVERY, "%zu's number of outstanding requests is %d\n", 
                    i, nrequests);

            // Allocate space for the dependencies and enqueue the request
            if (nrequests > 0) {
                // Resize the response buffer
                erpc::rt_assert((sizeof(dependency_t) * nrequests) <= 
                        (MAX_BLOCKS_PER_MSG * BYTES_PER_BLOCK),
                        "Need more space for dependencies!\n");
                c->rpc->resize_msg_buffer(&c->resp_msgbufs[i], 
                        sizeof(dependency_t) * nrequests);

                c->batch_requests += nrequests;  // Keep track of #batches needing seqnums
                c->rpc->enqueue_request(c->session_num_vec[i], 
                        static_cast<uint8_t>(ReqType::kGetDependencies),
                        &c->req_msgbufs[i], 
                        &c->resp_msgbufs[i], 
                        request_dependencies_cont_func, 
                        reinterpret_cast<void *>(&c->tags[i]));
            } else {
                // We can count this proxy machine as done; no holes
                c->proxy_responses++;
            }
        } else {
            debug_print(DEBUG_RECOVERY, "%zu is not an active proxy! Skipping...\n", i);
        }
    }
}


// Reads in the data for a received block of seqnums and
// allocates memory for the seqnums if it's the first response for this
// proxy. Copies over the bitmap. 
void
process_bitmap_payload(RecoveryContext *c, erpc::MsgBuffer *resp_msgbuf, size_t i)
{
    recovery_payload_t *resp = 
        reinterpret_cast<recovery_payload_t *>(resp_msgbuf->buf);

    if (c->recovery_data[i] == nullptr) {
        // This is the first time we've gotten a message from this proxy
        // Allocate space for and copy over the metadata for this machine's
        // recovery
        debug_print(DEBUG_RECOVERY, "Got first packet from %zu: "
                "%d blocks total, base_seqnum %lu, head_block %lu, "
                "tail_block %lu, nrequests %zu\n", i, resp->nblocks_total, 
                resp->base_seqnum, resp->head_block,
                resp->tail_block, resp->nrequests);

        c->recovery_data[i] = new recovery_data_t(); 
        c->recovery_data[i]->session_num = static_cast<size_t>(c->session_num_vec.at(i));
        c->recovery_data[i]->nblocks_received = 0;
        c->recovery_data[i]->nrequests = resp->nrequests;  // Batches that need seqnums
        c->recovery_data[i]->nblocks_total = resp->nblocks_total;

        Bitmap *received_seqnums = new Bitmap;
        c->recovery_data[i]->received_seqnums = received_seqnums;
        received_seqnums->base_seqnum = resp->base_seqnum;
        received_seqnums->nblocks = resp->nblocks_total;
        received_seqnums->head_block = resp->head_block;
        received_seqnums->tail_block = resp->tail_block;
        delete[] received_seqnums->bitmap; 

        received_seqnums->bitmap = new uint8_t[
            received_seqnums->nblocks * BYTES_PER_BLOCK]();
    }

    // sanity check to make sure we didn't mess up requests for data
    // (shouldn't request more recovery data from a proxy with no blocks left)
    erpc::rt_assert((resp->nblocks_this_resp > 0) && (resp->nblocks_total > 0), 
            "Requested too many blocks!");
    if (resp->nblocks_total == 0) {
        return;
    }

    // Compute byte offset given blocks we've already received, 
    // then copy new seqnums there
    // This isn't affected by reordering since we get a chunk of bitmap
    // at a time from the proxy
    uint64_t offset = (c->recovery_data[i]->nblocks_received * 
            static_cast<uint64_t>(BYTES_PER_BLOCK) * sizeof(uint8_t));
    memcpy(c->recovery_data[i]->received_seqnums->bitmap + offset, 
            resp->received_seqnums, 
            resp->nblocks_this_resp * static_cast<size_t>(BYTES_PER_BLOCK) * 
            sizeof(uint8_t));
    c->recovery_data[i]->nblocks_received += resp->nblocks_this_resp;
}


void
notify_proxy_of_backup_cont_func(void *_context, void *_tag)
{
    size_t i = *(reinterpret_cast<size_t *>(_tag));
    auto *c = static_cast<RecoveryContext *>(_context);
    erpc::MsgBuffer *resp_msgbuf = &c->resp_msgbufs[i];

    size_t nleaders = reinterpret_cast<uint8_t>(*(resp_msgbuf->buf));

    debug_print(DEBUG_RECOVERY, 
            "Got recovery replication response from %zu for %zu leaders\n",
            i, nleaders);

    c->nleaders_confirmed += nleaders;
    if (nleaders > 0) {
        c->active_proxies[i] = true;
        c->nproxies++;
    }
}


void
request_bitmap_cont_func(void *_context, void *_tag)
{
    size_t i = *(reinterpret_cast<size_t *>(_tag));
    auto *c = static_cast<RecoveryContext *>(_context);
    erpc::MsgBuffer *resp_msgbuf = &c->resp_msgbufs[i];

    debug_print(DEBUG_RECOVERY, "Got packet from %d\n", i);
    process_bitmap_payload(c, resp_msgbuf, i);

    if (c->recovery_data[i]->nblocks_received < 
            c->recovery_data[i]->nblocks_total) {

        // Bytes we've already gotten
        uint64_t offset = (c->recovery_data[i]->nblocks_received * 
            static_cast<uint64_t>(BYTES_PER_BLOCK) * sizeof(uint8_t));

        debug_print(DEBUG_RECOVERY, "Sending request for "
                "next set of blocks to %zu. Got %zu blocks, "
                "need %zu blocks, offset %zu\n", 
                i, c->recovery_data[i]->nblocks_received,
                c->recovery_data[i]->nblocks_total, offset);

        request_bitmap(c, i, offset);
        return;
    }

    // If we've processed the last request, add the bitmap to what 
    // we've already received 
    process_proxy_bitmap(c, i);
}


// Process a proxy's completed bitmap; frees bitmap after completion
void
process_proxy_bitmap(RecoveryContext *c, size_t machine_idx)
{
    static bool first = true;

    Bitmap *p_seqnums = c->recovery_data[machine_idx]->received_seqnums;

    c->proxy_responses++;

    if (p_seqnums->nblocks == 0) {
        debug_print(DEBUG_RECOVERY, "Received 0 blocks\n");
        return;
    }
    
    // Persist the metadata and bitmap
    if (DEBUG_RECOVERY) {
        debug_print(DEBUG_RECOVERY, "Metadata is: base: %" PRIu64 " " 
                "n_blocks %d head %d tail %d\n", 
                p_seqnums->base_seqnum, p_seqnums->nblocks, 
                p_seqnums->head_block, p_seqnums->tail_block);

        if (DEBUG_BITMAPS) {
            FILE *f;
            char fname[100];
            sprintf(fname, "temp-%s", other_ip_list[machine_idx].c_str());
            f = fopen(fname, "w");
            fprintf(f, "HEAD %zu, TAIL %zu, BASE %zu, NBLOCKS %zu\n", 
                    p_seqnums->head_block, p_seqnums->tail_block, 
                    p_seqnums->base_seqnum, p_seqnums->nblocks);
            for (size_t i = 0; i < p_seqnums->nblocks * BYTES_PER_BLOCK; i++) {
                fprintf(f, "%" PRIu8 "\n", p_seqnums->bitmap[i]);
            }
            fprintf(f, "\n");
            fclose(f);
        }
    }

    // Integrate this proxy's bitmap into the aggregate seqnum record
    if (first) {
        agg_seqnums = p_seqnums; 
        first = false;
    } else {
        // Integrate proxy's bitmap
        // Truncate original bitmap
        while (agg_seqnums->base_seqnum < p_seqnums->base_seqnum) {
            debug_print(DEBUG_RECOVERY, "Truncating original\n");
            agg_seqnums->truncate();
        }

        // Truncate new bitmap
        while (agg_seqnums->base_seqnum > p_seqnums->base_seqnum) {
            debug_print(DEBUG_RECOVERY, "Truncating new\n");
            p_seqnums->truncate();
        }
        debug_print(DEBUG_RECOVERY, "Agg head: %zu, tail %zu, base_seqnum %zu; "
                "\n\tNew head %zu, tail %zu, base_seqnum %zu\n",
                agg_seqnums->head_block, agg_seqnums->tail_block, agg_seqnums->base_seqnum,
                p_seqnums->head_block, p_seqnums->tail_block, p_seqnums->base_seqnum);

        // Keep the larger bitmap: iterate through each byte and |= it with the 
        // corresponding byte of the smaller bitmap
        if (agg_seqnums->nblocks >= p_seqnums->nblocks) {
            for (size_t i = 0; i < p_seqnums->nblocks * BYTES_PER_BLOCK; i++ ) {
                agg_seqnums->bitmap[(
                        agg_seqnums->head_block * BYTES_PER_BLOCK + i) % (
                        agg_seqnums->nblocks * BYTES_PER_BLOCK)] |=
                    p_seqnums->bitmap[(
                            p_seqnums->head_block * BYTES_PER_BLOCK + i) % (
                            p_seqnums->nblocks * BYTES_PER_BLOCK)];
            }
            debug_print(DEBUG_RECOVERY, "Deleting proxy bitmap\n");
            delete p_seqnums;
            p_seqnums = nullptr;
        } else {
            for (size_t i = 0; i < agg_seqnums->nblocks * BYTES_PER_BLOCK; i++) {
                p_seqnums->bitmap[(
                        p_seqnums->head_block * BYTES_PER_BLOCK + i) % (
                        p_seqnums->nblocks * BYTES_PER_BLOCK)] |=
                    agg_seqnums->bitmap[(
                            agg_seqnums->head_block * BYTES_PER_BLOCK + i) % (
                            agg_seqnums->nblocks * BYTES_PER_BLOCK)];
            }

            debug_print(DEBUG_RECOVERY, "Deleting aggregated bitmap\n");
            delete agg_seqnums;
            agg_seqnums = p_seqnums;
        }
    }
    debug_print(DEBUG_RECOVERY, "Finished integrating\n");
}


void
process_dependency_payload(RecoveryContext *c, erpc::MsgBuffer *resp_msgbuf, 
        size_t i)
{
    debug_print(1, "Processing dependency payload...\n");
	dependency_t *dep_array = reinterpret_cast<dependency_t *>(resp_msgbuf->buf);
	
	size_t ndeps = c->recovery_data[i]->nrequests;
    erpc::rt_assert(resp_msgbuf->get_data_size() == (ndeps * sizeof(dependency_t)), 
            "Dependency payload too big!");

    debug_print(1, "Creating array of dependencies, %zu deps\n", ndeps);
	for (size_t j = 0; j < ndeps; j++) {
        dep_array[j].machine_idx = i;
        c->dependencies.push_back(dep_array[j]); 
		c->total_holes += dep_array[j].batch_size;
	}
    dep_array = nullptr;
    debug_print(1, "Done processing dependency payload...\n");
}


void
request_dependencies_cont_func(void *_context, void *_tag)
{
    // Process the dependencies
    debug_print(DEBUG_RECOVERY, "Got a dependency!\n");
    size_t machine_idx = *(reinterpret_cast<size_t *>(_tag));
    auto *c = static_cast<RecoveryContext *>(_context);
    erpc::MsgBuffer *resp_msgbuf = &c->resp_msgbufs[machine_idx];

    process_dependency_payload(c, resp_msgbuf, machine_idx);

    c->proxy_responses++;
    debug_print(DEBUG_RECOVERY, "%zu dependencies so far.\n", c->proxy_responses);
}


uint64_t
assign_seqnums_to_holes_and_send(RecoveryContext *c, dependency_t *deps)
{
    uint64_t hole = 0;
    dependency_t *response;

    // Allocate space for dependency reqs/responses
    debug_print(DEBUG_RECOVERY, "Allocating space for the hole structs\n");
    c->dep_req_msgbufs = new erpc::MsgBuffer[c->total_holes]();
    c->dep_resp_msgbufs = new erpc::MsgBuffer[c->total_holes]();

    int k = 0;
    debug_print(DEBUG_RECOVERY, "Searching for %zu holes\n", c->total_holes);
    for (size_t i = 0; i < c->batch_requests; i++) {
        // batch has multiple requests
        debug_print(DEBUG_RECOVERY, "%zu holes this dep\n", deps[i].batch_size);
        for (size_t j = 0; j < deps[i].batch_size; j++) {
            c->dep_req_msgbufs[k] = c->rpc->alloc_msg_buffer_or_die(
                    sizeof(dependency_t));
            c->dep_resp_msgbufs[k] = c->rpc->alloc_msg_buffer_or_die(
                    sizeof(dependency_t));

            hole = find_next_hole();

            response = reinterpret_cast<dependency_t *>(c->dep_req_msgbufs[k].buf);

            response->proxy_id = deps[i].proxy_id;
            response->thread_id = deps[i].thread_id;
            response->batch_size = deps[i].batch_size;
            response->seqnum = hole;

            // send the hole/request to proxy
            size_t machine_idx = deps[i].machine_idx;
            c->rpc->enqueue_request(c->session_num_vec.at(machine_idx), 
                    static_cast<uint8_t>(ReqType::kAssignSeqnumToHole), 
                    &c->dep_req_msgbufs[k], &c->dep_resp_msgbufs[k], 
                    assign_seqnum_to_hole_cont_func, nullptr);

            k++;
        }
    }

    debug_print(DEBUG_RECOVERY, "Max seen so far: %zu\n", hole);
    return hole;  // This will be the max seen so far
}


void
assign_seqnum_to_hole_cont_func(void *_c, void *_tag)
{
    (void)_tag;
    RecoveryContext *c = reinterpret_cast<RecoveryContext *>(_c);
    c->hole_acks++;
}


uint64_t 
check_bitmap_for_holelessness(uint64_t highest_assigned_seqnum)
{
    uint64_t idx = agg_seqnums->head_block * BYTES_PER_BLOCK;
    uint64_t temp = 0;
    uint64_t sequence_number = 0;
    uint8_t *bitmap = agg_seqnums->bitmap;

    temp = idx;
    debug_print(DEBUG_RECOVERY, "at start idx: %" PRIu64 " temp: %" PRIu64 "\n", idx, temp);

    erpc::rt_assert(
            agg_seqnums->get_seqnum_from_loc(idx, 0) == agg_seqnums->base_seqnum, 
            "ASSERT BASE_SEQ_NUM != BASE_SEQ_NUM translation is wrong");

    bool foundFirstZero = false;
    for (;;) {
        if (foundFirstZero && (bitmap[idx] != 0x00)) {
            // if we are now looking for 0x00, and it isn't 00, 
            // there is a hole, because we already have a 0
            printf("Found a hole... idx: %" PRIu64 " %" PRIu64 " "
                    "Dying...\n", idx, sequence_number);
            printf("the final byte is %" PRIu8 "\n", bitmap[idx]);
            printf("numbers in byte: \n");
            for (size_t i = 0; i < 8; i++) {
                printf("\t%" PRIu64 "\n", 
                        (agg_seqnums->base_seqnum + idx*8 - 
                         agg_seqnums->head_block * SEQNUMS_PER_BLOCK + i));
            }
            erpc::rt_assert(false, "Found a hole!");
        } 
        
        if (bitmap[idx] != 0xff) {
            // loop through this byte and find the first bit that isn't on
            // make sure the rest of the bits aren't on
            for (size_t i = 0; i < 8; i++) {
                // Check if byte is zero
                if (!(bitmap[idx] & (1 << i))) {
                    if (!foundFirstZero) {
                        // this is the first zero
                        // this is the end of the sequence if it is holeless
                        sequence_number = agg_seqnums->get_seqnum_from_loc(idx, i);
                        printf("The next sequence number to assign is "
                                "%" PRIu64 "\n", sequence_number);
                        debug_print(DEBUG_RECOVERY, 
                                "Found first zero at idx: %" PRIu64 " %" PRIu64 "\n",
                                idx, sequence_number);
                        foundFirstZero = true;
                    } 
                } else {
                    // we have a 1, either we found a zero (BAD == HOLE) or we have not (ok)
                    if (foundFirstZero) {
                        printf("Found a hole at %" PRIu64 "... Dying...",
                               agg_seqnums->get_seqnum_from_loc(idx, i));
                        fflush(stdout);
                        erpc::rt_assert(false);
                    }
                }
            }
        }

        // loop through the rest of the bitmap and make sure they are all 0s
        idx = (idx + 1) % (agg_seqnums->nblocks * BYTES_PER_BLOCK);
        if (idx == temp) {
            // We're done!
            break;
        }
    }

    // if we made it here without dying it is a holeless sequence
    debug_print(DEBUG_RECOVERY, "THE SEQUENCE IS HOLELESS.\n");

    return std::max(sequence_number, highest_assigned_seqnum + 1);
}


void
confirm_recovery(RecoveryContext *c) 
{
    // This needs to go to everyone we've managed to connect to
    for (size_t i = 0; i < other_ip_list.size(); i++) {
        int session_num = c->session_num_vec[i];
        if (!c->rpc->is_connected(session_num)) {
            continue;
        }

        c->rpc->enqueue_request(session_num,
                static_cast<uint8_t>(ReqType::kRecoveryComplete),
                &c->req_msgbufs[i], &c->resp_msgbufs[i],
                confirm_recovery_cont_func, 
                reinterpret_cast<void *>(&c->tags[i]));
    }
}


void
confirm_recovery_cont_func(void *_c, void *_tag)
{
    RecoveryContext *c = reinterpret_cast<RecoveryContext *>(_c);
    size_t i = *(reinterpret_cast<size_t *>(_tag));

    debug_print(DEBUG_RECOVERY, "Got confirmation of recovery completion "
            "from %zu; destroying session %zu\n", i, c->session_num_vec[i]);
    c->proxy_responses++;
    c->rpc->destroy_session(c->session_num_vec[i]);
}

// Gathers recovery data from all proxies, finds holes, and sends out
// sequence numbers to proxies. 
void  
recover(RecoveryContext *c) {
    // How this will work: The sequencer will connect, on thread 0, 
    // to the proxy machine when recovery is underway. The proxy threads 
    // responsible for recovery will collect the bitmaps from all proxy threads
    // and compile them before sending to the sequencer. 
    debug_print(1, "Contacted by a proxy; initiating recovery!\n");
    connect_to_proxies(c); 

    while ((c->nleaders_confirmed < c->nleaders) && !force_quit) {
        c->rpc->run_event_loop(10);
    }
    debug_print(1, "Connected to all proxies.\n");

    if (force_quit) {
        erpc::rt_assert(false, "Force quitting!");
    }

    // Request all the bitmaps
    debug_print(DEBUG_RECOVERY, "Requesting bitmaps...\n");
    c->proxy_responses = 0;
    for (size_t i = 0; i < other_ip_list.size(); i++) {
        if (c->active_proxies[i]) {
            request_bitmap(c, i, 0);
        }
    }
    while ((c->proxy_responses < c->nproxies) && !force_quit) {
        c->rpc->run_event_loop(1);
    }
    if (force_quit) {
        erpc::rt_assert(false, "Force quitting!");
    }
    debug_print(DEBUG_RECOVERY, "Got enough proxy responses!\n");

    c->proxy_responses = 0;
    
    // We have all the bitmaps; request dependencies
    request_dependencies(c);
    while ((c->proxy_responses < c->nproxies) && !force_quit) {
        c->rpc->run_event_loop(1);
    }
    if (force_quit) {
        erpc::rt_assert(false, "Force quitting!");
    }

    // Access the underlying array
    dependency_t *deps = c->dependencies.data();

    // loop through the mins assigning the lowest sequence number greater than the min
    uint64_t highest_assigned_seqnum = assign_seqnums_to_holes_and_send(c, deps);
    
    // Run event loop until we've heard back about all of the hole assignments
    while ((c->hole_acks < c->total_holes) && !force_quit) {
        c->rpc->run_event_loop(1);
    }
    debug_print(DEBUG_RECOVERY, "Proxies were notified of all holes!\n");

    sequence_number = check_bitmap_for_holelessness(highest_assigned_seqnum);

    confirm_recovery(c);

    // Give proxies 50ms to respond (10x retx period), then consider recovery complete
    c->rpc->run_event_loop(50);
}
