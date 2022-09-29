#include "corfu_server.h"


size_t numa_node = 0;

volatile bool force_quit = false;

void
sm_handler(int session_num, erpc::SmEventType sm_event_type,
           erpc::SmErrType sm_err_type, void *_context)
{
    auto *c = static_cast<CorfuContext *>(_context);

    printf ("session_num %d, thread_id %zu: ", session_num, c->thread_id);

    erpc::rt_assert(
            sm_err_type == erpc::SmErrType::kNoError,
            "Got a SM error: " + erpc::sm_err_type_str(sm_err_type));

    if (!(sm_event_type == erpc::SmEventType::kConnected ||
          sm_event_type == erpc::SmEventType::kDisconnected)) {
        throw std::runtime_error("Unexpected SM event!");
    }

    if (sm_event_type == erpc::SmEventType::kConnected) {
        printf("Got a connection!\n");
        c->nconnections++;
    } else {
        printf("Lost a connection!\n");
        c->nconnections--;
    }
}

// Heavily borrowed from erpc code:
// https://github.com/erpc-io/eRPC/blob/master/apps/small_rpc_tput/small_rpc_tput.cc
void
CorfuContext::print_stats()
{
    double seconds = erpc::sec_since(tput_t0);

    double tput_mrps = stat_resp_tx_tot / (seconds * 1000000);
    app_stats[thread_id].mrps = tput_mrps;
    app_stats[thread_id].num_re_tx = rpc->pkt_loss_stats.num_re_tx;

    printf(
            "Thread %zu: %.3f Mrps, re_tx = %zu, still_in_wheel = %zu. "
            "RX: %luK resps. Capacity %lu. Size (B) %lu\n",
            thread_id, tput_mrps, app_stats[thread_id].num_re_tx,
            rpc->pkt_loss_stats.still_in_wheel_during_retx,
            stat_resp_tx_tot / 1000,
            corfu_server_vector.capacity(),
            corfu_server_vector.capacity()*sizeof(corfu_entry_t));

    if (thread_id == 0) {
        app_stats_t accum;
        for (size_t i = 0; i < N_SEQTHREADS; i++) {
            accum += app_stats[i];
        }

        std::string totals = accum.to_string();
    }

    stat_resp_tx_tot = 0;
    rpc->pkt_loss_stats.num_re_tx = 0;

    clock_gettime(CLOCK_REALTIME, &tput_t0);
    fflush(stdout);
}

void
corfu_append_req_handler(erpc::ReqHandle *req_handle, void *_context)
{
    // Get message buffer and check messagesize
    auto *c = static_cast<CorfuContext *>(_context);
    const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
    erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(corfu_entry_t), "entry wrong size");

    auto *entry = reinterpret_cast<corfu_entry_t *>(req_msgbuf->buf);
    debug_print(DEBUG, "Thread %d: received request log position %lu temp %lu noop? %d retcode %u\n",
                c->thread_id, entry->log_position, *reinterpret_cast<int64_t *>(entry->entry_val),
                entry->return_code == RetCode::kNoop, RetCode::kNoop);

    erpc::Rpc<erpc::CTransport>::resize_msg_buffer(&req_handle->pre_resp_msgbuf, 1);
    c->write_to_vector(entry);

    debug_print(DEBUG, "Thread %d enqueueing response\n", c->thread_id);

    c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
    c->stat_resp_tx_tot++;
}

void
corfu_read_req_handler(erpc::ReqHandle *req_handle, void *_context)
{
    // Get message buffer and check messagesize
    auto *c = static_cast<CorfuContext *>(_context);
    const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
    erpc::rt_assert(req_msgbuf->get_data_size() == sizeof(corfu_entry_t),
            "corfu entry wrong size in read");

    auto *req_entry = reinterpret_cast<corfu_entry_t *>(req_msgbuf->buf);
    debug_print(DEBUG, "Thread %d: received read request log position %lu temp %lu\n",
                c->thread_id, req_entry->log_position, *reinterpret_cast<int64_t *>(req_entry->entry_val));

    erpc::Rpc<erpc::CTransport>::resize_msg_buffer(&req_handle->pre_resp_msgbuf, sizeof(corfu_entry_t));

    auto *resp_entry = reinterpret_cast<corfu_entry_t *>(req_handle->pre_resp_msgbuf.buf);
    resp_entry->log_position = req_entry->log_position;

    if (c->read_from_vector(resp_entry)) {
    } else {
        resp_entry->return_code = RetCode::kDoesNotExist;
    }

    debug_print(DEBUG, "Thread %d enqueueing response\n", c->thread_id);

    c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
    c->stat_resp_tx_tot++;
}

void
corfu_thread_func(size_t thread_id, erpc::Nexus *nexus, app_stats_t *app_stats)
{
    CorfuContext c;
    c.thread_id = thread_id;
    c.nconnections = 0;
    c.app_stats = app_stats;
    c.nCorfuServers = FLAGS_ncorfu_servers * N_CORFUTHREADS;

    c.default_entry.log_position = 0;
    memset(&(c.default_entry.entry_val), 0, MAX_CORFU_ENTRY_SIZE); // = 0;
    c.default_entry.return_code = RetCode::kDoesNotExist;

    c.corfu_server_vector.resize(INIT_SIZE, c.default_entry);

    uint64_t i = 0;
    int64_t j = 0;

    if (FLAGS_max_log_position) {
        uint64_t max_log_position = FLAGS_max_log_position;

        LOG_INFO("Writing %zu log positions for mlp %zu with %zu servers\n",
                max_log_position/(c.nCorfuServers/2) + 2, max_log_position, c.nCorfuServers);
        for (i = 0; i < max_log_position/(c.nCorfuServers/2) + 2; i++) {
            while (unlikely(c.corfu_server_vector.capacity() <= i)) {
                c.corfu_server_vector.resize(2 * c.corfu_server_vector.capacity(), c.default_entry);
                debug_print(DEBUG, "Thread %d: resized to %lu\n",
                            thread_id, c.corfu_server_vector.capacity());
            }
            memcpy(c.corfu_server_vector[i].entry_val, &j, sizeof(j));
            c.corfu_server_vector[i].return_code = RetCode::kSuccess;

            j++;
        }

        LOG_INFO("Done writing corfu vector\n");
    }

    LOG_INFO("[%zu] creating rpc\n", thread_id);
    erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c),
                                    static_cast<uint8_t>(thread_id),
                                    sm_handler, 0);
    c.rpc = &rpc;
    // ensure the pre_resp_msgbuf is large enough for our responses
    c.rpc->set_pre_resp_msgbuf_size(sizeof(corfu_entry_t));

    clock_gettime(CLOCK_REALTIME, &c.tput_t0);
    printf("Thread %lu beginning main loop...\n", thread_id);
    fflush(stdout);
    while (!force_quit) {
        printf("Thread %lu calling run_event_loop...\n", thread_id);
        rpc.run_event_loop(1000);
        printf("Thread %lu done calling run_event_loop...\n", thread_id);

        fflush(stdout);
        c.print_stats();
    }
}


void
launch_threads(size_t nthreads, erpc::Nexus *nexus)
{
    // Spin up the requisite number of threads
    std::vector<std::thread> threads(nthreads);
    auto *app_stats = new app_stats_t[nthreads];

    for (size_t i = 0; i < nthreads; i++) {
        printf("Launching thread %zu\n", i);
        threads[i] = std::thread(corfu_thread_func, i, nexus, app_stats);
        erpc::bind_to_core(threads[i],
                           N_CORFUTHREADS > 8? i % 2 : numa_node, nthreads > 8? i / 2 : i);
    }

    for (auto &thread : threads) thread.join();
    delete[] app_stats;
}


static void
signal_handler(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        printf("\n\nSignal %d received, preparing to exit...\n",
               signum);
        force_quit = true;
    }
    if (signum == SIGSEGV) {
        void *array[10];
        int size;

        // get void*'s for all entries on the stack
        size = backtrace(array, 10);

        // print out all the frames to stderr
        fprintf(stderr, "SEGFAULT: signal %d:\n", signum);
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        exit(1);
    }
}


int
main (int argc, char **argv)
{
    printf("Starting Corfu server...\n");
    fflush(stdout);
    int ret = 0;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGSEGV, signal_handler);

    // Parse command line args
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Create a Nexus object (1 per NUMA node)
    std::string uri = FLAGS_my_ip + ":31850";
    printf("Creating nexus at %s\n", uri.c_str());
    fflush(stdout);
    erpc::Nexus nexus(uri, numa_node, 0);

    // Otherwise, register handlers for sequencer's three types of requests:
    nexus.register_req_func(static_cast<uint8_t>(ReqType::kCorfuAppend),
                            corfu_append_req_handler);
    nexus.register_req_func(static_cast<uint8_t>(ReqType::kCorfuRead),
                            corfu_read_req_handler);

    // ...and launch threads.
    size_t nthreads = N_CORFUTHREADS;
    launch_threads(nthreads, &nexus);

    printf("Bye...\n");
    return ret;
}

