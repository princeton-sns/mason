#include "sequencer.h"
#include "recovery.h"

size_t numa_node = 0;

DEFINE_bool(am_backup, false,
        "Set to true if this sequencer is a backup");
DEFINE_string(my_ip, "",
        "Sequencer IP address for eRPC setup");
DEFINE_string(other_ips, "", 
        "IP addresses to connect to if this is the backup");
DEFINE_string(out_dir, "",
        "Output directory");
DEFINE_uint64(nleaders, 0,
        "Total number of proxy groups (needed for recovery)");

volatile std::atomic_uint64_t sequence_number(0);
volatile bool force_quit = false;

std::vector<std::string> other_ip_list;
size_t nproxy_threads;
size_t nproxy_machines;

void notify_proxy_of_backup(RecoveryContext *, size_t); 

FILE *fp;

void
sm_handler(int session_num, erpc::SmEventType sm_event_type, 
        erpc::SmErrType sm_err_type, void *_context)
{
    auto *c = static_cast<ThreadContext *>(_context);

    printf ("session_num %d, thread_id %zu: ", session_num, c->thread_id);

    erpc::rt_assert(
            sm_err_type == erpc::SmErrType::kNoError,
            "Got a SM error: " + erpc::sm_err_type_str(sm_err_type));

    if (!(sm_event_type == erpc::SmEventType::kConnected ||
                sm_event_type == erpc::SmEventType::kDisconnected)) {
        throw std::runtime_error("Unexpected SM event!");
    }

    if (sm_event_type == erpc::SmEventType::kConnected) {
        c->nconnections++;
        if (c->thread_id == RECOVERY_RPCID) {
            std::vector<int>::iterator it; 
            int pid; 

            it = std::find(c->session_num_vec.begin(), 
                    c->session_num_vec.end(), session_num); 
            erpc::rt_assert(it != c->session_num_vec.end(), 
                    "Couldn't find session number for this session!\n"); 

            pid = std::distance(c->session_num_vec.begin(), it);    
            erpc::rt_assert(pid >= 0, "Session number is less than 0!\n");
            
            // Send message to each proxy, notifying them that the backup should
            // be used from now on. 
            auto *rc = static_cast<RecoveryContext *>(_context);
            size_t i = static_cast<size_t>(std::abs(pid));

            notify_proxy_of_backup(rc, i); 
        }
    } else {
        printf("Lost a connection!\n");
        c->nconnections--;
    }
}

// Heavily borrowed from erpc code:
// https://github.com/erpc-io/eRPC/blob/master/apps/small_rpc_tput/small_rpc_tput.cc
void
SeqContext::print_stats()
{
    double seconds = erpc::sec_since(tput_t0);

    double tput_mrps = stat_resp_tx_tot / (seconds * 1000000);
    app_stats[thread_id].mrps = tput_mrps;
    app_stats[thread_id].num_re_tx = rpc->pkt_loss_stats.num_re_tx;

    printf(
        "Thread %zu: %.3f Mrps, re_tx = %zu, still_in_wheel = %zu. "
        "RX: %lu resps.\n",
        thread_id, tput_mrps, app_stats[thread_id].num_re_tx,
        rpc->pkt_loss_stats.still_in_wheel_during_retx,
        stat_resp_tx_tot);
    fflush(stdout);

    if (thread_id == 0) {
        app_stats_t accum;
        for (size_t i = 0; i < N_SEQTHREADS; i++) {
            accum += app_stats[i];
        }

        std::string totals = accum.to_string();
        fwrite(totals.c_str(), sizeof(char), strlen(totals.c_str()), fp);
        printf("TOTAL: %s\n\n", totals.c_str());
        uint64_t tmp = sequence_number;
        printf("Current seqnum: %lu\n", tmp);
        fflush(fp);
    }

    stat_resp_tx_tot = 0;
    rpc->pkt_loss_stats.num_re_tx = 0;

    clock_gettime(CLOCK_REALTIME, &tput_t0);
    fflush(stdout);
}

void
seqnumreq_handler(erpc::ReqHandle *req_handle, void *_context)
{
    // Get message buffer and check messagesize
    auto *c = static_cast<SeqContext *>(_context);
    const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
    assert(req_msgbuf->get_data_size() == sizeof(payload_t));

    // Get the batch size and increment seqnums
    // If sn = 0 (we haven't started) and batch_size 5:
    // requester gets 0,1,2,3,4, next sn is 5, return 4 to requester.
    payload_t *payload = reinterpret_cast<payload_t *>(req_msgbuf->buf);
    debug_print(DEBUG, "Thread %d: received request from %d with batch size: %d\n",
            payload->proxy_id, c->thread_id, payload->batch_size);

    erpc::Rpc<erpc::CTransport>::resize_msg_buffer(&req_handle->pre_resp_msgbuf, 
            sizeof(payload_t));

    payload_t *response = reinterpret_cast<payload_t *>(
            req_handle->pre_resp_msgbuf.buf);
    memcpy(response, payload, sizeof(payload_t));

    // Check if we have this proxy_id...
    if (c->amo_map.size() <= payload->proxy_id) {
        c->amo_map.resize(payload->proxy_id + 1);
    }

    // ...and if we've seen this seq_req_id
    if (c->amo_map[payload->proxy_id].size() <= payload->seq_req_id) {
        c->amo_map[payload->proxy_id].resize(payload->seq_req_id + 1);
    }

    AmoMapElem *m = &c->amo_map[payload->proxy_id][payload->seq_req_id];

    if (m->seqnum == UINT64_MAX) {
        uint64_t assigned_seqnum = 
            sequence_number.fetch_add(
                    payload->batch_size) + payload->batch_size - 1;

        AmoMapElem *m = &c->amo_map[payload->proxy_id][payload->seq_req_id];

        m->seqnum = assigned_seqnum;
        m->batch_size = payload->batch_size; 

        response->seqnum = assigned_seqnum;
        response->retx = false;

        debug_print(DEBUG, "assigning new num %lu in map %lu req_id %lu\n",
                response->seqnum, m->seqnum, payload->seq_req_id);
    } else {
        response->seqnum = m->seqnum;
        response->batch_size = m->batch_size; 
        response->retx = true;

        debug_print(1, "at-most-once pid: %d bid %d returning seqnum %lu, "
                "batch_size %lu\n",
                payload->proxy_id, payload->seq_req_id, 
                m->seqnum, m->batch_size); 
    }

    c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
    c->stat_resp_tx_tot++;
}


void
heartbeat_handler(erpc::ReqHandle *req_handle, void *_context)
{
    // Ping back to proxy
    auto *c = static_cast<SeqContext *>(_context);

    c->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf, 1);
    c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
    c->stat_resp_tx_tot++;
}

void
seq_thread_func(size_t thread_id, erpc::Nexus *nexus, app_stats_t *app_stats)
{
    SeqContext c;
    c.thread_id = thread_id;
    c.nconnections = 0;
    c.app_stats = app_stats;

    erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c),
            static_cast<uint8_t>(thread_id),
            sm_handler, 0);
    c.rpc = &rpc;
    
    clock_gettime(CLOCK_REALTIME, &c.tput_t0);
    printf("Thread %lu beginning main loop...\n", thread_id);
    fflush(stdout);

    while (unlikely(force_quit == false)) {
        c.rpc->run_event_loop(1000);
        c.print_stats();
    }
//    erpc::rt_assert(false, "ARTIFICIAL FAILURE! This if fine, force_quit is true.");
}


void
launch_threads(size_t nthreads, erpc::Nexus *nexus, 
        app_stats_t *app_stats, std::vector<std::thread> *threads)
{
    // Spin up the requisite number of sequencer threads
    for (size_t i = 0; i < nthreads; i++) {
        printf("Launching thread %zu\n", i);
        fflush(stdout);
        (*threads)[i] = std::thread(seq_thread_func, i, nexus, app_stats);
        erpc::bind_to_core((*threads)[i], numa_node, i);
    }

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

void
all_other_rpcs_handler(erpc::ReqHandle *req_handle __attribute__((unused)), 
        void *) {

    LOG_INFO("Sequencer received an unexpected request of type %u\n\trequests start at %u\n",
           req_handle->get_req_msgbuf()->get_req_type(),
           static_cast<uint8_t>(ReqType::kGetBitmap));
}

int
main (int argc, char **argv)
{
    int ret = 0;

	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);
    signal(SIGSEGV, signal_handler);

    // Parse command line args
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    erpc::rt_assert((FLAGS_other_ips.length() > 0) ^ (FLAGS_am_backup == false), 
            "Must include other IPs if this is a backup");
    erpc::rt_assert(FLAGS_nleaders > 0, 
            "Must include number of proxy groups!");
    boost::split(other_ip_list, FLAGS_other_ips, boost::is_any_of(","));
    printf("Other ips: %s\n", FLAGS_other_ips.c_str());

    // Count on a session per proxy; connections will only ever be one-way
    // (i.e., sequencer will only ever be "client" or "server"
    size_t nproxies = other_ip_list.size();
    const size_t num_sessions = nproxies * N_SEQTHREADS;
    erpc::rt_assert(num_sessions * erpc::kSessionCredits <=
                            erpc::Transport::kNumRxRingEntries,
          "Too few ring buffers");

    // Create a Nexus object (1 per NUMA node)
    std::string uri = FLAGS_my_ip + ":31850";
    printf("Creating nexus at %s\n", uri.c_str());
    fflush(stdout);
    erpc::Nexus nexus(uri, numa_node, 0);

    
    // Register handlers for sequencer's normal requests: 
    nexus.register_req_func(static_cast<uint8_t>(ReqType::kGetSeqNum), 
            seqnumreq_handler);
    nexus.register_req_func(static_cast<uint8_t>(ReqType::kHeartbeat), 
            heartbeat_handler); 
    nexus.register_req_func(static_cast<uint8_t>(ReqType::kInitiateRecovery), 
            recovery_handler);

    for (uint8_t i = 0; i <= 15; i++) {
        if (i != static_cast<uint8_t>(ReqType::kGetSeqNum) &&
            i != static_cast<uint8_t>(ReqType::kHeartbeat) &&
            i != static_cast<uint8_t>(ReqType::kInitiateRecovery))
            nexus.register_req_func(i, all_other_rpcs_handler);
    }
    
    // File for writing outputs
    char fname[100];
    sprintf(fname, "%s/sequencer-0.log2", FLAGS_out_dir.c_str());
    fp = fopen(fname, "w+");
    assert(fp != NULL);

    size_t nthreads = N_SEQTHREADS;
    std::vector<std::thread> threads(nthreads);
    auto *app_stats = new app_stats_t[nthreads];

    launch_threads(nthreads, &nexus, app_stats, &threads);
    
    // If I'm the backup, start with waiting for recovery requests
    if (FLAGS_am_backup) {
        run_recovery_loop(&nexus, FLAGS_nleaders);
        printf("Recovery complete!\n");
        fflush(stdout);
    }

    for (auto &thread : threads) thread.join();
    delete[] app_stats;

    fclose(fp);
	printf("Bye...\n");
	return ret;
}

