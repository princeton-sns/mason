The proxy component is the scalable layer in between clients and the rest of the
service. Proxies are replicated over Raft. The typically message pattern is: clients
send an operation to proxies, which request a multi-sequence number from the sequencer,
replicate to other proxies in its group, send the operation to the service's shards,
and reply to the client.

The flags to proxy are (flag, default, description):
```
nthreads,           0,      Number of worker threads to launch on this machine
proxy_id_start,     0,      Where proxy ids begin, 1 proxy group for each thread!
nleaders,           0,      Number of leaders to run on each worker thread
nfollowers,         0,      Number of followers to run on each worker thread
nclients,           0,      Max number of client threads that will be serviced on this machine
nservers,           0,      Max number of servers we will connect to with this machine
am_gc_leader,       false,  Whether this proxy initiates garbage collection
no_gc,              false,  Whether to do garbage collection

IP addresses
my_ip,              "",     IP address for this machine
nextproxy0_ip,      "",     IP address of next proxy in garbage collection ring
nextproxy1_ip,      "",     IP address of next proxy in garbage collection ring
nextproxy2_ip,      "",     IP address of next proxy in garbage collection ring
seq_ip,             "",     IP address of sequencer
backupseq_ip,       "",     IP address of backup sequencer
replica_1_ip,       "",     IP address of replica 1
replica_2_ip,       "",     IP address of replica 2
client_ip,          "",     IP address of client for recording noop seqnums

batch_to,           1000,   Batch timeout in us
my_raft_id,         0,      The unique Raft ID for this machine's proxy groups.
replica_1_raft_id,  0,      The unique Raft ID for this machine's first replica.
replica_2_raft_id,  0,      The unique Raft ID for this machine's second replica.
max_log_size,       10000,  The maximum size to keep proxy's log. Determines how often to snapshot.
nsequence_spaces,   5,      The number of sequence spaces we are running with.
```

The `rsmkeeper` and `zk-mason` branches additionally have:
```
zk_ips,             "",     ZooKeeper IP addresses // unused in rsmkeeper
client_ips,         "",     All of the clients IPs for watches
nclient_threads,    0,      number of client threads per machine
```

The `corfumason` branch has:
```
corfu_ips,          "",     Corfu IP addresses
```

`../run_experiment.py` uses different defaults; flags should only be modified through `../run_experiment.py`.

Three hardcoded flags relating to the proxy are: 
`INIT_N_BLOCKS` in `common.h` which controls the initial number of blocks in the bitmap that holds received sequence numbers, 
`BYTES_PER_BLOCK` in `common.h` which controls the size of the blocks, and 
`GC_TIMER_US` in `proxy.h` which controls how often the garbage collection leader proxy initiates garbage collection.

For all experiments other than ZK-Mason `getData` they should be left default. 
For ZK-Mason `getData` they should be set to the following before compiling components depending on the number of shards in the experiment:

    1 shard: 	INIT_N_BLOCKS: 1, BYTES_PER_BLOCK 65536,    GC_TIMER_US: 10000
    2 shards: 	INIT_N_BLOCKS: 8, BYTES_PER_BLOCK 65536*16, GC_TIMER_US: 10000
    4 shards: 	INIT_N_BLOCKS: 8, BYTES_PER_BLOCK 65536*16, GC_TIMER_US: 100000
    8 shards: 	INIT_N_BLOCKS: 1, BYTES_PER_BLOCK 65536,    GC_TIMER_US: 10000