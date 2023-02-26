This repo contains 5 branches:
- Master
    - Clients request multi-sequence numbers which are returned by proxies.
- Corfu
    - An implementation of the main Corfu protocol atop our sequencer. Clients get sequence numbers and interact with servers directly.
- CorfuMason
    - An implementation of CorfuMason which uses proxies for scalability and contiguity. Clients execute operations through proxies.
- RSMKeeper
    - An implementation of ZooKeeper over Raft. We modified the proxy/ code to execute ZooKeeper operations.
- ZK-Mason
    - A scalable implementation of ZooKeeper atop Mason.

...plus setup scripts and scripts for parsing results.

This repo also contains a modified version of [eRPC](https://github.com/erpc-io/eRPC) and uses code from [willemt/raft](https://github.com/willemt/raft) which is contained in the Emulab disk image.

# Setting up machines
These instructions are for Mason on Emulab with d430s running Ubuntu 18.04. These directions install and configure DPDK/hugepages for use with eRPC, and gather information from each machine to facilitate automated experiment-launching. 

On Emulab use the `mason` profile under the project `Mason`. 
This profile contains d430s connected with 10Gb NICs, with nodes: sequencer-0, sequencer-1, proxy-#, client-#, server-#. 
There is also a genilib script `emulab_genilib` in this repository you can use to create the same profile.
Proxies should be a multiple of 3, servers a multiple of 2 for Corfu and 3 for ZooKeeper.
The default machine numbers are a minimal setup for ZK-Mason.

Once an experiment is swapped in `ssh` into any node (e.g. proxy-0) and clone this repository; we recommend cloning into `/proj/your-project/` to avoid disk usage quota issues when running experiments. 

On Emulab cloning to this directory or your home directory should clone it to every node through Emulab's NFS.
Note: this did not work for one user and their solution was to modify `parse_machine_list.py` to clone the repo on each node: "Replace line 188 and 189 (https://github.com/masonj2022/mason/blob/24da3117d02270adbbd329873fb4514f605304fe/setup/parse_machine_list.py#L188) with `setup_cmd = ("cd ~; git clone https://github.com/masonj2022/mason; cd mason/setup; sudo bash dpdk_apt_setup.sh %s %s" % (machines[machine]['iface1'], machines[machine]['iface2']))` Alternatively, you can ssh into each machine and manually clone the repo before running the setup script."

`cd` into the `setup/` directory in this repo.
Copy the experiment "List View" from Emulab into `setup/machine_list.txt`. The file should look something like this:

    sequencer-0	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    sequencer-1	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    proxy-0	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    proxy-1	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    proxy-2	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    client-0	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net

Then run `python3 parse_machine_list.py [your Emulab username]`. This script parses the list of machines from `machine_list.txt`, sets up hugepages and DPDK, and outputs `machine_info.txt`. Ensure setup completed successfully with status 0.

When using the `parse_machine_list.py` script one user had an indexing issue which caused the an `ssh` command to be incorrect.
If you run into this issue their solution was to change change the 6 to a 7 on line 102: `ssh = " ".join(fields[6:]) + " -o StrictHostKeyChecking=no "` -> `ssh = " ".join(fields[7:]) + " -o StrictHostKeyChecking=no "`.

Note: some users experienced problems with `ssh` keys when using CloudLab. To solve these problems the user added an extra `ssh` key: "Add an extra ssh key to cloud lab (this is because the easiest way to setup ssh between cloud lab nodes in an experiment is to copy a private key onto one of the nodes); I created an additional/extra key so I can delete this key after the artifact evaluation for privacy/safety", `ssh`'ed into a node, and uploaded the private key "Upload your private ssh key onto the node and run the following commands: `chmod 600 /your/priv/key eval “$(ssh-agent -s)” ssh-add /your/priv/key"`.

# How to run an experiment
To make all components run `bash make_all.sh`.
Setting `kSessionCredits` and `kSessionReqWindow` in `eRPC/src/sm_types.h` properly is important for performance. 
`kSessionCredits` is limited by the number of connections for each component.
`SESSION_CREDITS` in `ltomake` is a compile time parameter to set `kSessionCredits`. It is set for the largest configuration in each branch.
If you modify `SESSION_CREDITS` for a component you need to `make clean` the component to rebuild the eRPC code.
You can clean and make all components with `bash make_all.sh --clean`.
<!-- For the best performance on Emulab d430s with DPDK use 128 for all clients except Corfu clients which use 32.
Proxies always use 16. Master branch sequencer uses 8. 
Corfu's sequencer and servers use 8. CorfuMason/ZK-Mason uses 4 for servers and the sequencer. -->
Note that if you are configuring `SESSION_CREDITS`, clients must have `--client_concurrency <= kSessionCredits` otherwise proxies may deadlock waiting for client requests to ensure client-determined order.

To build each component cd into the component's directory and run `make` or run `bash make_all.sh`.

Run experiments with `python3 run_experiment.py [your Emulab username]`.
Default values are set in each branch to get the highest throughput at reasonable latency on the smallest scale experiment in the paper.

For Figure 3 choose a sequence space count and to double throughput double `--nproxies` and `--nproxy_leaders` and double the load `--nclients`.

For Figure 4 double `--ncorfu_servers --nproxies --nproxy_leaders --nclients`.

For Figure 5 double `--nservers --nproxies --nproxy_leaders --nclients`. 

`--nclient_concurrency` may need to be varied to find the right throughput/latency tradeoff.

There are 3 hardcoded parameters for ZK-Mason getData experiments that neet to be set based on the number of ZK-Mason shards in the experiment: INIT_N_BLOCKS in common.h which controls the initial number of blocks in the bitmap that hols received sequence numbers, BYTES_PER_BLOCK in common.h which controls the size of the blocks, and GC_TIMER_US in proxy/proxy.h which controls how often the garbage collection leader proxy initiates garbage collection. They should be set to the following before compiling components:

    1 shard: 	INIT_N_BLOCKS: 1, BYTES_PER_BLOCK 65536,    GC_TIMER_US: 10000
    2 shards: 	INIT_N_BLOCKS: 8, BYTES_PER_BLOCK 65536*16, GC_TIMER_US: 10000
    4 shards: 	INIT_N_BLOCKS: 8, BYTES_PER_BLOCK 65536*16, GC_TIMER_US: 100000
    8 shards: 	INIT_N_BLOCKS: 1, BYTES_PER_BLOCK 65536,    GC_TIMER_US: 10000

For Figure 5 (recovery) set `#define PLOT_RECOVERY 1` in common.h when building the components. This flag makes proxies connect to clients to send them noops and clients to record received sequence numbers. Run `python3 run_experiment.py [your Emulab username] --client_concurrency 8 --nclient_threads 16 --expduration 30 --nproxies 6 --nclients 4 --nsequence_spaces 4 --kill_leader 6 --nproxy_threads 8 --nproxy_leaders 16 --kill_sequencer 16` which kills a proxy leader and the sequencer 10 and 20 seconds into the experiment, respectively, after waiting 4 seconds for warmup. Then `cd` to `recovery/` and run `bash create_recovery_plot.sh`. You may need to install the `numpy` and `pandas` python3 packages. `pip3 install [package]`.
# How to parse data
Run `bash parse_datfiles.sh results` to aggregate the throughput and show median client latencies. Output is `aggregrate-throughput 50 99 99.9 99.99 percentile`.