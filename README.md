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

Note: if you run into problems please check the [Troubleshooting](#troubleshooting) section at the bottom.

# Setting up machines
These instructions are for Mason on Emulab with d430s running Ubuntu 18.04. These directions install and configure DPDK/hugepages for use with eRPC, and gather information from each machine to facilitate automated experiment-launching. 

First, the user should change their default shell to bash: on Emulab/CloudLab top right "your-user-name" drop down
-> Manage Account -> Default Shell -> bash. It may take a few minutes for it to change.
All of our scripts assume bash is the default shell.

On Emulab use the `mason` profile under the project `Mason`. 
This profile contains d430s connected with 10Gb NICs, with nodes: sequencer-0, sequencer-1, proxy-#, client-#, server-#. 
There is also a genilib script `emulab_genilib` in this repository you can use to create the same profile.

More explicit instructions from a previous user which work for Emulab as well as CloudLab (all of our experiments were conducted over Emulab):

    You can find this profile on CloudLab as follows.
    * From the menu, choose "Experiments -> Start Experiment".
    * In the resulting screen, click "Change Profile".
    * In the dialog, go to the search box (upper left) and enter "mason".
    * Scroll down in the list if necessary, and choose the "Mason" profile.
    Then click the "Select Profile" button and proceed with the usual experiment-instantiation process.

<!-- Proxies should be a multiple of 3, servers a multiple of 2 for Corfu and 3 for ZooKeeper. -->
The default machine numbers are the largest setup for corfumason.

We recommend starting an experiment with the largest setup (corfumason) and then modifying `setup/machine_info.txt` to later change the configuration (described more below).

Once an experiment has started `ssh` into any node (e.g. proxy-0) and clone this repository; we recommend cloning into `/proj/your-project/` to avoid disk usage quota issues when running experiments. 

On Emulab cloning to this directory or your home directory should clone it to every node through Emulab's NFS. If this doesn't work see [Troubleshooting](#Troubleshooting).

`cd` into the `setup/` directory in this repo.
Copy the experiment "List View" from Emulab into `setup/machine_list.txt`. The file should look something like this:

    sequencer-0	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    sequencer-1	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    proxy-0	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    proxy-1	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    proxy-2	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net
    client-0	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net

Then run `python3 parse_machine_list.py [your Emulab username]`.
This script parses the list of machines from `machine_list.txt`, sets up hugepages and DPDK, and outputs `machine_info.txt`.
Ensure setup completed successfully with status 0.
If you run into an indexing issue or if there problems `ssh`'ing, for example many permission denied errors, see [Troubleshooting](#troubleshooting).

# How to run an experiment
To make all components run `bash make_all.sh`.
<!-- Setting `kSessionCredits` and `kSessionReqWindow` in `eRPC/src/sm_types.h` properly is important for performance. 
`kSessionCredits` is limited by the number of connections for each component.
`SESSION_CREDITS` in `ltomake` is a compile time parameter to set `kSessionCredits`. It is set for the largest configuration in each branch.
If you modify `SESSION_CREDITS` for a component you need to `make clean` the component to rebuild the eRPC code. -->
You can clean and make all components with `bash make_all.sh --clean`.
<!-- For the best performance on Emulab d430s with DPDK use 128 for all clients except Corfu clients which use 32.
Proxies always use 16. Master branch sequencer uses 8. 
Corfu's sequencer and servers use 8. CorfuMason/ZK-Mason uses 4 for servers and the sequencer.
Note that if you are configuring `SESSION_CREDITS`, clients must have `--client_concurrency <= kSessionCredits` otherwise proxies may deadlock waiting for client requests to ensure client-determined order.
-->

To build each component run `bash make_all.sh` or cd into the component's directory and run `make`.

Run experiments with `python3 run_experiment.py [your Emulab username]`.
Run `bash parse_datfiles.sh results` to aggregate the throughput and show median client latencies.
Output is `aggregrate-throughput 50 99 99.9 99.99 percentile`.
Default values are set in each branch to get the highest throughput at reasonable latency on the smallest scale experiment in the paper.

Each branch has the script `run_suite.sh`. This script will run enough experiments to recreate each figure. Though one trial each where in the paper we use 5 trials each and take the median throughput and median latencies. To run fewer experiments or on a smaller setup modify the loop parameters in `run_suite.sh`.

For the largest experiments some clients non-deterministically fail on startup. `run_suite.sh` will detect this, delete the corresponding `results-` dir and restart the experiment. If the user notices the failure during a run `^C` will terminate the run and `run_suite.sh` will still detect, delete, and restart the run.

We include the largest configuration for each figure below. To assign nodes to a different role (proxy/server/client), after setting up the machines with DPDK, you can change the name manually in `machine_info.txt` and then use `run_experiment.py` (or `run_suite.sh`).
To change a machine type, for example from a proxy to a server, open `setup/machine_info.txt`
choose which proxy to change and replace `proxy-#` to `server-#`. 
The main experiment script `run_experiment.py` will use the names to determine the type of machine.
For example, after running the largest corfumason experiment, you can change some proxies to servers to make 24 servers.

Note if you are running with smaller configurations than the largest configurations we give below, then proxies should be a multiple of 3, servers a multiple of 2 for Corfu and 3 for ZooKeeper.

## Figure 3
Largest configurations: 
    
    master:     2 sequencers   48 proxies   16 clients   0  servers   (66  total).

The easiest way to run experiments is for the user to use `run_suite.sh` which will iterate through 1, 2, 4, 8, 16 proxies and 1, 2, 4, 8 sequence spaces increasing client load for each pair.
The user can also choose a sequence space count and to double throughput double `--nproxies` and `--nproxy_leaders` and double the load `--nclients`.
For example, after cloning the repo and setting up the machines as above a user can run the following to create on trial of each data point:

    git checkout master
    bash make_all.sh --clean
    bash run_suite.sh <username>

## Figure 4 
Largest configurations: 

    corfu:      2 sequencers   0  proxies   16 clients   8  servers   (26  total)
    corfumason: 2 sequencers   72 proxies   24 clients   8  servers   (106 total).

Double `--ncorfu_servers --nproxies --nproxy_leaders --nclients`. Or:

    git checkout corfu
    bash make_all.sh --clean
    bash run_suite.sh <username> --appends
    bash run_suite.sh <username> --reads

or for CorfuMason.

    git checkout corfumason
    bash make_all.sh --clean
    bash run_suite.sh <username> --appends
    bash run_suite.sh <username> --reads

The default option of `--corfu_replication_factor` 2 should be used. This implies `--ncorfu_servers/--corfu_replication_factor` is the number of corfu shards. CorfuMason uses 6 replicated proxies per shard (the value used in `run_suite.sh`.)

## Figure 5 
Largest configurations:

    rsmkeeper:  0 sequencers   3  proxies   1  client    0  servers   (4   total)
    zk-mason:   2 sequencers   48 proxies   16 clients   24 servers   (90  total)

Double `--nservers --nproxies --nproxy_leaders --nclients`. 

`--client_concurrency` may need to be varied to find the right throughput/latency tradeoff.

Or run a full suite for RSMKeeper:

    git checkout rsmkeeper
    bash make_all.sh --clean
    bash run_suite.sh <username> --setDatas
    bash run_suite.sh <username> --getDatas

For ZK-Mason:

    git checkout zk-mason
    bash make_all.sh --clean
    bash run_suite.sh <username> --setDatas

The ratio of proxies to shards in `run_suite.sh` (2 replicated proxies per ZK shard) should be used if running manually with `run_experiment.py`.

For ZK-Mason --getDatas experiments there are hardcoded parameters which need to be modified and all components then rebuilt depending on the number of shards in the experiment.
The user, after building with the correct parameters (below) should modify the outer loop in `run_suite.sh` to only run for the shard configuration for which the user built.

The 3 hardcoded parameters for ZK-Mason getData experiments that need to be set based on the number of ZK-Mason shards in the experiment: INIT_N_BLOCKS in common.h which controls the initial number of blocks in the bitmap that hols received sequence numbers, BYTES_PER_BLOCK in common.h which controls the size of the blocks, and GC_TIMER_US in proxy/proxy.h which controls how often the garbage collection leader proxy initiates garbage collection. They should be set to the following before compiling components:

    1 shard: 	INIT_N_BLOCKS: 1, BYTES_PER_BLOCK 65536,    GC_TIMER_US: 10000
    2 shards: 	INIT_N_BLOCKS: 8, BYTES_PER_BLOCK 65536*16, GC_TIMER_US: 10000
    4 shards: 	INIT_N_BLOCKS: 8, BYTES_PER_BLOCK 65536*16, GC_TIMER_US: 100000
    8 shards: 	INIT_N_BLOCKS: 1, BYTES_PER_BLOCK 65536,    GC_TIMER_US: 10000

## Figure 6 (recovery)
Before building components set `#define PLOT_RECOVERY 1` in common.h and uncomment line 19 in `ltomake` `SESSION_CREDITS=64` so that client build with less `SESSION_CREDITS` to allow for proxy-to-client eRPC connections.
`SESSION_CREDITS` in `ltomake` is a compile time parameter to set `kSessionCredits`, a hardcoded value in eRPC/sm_types.h.
It limits the number of connections on a machine.
If you modify `SESSION_CREDITS` for a component you need to `make clean` the component to rebuild the eRPC code.
Note that when you are configuring `SESSION_CREDITS`, clients must have `--client_concurrency <= kSessionCredits` otherwise proxies may deadlock waiting for client requests to ensure client-determined order

The flag `PLOT_RECOVERY` makes proxies connect to clients to send them noops and clients to record received sequence numbers.
Then rebuild all components `bash make_all.sh --clean`.
Run:

    python3 run_experiment.py <your Emulab username> --client_concurrency 8 --nclient_threads 16 --expduration 30 --nproxies 6 --nclients 4 --nsequence_spaces 4 --kill_leader 6 --nproxy_threads 8 --nproxy_leaders 16 --kill_sequencer 16;
    cd recovery;
    bash create_recovery_plot.sh;

which kills a proxy leader and the sequencer 10 and 20 seconds into the experiment, respectively, after waiting 4 seconds for warmup. Then `cd`s to `recovery/` and runs `bash create_recovery_plot.sh`. You may need to install the `numpy` and `pandas` python3 packages. `pip3 install <package>`. This will output `recovery.pdf`.
# How to parse data
Run `bash parse_datfiles.sh results` to aggregate the throughput and show median client latencies. Output is `aggregrate-throughput 50 99 99.9 99.99 percentile`.

# Troubleshooting
When using the `parse_machine_list.py` script, one user had an indexing issue which caused the an `ssh` command to be incorrect.
If you run into this issue their solution was to change change the 6 to a 7 on line 102: `ssh = " ".join(fields[6:]) + " -o StrictHostKeyChecking=no "` -> `ssh = " ".join(fields[7:]) + " -o StrictHostKeyChecking=no "`.

Note: some users experienced problems with `ssh` keys when using CloudLab. To solve these problems the user added an extra `ssh` key: "Add an [extra ssh key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) to cloud lab (this is because the easiest way to setup ssh between cloud lab nodes in an experiment is to copy a private key onto one of the nodes); I created an additional/extra key so I can delete this key after the artifact evaluation for privacy/safety", `ssh`'ed into a node, and uploaded the private key "Upload your private ssh key onto the node and run the following commands: "
    
    chmod 600 /your/priv/key 
    eval “$(ssh-agent -s)” 
    ssh-add /your/priv/key

Another solution is to insert `-i <privkey>` to all ssh paths specified in `parse_machine_list.py` and `run_experiment.py`.

One user had a problem with Emulab/Cloudlab not syncing the cloned directory over NFS and their solution was to modify `parse_machine_list.py` to clone the repo on each node: "Replace line 188 and 189 (ht<span>tps://</span>github.com/masonj2022/mason/blob/24da3117d02270adbbd329873fb4514f605304fe/setup/parse_machine_list.py#L188) with `setup_cmd = ("cd ~; git clone https://github.com/masonj2022/mason; cd mason/setup; sudo bash dpdk_apt_setup.sh %s %s" % (machines[machine]['iface1'], machines[machine]['iface2']))` Alternatively, you can ssh into each machine and manually clone the repo before running the setup script."