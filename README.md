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

The user should create an Emulab account and join the project "Mason". The user must set their account's default shell to "bash" under account settings.
The user must edit `~/.shrc` to contain `LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/proj/Mason/mason/raft/`.

Under this project the user can create an experiment using the profile "mason" which contains a disk image that can compile and run this code.
Proxies should be a multiple of 3, servers a multiple of 2 for Corfu and 3 for ZooKeeper.
The default machine numbers are a minimal setup for ZK-Mason.

Once an experiment is swapped in `cd` into the `setup/` directory. If in the project "Mason" the user can `cd` to `/proj/Mason/mason/` which contains this repository. Copy the experiment "List View" from Emulab into `setup/machine_list.txt`. The file should look something like this:

    sequencer-0	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net		
    sequencer-1	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net		
    proxy-0	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net		
    proxy-1	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net		
    proxy-2	    pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net		
    client-0	pc###	d430	ready	n/a	project/image ssh -p 22 username@pc###.emulab.net		

Then run `python3 parse_machine_list.py [your Emulab username]`. This script parses the list of machines from `machine_list.txt`, sets up hugepages and DPDK, and outputs `machine_info.txt`. Ensure setup completed successfully with status 0.

# How to run an experiment
Before running experiments ensure the `results/` directory exists.

Setting `kSessionCredits` and `kSessionReqWindow` in `eRPC/src/sm_types.h` properly is important for performance. 
`kSessionCredits` is limited by the number of connections for each component; so it depends on the application and the component.
For the best performance on Emulab d430s with DPDK use 128 for all clients except Corfu clients which use 32 and when running recovery which should use 64. Proxies always use 16. Master branch sequencer uses 8. Corfu's sequencer and servers use 8. CorfuMason/ZK-Mason uses 4 for servers and the sequencer. Note that clients must have `--client_concurrency <= kSessionCredits` otherwise proxies may deadlock waiting for client requests to ensure client-determined order.

To build each component: set its `kSessionCredits` correctly, cd into the component's directory and run `make`.

Run experiments with `python3 run_experiment.py [your Emulab username]`.
Default values are set in each branch to get the highest throughput at reasonable latency on the smallest scale experiment in the paper. When scale is increased `kSessionCredits` must be reduced (as above) to allow for more connections.

# How to parse data
Run `bash parse_datfiles.sh results` to aggregate the throughput and show median client latencies. Output is `aggregrate-throughput 50 99 99.9 99.99 percentile`.