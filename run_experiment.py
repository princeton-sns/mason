import os
import shutil
import re
import sys
import itertools
import argparse
import shlex
import subprocess
import time
import atexit
import signal
import socket
import fcntl
import struct
import multiprocessing

client_warmup = 4

class Experiment:
    def __init__(self, args):
        self.whoami = args.whoami
        self.expduration = args.expduration

        self.nsequence_spaces = args.nsequence_spaces

        # client stuff
        self.nclient_threads = args.nclient_threads
        self.nclients = args.nclients

        # Create dictionaries for each machine class
        # Keyed by machine name (e.g., client-0); value is a dictionary
        # with keys 'machineid' (Emulab machine ID) and 'mac'
        (self.sequencers, self.available_proxies,
            self.available_clients) = self.parse_machine_info()

        self.no_gc = args.no_gc
        self.batch_timeout = args.batch_timeout

        self.nsequencers = len(self.sequencers)
        self.nclient_machines = len(self.available_clients)

        self.nbackend_servers = args.nbackend_servers

        # proxy stuff
        self.nproxy_threads = args.nproxy_threads
        self.nproxy_leaders = args.nproxy_leaders

        self.max_log_size = args.max_log_size

        if args.nproxies is None:
            self.nproxies = len(self.available_proxies)
        else:
            self.nproxies = args.nproxies
            if self.nproxies > len(self.available_proxies):
                print("Desired # of proxies is more than the number of " +
                      "available proxy machines! Exiting...")
                exit(1)
        self.proxy_list = sorted(self.available_proxies.keys())[:self.nproxies]

        # client stuff
        self.client_concurrency = args.client_concurrency
        if args.nclients is None:
            self.nclients = len(self.available_clients)
        else:
            if self.nclients > len(self.available_clients):
                print("Desired # of clients is more than the number of " +
                        "available client machines! Exiting...")
                exit(1)
        self.client_list = sorted(self.available_clients.keys())[:self.nclients]

        self.home = os.getcwd()

        self.numactl_string = """sudo -E env LD_LIBRARY_PATH=$LD_LIBRARY_PATH \\
                numactl --cpunodebind=0 --membind=0"""

        # killing stuff
        self.time_to_kill_sequencer = args.kill_sequencer
        self.time_to_kill_leader = args.kill_leader
        self.only_kill_zombies = args.only_kill_zombies

        sequencer_list = list(self.sequencers.keys())
        self.primary_sequencer = self.sequencers[sequencer_list[0]]
        if len(self.sequencers) > 1:
            self.backup_sequencer = self.sequencers[sequencer_list[1]]
        else:
            self.backup_sequencer = {'ctrl_ip': ""}

        # adding an option to just kill_zombies
        if self.only_kill_zombies == 1:
            self.kill_zombies()
            exit(0)

        timestamp = int(time.time())

        exp_outdir = args.outdir

        # tmp dir will stage data until the experiment is over (easier to monitor)
        self.tmpdir = os.path.join(self.home, "results", exp_outdir)

        shutil.rmtree(self.tmpdir, ignore_errors=True)
        try:
            os.mkdir(self.tmpdir)
        except:
            pass

        # ...then data will be copied here:
        self.outdir = os.path.join(self.home, "results", "results-%d" %
                                   timestamp)

        # Call this before the experiment runs and at exit
        self.kill_zombies()
        atexit.register(self.kill_zombies)

        # Dump all the class variables (experiment settings) to file
        with open("%s/config.txt" % self.tmpdir, 'w') as f:
            for var, value in self.__dict__.items():
                f.write("%s: %s\n" % (var, value))

    def launch_machines(self, machine_dicts):
        # For sequencers
        subprocesses = []
        for machine_name, machine_dict in machine_dicts.items():
            p = self.launch_machine(machine_dict)
            subprocesses.append(p)

        atexit.register(self.kill_processes, subprocesses)

        return subprocesses


    def launch_proxies(self):
        subprocesses = []

        my_raft_id = 0
        rep_ips = ['','','']

        proxy_id_start = -experiment.nproxy_threads

        next_neighbor0 = {'ctrl_ip': ''}
        next_neighbor1 = {'ctrl_ip': ''}
        next_neighbor2 = {'ctrl_ip': ''}
        for i, proxy in enumerate(self.proxy_list):
            me = self.available_proxies[proxy]

            if my_raft_id == 0:
                # so that it starts at 0
                proxy_id_start += experiment.nproxy_threads

                rep_ips[0] = me['ctrl_ip']
                rep_ips[1] = self.available_proxies[self.proxy_list[(i + 1) % len(self.proxy_list)]]['ctrl_ip']
                rep_ips[2] = self.available_proxies[self.proxy_list[(i + 2) % len(self.proxy_list)]]['ctrl_ip']

                # for now next neigher is the leader of the next group!, this will need to be changed for failover
                # we need to speak with the leader of the next group which may change!
                if experiment.nproxies > 3:
                    next_neighbor0 = self.available_proxies[self.proxy_list[(i + 3) % len(self.proxy_list)]]
                    next_neighbor1 = self.available_proxies[self.proxy_list[(i + 4) % len(self.proxy_list)]]
                    next_neighbor2 = self.available_proxies[self.proxy_list[(i + 5) % len(self.proxy_list)]]

            p = self.launch_proxy(i, me, next_neighbor0, next_neighbor1, next_neighbor2, my_raft_id, (my_raft_id+1) % 3, (my_raft_id+2) % 3,
                                  rep_ips[(my_raft_id+1) % 3], rep_ips[(my_raft_id+2) % 3], proxy_id_start)
            subprocesses.append(p)


            my_raft_id = (my_raft_id + 1) % 3

        atexit.register(self.kill_processes, subprocesses)

        return subprocesses


    def launch_clients(self):
        subprocesses = []

        cnt = 0
        rep_ips = ['','','']
        map = {}
        proxy_id_start = 0
        nproxies = len(self.available_proxies)
        for j, client in enumerate(self.client_list):
            proxy_threadid = 0  # dummy var
            proxy_idx = j % nproxies

            rep_ips[0] = self.available_proxies[self.proxy_list[(cnt) % len(self.proxy_list)]]['ctrl_ip']
            rep_ips[1] = self.available_proxies[self.proxy_list[(cnt + 1) % len(self.proxy_list)]]['ctrl_ip']
            rep_ips[2] = self.available_proxies[self.proxy_list[(cnt + 2) % len(self.proxy_list)]]['ctrl_ip']

            # this is so that clients are given to proxy groups (all proxy groups in proxy threads are on the same machine)
            # in round-robin order.
            cnt += 3

            if rep_ips[0] in map:
                map[rep_ips[0]] += 1
            else:
                map[rep_ips[0]] = 1

            if rep_ips[1] in map:
                map[rep_ips[1]] += 1
            else:
                map[rep_ips[1]] = 1

            if rep_ips[2] in map:
                map[rep_ips[2]] += 1
            else:
                map[rep_ips[2]] = 1

            client = self.available_clients[self.client_list[j]]

            # there better be a multiple of 3 proxies in the replicated setting
            assert(self.nproxies%3 == 0)

            p = self.launch_client(client, rep_ips[0], rep_ips[1], rep_ips[2], proxy_threadid, proxy_id_start)

            # next group starts at where this one started plus threads per proxy machine, but want to wrap around by total threads
            proxy_id_start = (proxy_id_start + self.nproxy_threads) % (self.nproxy_threads * (len(self.proxy_list)/3))

            subprocesses.append(p)

        atexit.register(self.kill_processes, subprocesses)

        for k in map:
            print("proxy_ip %s has %d clients" % (k, map[k]))

        return subprocesses


    def launch_machine(self, machine):
        # For sequencers
        machineid = machine['machineid']
        machine_name = machine['name']
        machine_type = machine_name.split("-")[0]
        machine_ip = machine['ctrl_ip']

        ssh = "ssh -p 22 %s@%s.emulab.net -o StrictHostKeyChecking=no" % (self.whoami,
                                              machineid)

        cmd = ("cd %s;" % os.path.join(os.getcwd(), machine_type) +
                " %s" % self.numactl_string + 
               " ./%s" % machine_type + 
               " --my_ip %s" % machine_ip)

        if machine_type == 'sequencer':
            cmd += " --nleaders %d" % self.nproxy_leaders
            cmd += " --nsequence_spaces %d" % self.nsequence_spaces
        if machine_name.split("-")[1] != "0":
                proxy_ips = ",".join([self.available_proxies[p]["ctrl_ip"] 
                        for p in self.proxy_list])
                cmd += " --other_ips %s" % proxy_ips
                cmd += " --am_backup"
                cmd += " --out_dir %s" % self.tmpdir

        # Dump program output to file to make sure things don't go wrong
        cmd += " &> %s/%s.log" % (self.tmpdir, machine_name)
        cmd = "%s '%s'" % (ssh, cmd)
        print("Command: %s\n" % cmd)
        p = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
        return p


    def launch_client(self, client, proxy_ip_0, proxy_ip_1, proxy_ip_2, proxy_threadid, proxy_id):
        client_name = client['name']
        machineid = client['machineid']
        ctrl_ip = client['ctrl_ip']
        client_dir = os.path.join(self.home, 'client')


        ssh = ("ssh -p 22 %s@%s.emulab.net -o StrictHostKeyChecking=no" % (self.whoami, machineid))
        cmd = ("cd %s;" % client_dir +
                " %s" % self.numactl_string +
                " ./client" +
                " --my_ip %s" % ctrl_ip +
                " --nthreads %d" % self.nclient_threads + 
                " --concurrency %d" % self.client_concurrency + 
                " --nproxy_leaders %d" % self.nproxy_leaders + 
                " --nproxy_threads %d" % self.nproxy_threads +
                " --proxy_threadid %d" % proxy_threadid + 
                " --expduration %d" % self.expduration + 
                " --proxy_ip_0 %s" % proxy_ip_0 +
                " --proxy_ip_1 %s" % proxy_ip_1 +
                " --proxy_ip_2 %s" % proxy_ip_2 +
                " --proxy_id %d" % proxy_id +
                " --out_dir '%s'" % experiment.tmpdir +
                " --results_file %s.dat" % client_name +
                " --nsequence_spaces %d" % self.nsequence_spaces)

        # Dump program output to file to make sure things don't go wrong
        cmd += " &> %s/%s.log" % (self.tmpdir, client_name)

        cmd = "%s '%s'" % (ssh, cmd)
        print("Command: %s" % cmd)
        p = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
        return p


    def launch_proxy (self, proxy_id, this_proxy, next_proxy0, next_proxy1, next_proxy2,
                      my_raft_id, replica_1_raft_id, replica_2_raft_id, replica_1_ip, replica_2_ip, proxy_id_start):
        machineid = this_proxy['machineid']
        proxy_name = this_proxy['name']
        client_ip = self.available_clients[self.client_list[0]]['ctrl_ip']

        ssh = "ssh -p 22 %s@%s.emulab.net -o StrictHostKeyChecking=no" % (self.whoami,
                                              this_proxy['machineid'])

        assert(self.nproxy_threads > 0)
        cmd = ("cd %s;" % os.path.join(os.getcwd(), "proxy") + 
                " %s" % self.numactl_string + 
                # " valgrind -v -v -v" +
                " ./proxy" + 
                " --my_ip %s" % this_proxy['ctrl_ip'] + 
                " --seq_ip %s" % self.primary_sequencer['ctrl_ip'] +
                " --nthreads %d" % self.nproxy_threads +
                " --batch_to %d" % self.batch_timeout +
                " --replica_1_ip %s" % replica_1_ip +
                " --replica_2_ip %s" % replica_2_ip +
                " --my_raft_id %d" % my_raft_id +
                " --replica_1_raft_id %d" % replica_1_raft_id +
                " --replica_2_raft_id %d" % replica_2_raft_id +
                " --proxy_id_start %d" % proxy_id_start +
                " --client_ip %s" % client_ip +
                " --max_log_size %d" % self.max_log_size +
                " --nsequence_spaces %d" % self.nsequence_spaces
                )

        print("self.nsequence_spaces %d args.nsequence_spaces %d"%
              (self.nsequence_spaces, args.nsequence_spaces))
        if experiment.backup_sequencer['ctrl_ip'] != '':
            cmd += " --backupseq_ip %s" % self.backup_sequencer['ctrl_ip']

        if next_proxy0['ctrl_ip'] != '':
            cmd += " --nextproxy0_ip %s" % next_proxy0['ctrl_ip']
        if next_proxy1['ctrl_ip'] != '':
            cmd += " --nextproxy1_ip %s" % next_proxy1['ctrl_ip']
        if next_proxy2['ctrl_ip'] != '':
            cmd += " --nextproxy2_ip %s" % next_proxy2['ctrl_ip']


        if proxy_id is 0 and not self.no_gc:
            # This proxy is the garbage collection leader
            cmd += " --am_gc_leader"
        if self.no_gc:
            cmd += " --no_gc"

        # Dump program output to file to make sure things don't go wrong
        cmd += " &> %s/%s.log" % (self.tmpdir, proxy_name)
        cmd = "%s '%s'" % (ssh, cmd)
        print("Command: %s\n" % cmd)

        p = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
        return p


    def parse_machine_info(self):
        sequencers = {}
        proxies = {}
        clients = {}

        f = open('setup/machine_info.txt', 'r')
        for line in f.readlines():
            fields = line.strip().split(',')

            if 'sequencer' in fields[0]:
                sequencers[fields[0]] = {
                    'name': fields[0],
                    'machineid': fields[1],
                    'mac': fields[3],
                    'ctrl_ip': fields[5]
                }
            elif 'proxy' in fields[0]:
                proxies[fields[0]] = {
                    'name': fields[0],
                    'machineid': fields[1],
                    'mac': fields[3],
                    'ctrl_ip': fields[5]
                }
            elif 'client' in fields[0]:
                clients[fields[0]] = {
                    'name': fields[0],
                    'machineid': fields[1],
                    'mac': fields[3],
                    'ctrl_ip': fields[5]
                }
        f.close()
        return sequencers, proxies, clients


    def get_ip_address(self, ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,
            struct.pack('256s', ifname[:15])
        )[20:24])


    def kill_processes(self, subprocesses):
        print("Triggered kill_processes!")
        for p in subprocesses:
            try:
                p.kill()
            except:
                pass


    def kill_zombies(self):
        print("Killing zombies...")
        processes = []
        for machine_name, machine_info in itertools.chain(
                self.available_proxies.items(),
                self.sequencers.items(),
                self.available_clients.items()):

            # if 'proxy-0' in machine_name:
            #     continue

            print("Killing %s..." % machine_name)
            ssh = "ssh -p 22 %s@%s.emulab.net -o StrictHostKeyChecking=no" % (self.whoami,
                                                  machine_info['machineid'])
            if 'sequencer-' in machine_name:
                kill_keyword = '[s]equencer'
            elif 'proxy-' in machine_name:
                kill_keyword = '[p]roxy'
            elif 'client-' in machine_name:
                kill_keyword = '[c]lient'
            else:
                print("Couldn't identify this machine to kill...")
            cmd = "%s 'sudo pkill  -f -9 \"./%s\";" \
                  "sudo rm -f /usr/local/*snapshot*; " \
                  "sudo rm -f /usr/local/*bitmap*; '" % (ssh, kill_keyword)
            # "sudo find /usr/local/ -name '*snapshot_rand_1*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_2*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_3*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_4*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_5*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_6*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_7*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_8*' | sudo xargs rm -f; " \
            # "sudo find /usr/local/ -name '*snapshot_rand_9*' | sudo xargs rm -f; "

            # cmd = "%s 'sudo pkill  -f -9 \"./%s\"'" \
            #       "sudo rm -f /usr/local/*snapshot*; " \
            #       "sudo rm -f /usr/local/*bitmap*; '" % (ssh, kill_keyword)
            # # "sudo find /usr/local/ -name '*snapshot_rand_1*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_2*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_3*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_4*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_5*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_6*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_7*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_8*' | sudo xargs rm -f; " \
            # # "sudo find /usr/local/ -name '*snapshot_rand_9*' | sudo xargs rm -f; "% (ssh, kill_keyword)
            # # print(cmd)
            cmd = shlex.split(cmd)

            p = subprocess.Popen(cmd) 
            processes.append(p)

        for p in processes:
            p.wait()

    def kill_sequencer(self):
        time_to_kill = self.time_to_kill_sequencer + client_warmup

        if self.time_to_kill_sequencer < 1:
            return

        print("killing the sequencer for recovery... ")
        time.sleep(time_to_kill)

        # kill the sequencer now
        ssh = "ssh -p 22 %s@%s.emulab.net -o StrictHostKeyChecking=no" % (self.whoami, self.primary_sequencer['machineid'])
        kill_keyword = '\./[s]equencer'
        cmd = "%s 'sudo pkill -f -9 \"%s\"'" % (ssh, kill_keyword)
        print(cmd)
        cmd = shlex.split(cmd)
        p = subprocess.Popen(cmd)
        p.wait()
        print("Sequencer killed")
        

    def kill_leader(self):
        time_to_kill = self.time_to_kill_leader
        if time_to_kill <= 0:
            print("Not killing leader")
            return
        print("going to kill a leader for failover...")

        time.sleep(time_to_kill + client_warmup) # clients take 5 seconds to start the exp

        print("killing a leader for failover... %s" % self.available_proxies[self.proxy_list[0]])
        # kill a leader now
        ssh = "ssh -p 22 %s@%s.emulab.net -o StrictHostKeyChecking=no" % (self.whoami, self.available_proxies[self.proxy_list[0]]['machineid'])
        kill_keyword = 'proxy'
        cmd = "%s 'sudo pkill -f -9 \"./%s\"'" % (ssh, kill_keyword)
        cmd = shlex.split(cmd)
        p = subprocess.Popen(cmd)
        p.wait()
        print("Leader killed")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run multiple clients.',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # whoami isn't actually necessary...
    parser.add_argument('whoami',
                        help=('Emulab username'))
    parser.add_argument('--outdir',
                        help=('Output directory'),
                        type=str,
                        default='results-%d'%int(time.time()))
    parser.add_argument('--nclient_threads',
                        help=('Number of threads to run on each client machine'),
                        type=int,
                        default=16)
    parser.add_argument('--no_gc',
                        help=('Don\'t do garbage collection.'),
                        action='store_true')
    parser.add_argument('--batch_timeout',
                        help=('Batch timeout in microseconds.'),
                        type=int,
                        default=20)
    parser.add_argument('--kill_sequencer',
                        help=('The time (in seconds) into the experiment to kill the primary sequencer ' +
                              'to initiate recovery. -1 to not kill.'),
                        type=int,
                        default=-1)
    parser.add_argument('--kill_leader',
                        help=('The time (in seconds) into the experiment to kill a leader ' +
                              'to initiate leader failover. -1 to not kill.'),
                        type=int,
                        default=-1)
    parser.add_argument('--only_kill_zombies',
                        help=('If this run is to only kill zombies.'),
                        action='store_true')
    parser.add_argument('--expduration',
                        help=('How long experiment should run in seconds. Doesn\'t include ' +
                            'warmup and cooldown periods.'),
                        type=int,
                        default=60)
    parser.add_argument('--nproxies',
                        help=('Number of proxy machines to use in this experiment. ' +
                        'Must be less than the number of available proxy machines.'),
                        type=int,
                        default=3)
    parser.add_argument('--nproxy_threads',
                        help=('Number of proxy threads to run per machine.'),
                        type=int,
                        default=8)
    parser.add_argument('--nproxy_leaders',
                        help=('Total number of leaders in the system.'),
                        type=int,
                        default=8)
    parser.add_argument('--nbackend_servers',
                        help=('Number of backend servers for proxies to connect to.'),
                        type=int,
                        default=0)
    parser.add_argument('--nclients',
                        help=('Number of clients to run, clients are assigned to proxies in ' +
                            'round-robin fashion.'),
                        type=int,
                        default=1)
    parser.add_argument('--client_concurrency',
                        help=('Number of outstanding requests a client can have at once.'),
                        type=int,
                        default=32)
    parser.add_argument('--max_log_size',
                        help=('Specifies when to do compaction. Default: 100000'),
                        type=int,
                        default=500000)
    parser.add_argument('--nsequence_spaces',
                        help=('Total number of sequence spaces.'),
                        type=int,
                        default=1)

    args = parser.parse_args()
    experiment = Experiment(args)

    sequencer_procs = experiment.launch_machines(experiment.sequencers)
    proxy_procs = experiment.launch_proxies()

    # Sleep to make sure the other processes are up
    time.sleep(2)
    client_procs = experiment.launch_clients()

    # start thread to time the killing of sequencer
    kill_sequencer_process = multiprocessing.Process(
            target=experiment.kill_sequencer)

    kill_leader_process = multiprocessing.Process(
            target=experiment.kill_leader)

    kill_sequencer_process.start()
    kill_leader_process.start()

    for p in client_procs:
        p.wait()
    try:
        os.remove(experiment.tmpdir + "/../latest")
    except:
        pass
    os.symlink(experiment.tmpdir, experiment.tmpdir + "/../latest")
    print("Experiment over, data in %s..." % experiment.tmpdir)