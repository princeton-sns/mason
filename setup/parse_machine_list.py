# Takes as input the copy-pasted list of machines in the "List View" tab of
# Emulab experiment page (must be titled "machine_list.txt" in top-level dir)
# and sets up Emulab on proxies, sequencer, and DT(s). 
# Creates a file with each machine's ID, Node#, interface (only relevant for
# clients since it will be brought down for DPDK machines), and MAC address.
import os
import subprocess
import shlex
import argparse
import platform

which_ubuntu = int(platform.linux_distribution()[1].split(".")[0])

def get_ctrl_ip(ssh): 
    # Needed for erpc
    cmd = ssh + """ \"ifconfig | grep 155\\.98\\. -B1 | awk 'NR==1{ print $1 }'\" """
    try:
        iface = subprocess.check_output(shlex.split(cmd))
        iface = iface.decode().split(":")[0]
        if not iface:
            return None
    except:
        return None

    cmd = ssh + """ \"ifconfig %s | awk -F ' *|:' '/inet %s/{ print $3 }'\" """ % (iface, 'addr' if which_ubuntu is not 18 else '')
    try:
        ip = subprocess.check_output(shlex.split(cmd))
        ip = ip.decode().strip()
        if not ip:
            return None
    except:
        return None

    return ip


# Get both DPDK-enabled interfaces
def get_iface(ssh, i):
    cmd = ssh + """ \"ifconfig | grep 10\\.1\\.%d -B1 | awk 'NR==1{ print $1 }'\" """ % i
    try:
        iface = subprocess.check_output(shlex.split(cmd))
        iface = iface.decode().split(":")[0]
        if not iface:
            return None, None
    except:
        return None, None

    cmd = ssh + """ \"ifconfig %s | awk -F ' *|:' '/inet %s/{ print $3 }'\" """ % (iface, 'addr' if which_ubuntu is not 18 else '')
    try:
        ip = subprocess.check_output(shlex.split(cmd))
        ip = ip.decode().strip()
        if not ip:
            return None, None
    except:
        return None, None
    
    return ip, iface
    

def get_mac(ssh, iface):
    if iface is None:
        return None
    cmd = ssh + " \"cat /sys/class/net/%s/address\"" % iface
    try: 
        mac = subprocess.check_output(shlex.split(cmd))
        return mac.decode().strip()
    except: 
        return None

def get_machine_info():
    clients = []
    proxies = []
    sequencers = []
    deptrackers = []
    machines = {}

    manual_input_flag = 0

    f = open("machine_list.txt", "r")
    dump = open("machine_info.txt", "w")

    for line in f.readlines():
        if line == '\n':
            continue
        fields = line.split()
        machinename = fields[0]  # e.g., "client-0"
        machineid = fields[1]  # e.g., "pc722"

        if "proxy" in machinename:
            proxies.append(machinename)
        elif "dt" in machinename:
            deptrackers.append(machinename)
        elif "sequencer" in machinename:
            sequencers.append(machinename)
        else: 
            clients.append(machinename)
        
        machines[machinename] = {
                'machineid': machineid
        }

        ssh = " ".join(fields[6:])
        print("SSH command: " + ssh)

        ctrl_ip = get_ctrl_ip(ssh)
        machines[machinename]['ctrl_ip'] = ctrl_ip

        for i in [1, 2]: 
            ip, iface = get_iface(ssh, i)
            mac = get_mac(ssh, iface)

            if iface is None or mac is None:
                manual_input_flag = 1
                print("Couldn't get iface and/or mac--will pause before setting" +
                    " up DPDK so you can input them manually")

            machines[machinename]['iface%d' % i] = iface

        machines[machinename]['mac'] = mac
        machines[machinename]['ip'] = ip
        
        dump.write("%s,%s,%s,%s,%s,%s,%s\n" % (machinename, machineid, 
            machines[machinename]['iface1'], mac, 
            ip, ctrl_ip, machines[machinename]['iface2']))
        print(machinename, machineid, machines[machinename]['iface1'], 
                mac, ip, ctrl_ip, machines[machinename]['iface2'])

    f.close()
    dump.close()

    if manual_input_flag:
        input ("Pausing for manual input; save machine_list.txt with" + 
                " missing values filled in, then hit any key to keep going... ")
        machines, clients, servers = read_machine_info()

    return machines, clients, proxies, sequencers, deptrackers


def read_machine_info():
    clients = []
    proxies = []
    sequencers = []
    deptrackers = []
    machines = {}

    f = open("machine_info.txt", "r")

    for line in f.readlines():
        fields = line.split(',')
        machinename = fields[0]
        machineid = fields[1]
        iface = fields[2]
        mac = fields[3]
        ip = fields[4]
        ctrl_ip = fields[5]
        iface2 = fields[6]

        if "proxy" in machinename:
            proxies.append(machinename)
        elif "dt" in machinename:
            deptrackers.append(machinename)
        elif "sequencer" in machinename:
            sequencers.append(machinename)
        else: 
            clients.append(machinename)
        
        machines[machinename] = {
                'machineid': machineid,
                'iface1': iface,
                'mac': mac,
                'ip': ip,
                'ctrl_ip': ctrl_ip,
                'iface2': iface2
        }

    f.close()
    return machines, clients, proxies, sequencers, deptrackers


def setup_machines(whoami, args):
    home = os.getcwd()
    codes = []
    for i, machine in enumerate(machines.keys()):
        machineid = machines[machine]['machineid']

        print("\nSetting up DPDK on all machines...")
        # Takes as arg: iface
        setup_cmd = ("cd %s; sudo bash dpdk_apt_setup.sh %s %s" % 
                (home, machines[machine]['iface1'], machines[machine]['iface2']))

        ssh = "ssh %s@%s.emulab.net" % (whoami, machineid)
        setup_cmd = "%s '%s'" % (ssh, setup_cmd)
        p = subprocess.Popen(shlex.split(setup_cmd))
        
        # This needs to happen serially so compilation doesn't collide
        p.wait()
        codes.append((machine, p.returncode))

    for machine, rc in codes: 
        print("%s completed with exit status %d" % 
            (machine, rc))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Set up all machines.')
    parser.add_argument('whoami',
            help=('Emulab username'))
    parser.add_argument('--read_info',
            help=('If set, read machine info from machine_info.txt rather than' +
                ' parsing from file.'),
            action='store_true')

    args = parser.parse_args()

    if args.read_info:
        machines, clients, proxies, sequencers, deptrackers = read_machine_info()
    else:
        machines, clients, proxies, sequencers, deptrackers = get_machine_info()

    setup_machines(args.whoami, args)
