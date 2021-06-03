#!/usr/bin/python3

import os
import sys
import json
import logging
import argparse
import multiprocessing
import subprocess as sp

from threading import Thread, current_thread

from ldmsdutils import *

logging_config()

logger = logging.getLogger(__name__)

p = argparse.ArgumentParser(description =
        "Control LDMS Daemons / Monitoring Daemons on all hosts in the cluster")
p.add_argument("-l", "--list", default=False, action="store_true",
               help="List daemons (for *-status only)")
p.add_argument("-j", "--json", default=False, action="store_true",
               help="Produce JSON output (for ldmsd-status and mon-status only)")
p.add_argument("-t", "--threads", default=multiprocessing.cpu_count(), type=int,
               help="Number of threads")
p.add_argument("command", nargs='?', type=str, default="all_status",
               help="One of the: all-status (default), all-stop, all-start, all-cleanup, "
               "ldmsd-status, ldmsd-start, ldmsd-stop, ldmsd_cleanup, "
               "mon_status, mon_start, mon_stop, mon_cleanup")
args = p.parse_args()

# The script shall work in the WORK_DIR
os.chdir(WORK_DIR)
SRC_DIR = sys.path[0]
L = " -l" if args.list else ""
J = " -j" if args.json else ""

procs = dict() # output by host

cmd_map = {
        "ldmsd_status": SRC_DIR + "/ldmsd_ctl.py status" + L + J,
        "ldmsd_start": SRC_DIR + "/ldmsd_ctl.py start" + L + J,
        "ldmsd_stop": SRC_DIR + "/ldmsd_ctl.py stop" + L + J,
        "ldmsd_cleanup": SRC_DIR + "/ldmsd_ctl.py cleanup" + L + J,
        "mon_status": SRC_DIR + "/ldmsd_mon.py status" + J,
        "mon_start": SRC_DIR + "/ldmsd_mon.py start" + J,
        "mon_stop": SRC_DIR + "/ldmsd_mon.py stop" + J,
        "mon_cleanup": SRC_DIR + "/ldmsd_mon.py cleanup" + J,
        "all_start": "{SRC_DIR}/ldmsd_ctl.py start {L}; {SRC_DIR}/ldmsd_mon.py start;".format(**globals()),
        "all_stop": "{SRC_DIR}/ldmsd_ctl.py stop {L}; {SRC_DIR}/ldmsd_mon.py stop;".format(**globals()),
        "all_status": "{SRC_DIR}/ldmsd_ctl.py status {L}; {SRC_DIR}/ldmsd_mon.py status;".format(**globals()),
        "all_cleanup": "{SRC_DIR}/ldmsd_ctl.py cleanup {L}; {SRC_DIR}/ldmsd_mon.py cleanup;".format(**globals()),
    }

def command_unknown():
    print("Unknown command:", args.command)

# Find and execute command handler
cmd = cmd_map.get(args.command.replace("-", "_"))
if not cmd:
    command_unknown()
    sys.exit(-1)

def thread_proc(hosts):
    t = current_thread()
    logger.debug("hosts: {}".format(hosts))
    logger.debug("cmd: {}".format(cmd))
    _ssh_port = " -p {} ".format(SSH_PORT)
    for h in hosts:
        if h == MYHOST:
            _cmd = "bash -c '{}'".format(cmd)
        else:
            _cmd = "ssh " + _ssh_port + h + " " + "'" + cmd + "'"
        p = sp.run(_cmd, shell=True, executable="/bin/bash", stdout=sp.PIPE, stderr=sp.PIPE)
        procs[h] = p

hosts = list(set( SAMP_HOSTS + L1_HOSTS + L2_HOSTS + [ L3_HOST ] ))
hosts.sort()
N = args.threads
M = len(hosts) // N
R = len(hosts) % N

# distribute the load among threads
i0 = 0
load = []
for i in range(N):
    i1 = i0 + M + (i<R)
    load.append( hosts[i0:i1] )
    i0 = i1
thr = [ Thread(target=thread_proc, name="cluster_{}".format(i), args=[ load[i] ]) for i in range(N) ]
for t in thr: t.start()
for t in thr: t.join()

def output():
    for h in hosts:
        p = procs[h]
        print("==", h, "="*(80-4-len(h)))
        print(" -- stdout --")
        print(p.stdout.decode())
        if p.stderr:
            print(" -- stderr --")
            print(p.stderr.decode())
    print("-"*80)

def json_output():
    obj = { h: json.loads(procs[h].stdout.decode()) for h in hosts }
    print(json.dumps(obj, indent=2))

if args.json:
    json_output()
else:
    output()

# return code
rc = 0
for p in procs.values():
    if p.returncode:
        rc = p.returncode
        break
sys.exit(rc)
