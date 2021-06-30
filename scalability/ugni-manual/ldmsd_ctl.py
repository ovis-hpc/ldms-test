#!/usr/bin/python3

import os
import sys
import json
import logging
import argparse as ap
import subprocess as sp

from ldmsdutils import *

logging_config()

logger = logging.getLogger(__name__)

p = ap.ArgumentParser(description = "Control LDMS Daemons on a host")
p.add_argument("-l", "--list", default=False, action="store_true",
               help="List daemons (for status only)")
p.add_argument("-j", "--json", default=False, action="store_true",
               help="Produce JSON output (for status only)")
p.add_argument("command", nargs='?', type=str, default="status",
               help="status|start|stop|cleanup|gdb-start")
p.add_argument("daemons", nargs='*', type=str,
               help="list of daemons to control")
args = p.parse_args()

# The script shall work in the WORK_DIR
os.chdir(WORK_DIR)

dir_init()

if args.daemons:
    dset = set(args.daemons)
    fltr = lambda l: l.name in dset
else:
    fltr = None
daemons = get_daemons(fltr)

status_tbl = dict()

INT = lambda x: int(x) if x else 0

def gdb_start(d):
    """Handling `gdb-start` command"""
    if not daemons:
        print("No daemons to work with ... please check config.py")
        sys.exit(-1)
    if len(daemons) > 1:
        print("`gdb-start` supports only 1 daemon, "
              "selecting {} and ignore the rest.".format(d.name))
    d.writeConfig()
    cmd = d.getCmdline(gdb=True)
    print("cmd:", cmd)
    os.execl("/bin/bash", "/bin/bash", "-c", cmd)

fn_tbl = {
        "status": lambda l: status_tbl.update([(l.name, INT(l.getpid()))]),
        "start": lambda l: l.start() if not l.pid else 0,
        "stop": lambda l: l.stop() if l.pid else 0,
        "cleanup": lambda l: l.cleanup(),
        "gdb-start": gdb_start,
    }

fn = fn_tbl.get(args.command)
if not fn:
    print("Unknown command '{}'".format(args.command))
    sys.exit(-1)

rc = 0
for l in daemons:
    try:
        fn(l)
    except Exception as e:
        print(e)
        rc = 1

if args.command == "status":
    if args.json:
        print(json.dumps(status_tbl, indent=2))
        sys.exit(0)
    if args.list:
        items = list( status_tbl.items() )
        items.sort()
        for k, v in items:
            status = "is running, pid: {}".format(v) if v else "is not running"
            print(k, status)
        print("-"*80)
    r = sum( bool(v) for v in status_tbl.values() )
    n = len(status_tbl)
    print("{} out of {} daemons are running".format(r, n))
sys.exit(rc)
