#!/usr/bin/python3
#
# ldmsd monitoring utility

import os
import sys
import time
import logging
import argparse as ap
import subprocess as sp

from ldmsdutils import *

logging_config()

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT = "mon/{}.mon".format(MYHOST)

p = ap.ArgumentParser(description = "Monitor LDMS Daemons on a host")
p.add_argument("-j", "--json", default=False, action="store_true",
               help="Produce JSON output (for status only)")
p.add_argument("command", nargs='?', type=str, default="status",
               help="status|start|stop|cleanup")
args = p.parse_args()

# The script shall work in the WORK_DIR
os.chdir(WORK_DIR)

dir_init()

proc = LDMSDMon()

def status():
    pid = proc.getpid()
    if args.json:
        print('{{"mon": {}}}'.format(pid if pid else 0))
        return
    if pid:
        print("ldmsd_mon is running, pid:", pid)
    else:
        print("ldmsd_mon is NOT running")

def unknown():
    print("Unknown command:", args.command)

cmd_tbl = {
        "start": lambda : proc.start() if not proc.pid else 0,
        "stop": lambda: proc.stop() if proc.pid else 0,
        "status": status,
        "cleanup": proc.cleanup,
    }
rc = 0
fn = cmd_tbl.get(args.command, unknown)
try:
    fn()
except Exception as e:
    print(e)
    rc = 1
sys.exit(rc)
