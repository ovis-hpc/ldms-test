#!/usr/bin/env python3
#
# This script is originally meant to be run by `set_array_test` inside the
# container that `set_array_test` generates.
#
# SYNOPSIS
#     ldms_update.py -H HOST -P PORT -I INTERVAL -O OFFSET -C COUNT
#
# DESCRIPTION
#     Performs LDMS set update to all sets on the the specified ldmsd with
#     specified interval and offset. Each update results in a hunk of update
#     callbacks (e.g. by set_array_card) which is recorded in the script. The
#     script stops updating after COUNT updates. Finally, the recorded update
#     hunks are printed in JSON format to STDOUT.
#
# OUTPUT FORMAT
#     [
#       [
#         {"name": ..., "metrics": ...},
#         {"name": ..., "metrics": ...},
#         ...
#       ],
#       [
#         {"name": ..., "metrics": ...},
#         {"name": ..., "metrics": ...},
#         ...
#       ],
#       ...
#     ]
#
#    The output is a list of HUNKs. Each HUNK is a list of dictionaries.
#    Each dictionary in the hunk is a JSON dump of the LDMS set received in
#    update callback.


import os
import sys
import json
import time
import argparse
import threading

from ovis_ldms import ldms

if __name__ != "__main__":
    raise ImportError("This is not a module")

ldms.init(1024*1024*16)

M = 1000000 # million: 1e6

ap = argparse.ArgumentParser()
ap.add_argument("-H", "--host", type=str, default="node-1", help="ldmsd host.")
ap.add_argument("-P", "--port", type=int, default=10000, help="ldmsd port.")
ap.add_argument("-I", "--interval", type=int, default=1*M,
                help="update interval (in micro seconds).")
ap.add_argument("-O", "--offset", type=int, default=M//2,
                help="update offset (in micro seconds).")
ap.add_argument("-C", "--count", type=int, default = 3,
                help="The number of updates to perform.")
args = ap.parse_args()

update_hunks = []

def to_record(_set):
    # convert _set to JSON-friendly record
    return dict(
            card                   =  _set.card,
            data_gn                =  _set.data_gn,
            data_sz                =  _set.data_sz,
            gid                    =  _set.gid,
            instance_name          =  _set.instance_name,
            is_consistent          =  _set.is_consistent,
            meta_gn                =  _set.meta_gn,
            meta_sz                =  _set.meta_sz,
            name                   =  _set.name,
            perm                   =  _set.perm,
            producer_name          =  _set.producer_name,
            schema_name            =  _set.schema_name,
            transaction_duration   =  _set.transaction_duration,
            transaction_timestamp  =  _set.transaction_timestamp,
            uid                    =  _set.uid,
            metrics                =  _set.as_dict()
        )

def update_cb(_set, flags, arg):
    # arg is a list of records
    sem, lst = arg
    rec = to_record(_set)
    lst.append(rec)
    if (flags & ldms.UPD_F_MORE) == 0:
        sem.release()

def update_sets(sets, arg):
    sem = threading.Semaphore(0)
    for s in sets:
        s.update(cb=update_cb, cb_arg=(sem, arg))
    for s in sets: # update_cb does sem post
        sem.acquire()

# connect
x = ldms.Xprt()
x.connect(host=args.host, port=args.port)
dirs = x.dir()
names = [ d.name for d in dirs ]
sets = [ x.lookup(d) for d in names ]

I = args.interval
O = args.offset
for i in range(0, args.count):
    now = time.time()
    t1 = ((int(now*M)//I + 1)*I + O)/M # next interval
    dt = t1 - now
    time.sleep(dt)
    records = list()
    update_sets(sets, records)
    update_hunks.append(records)

print(json.dumps(update_hunks))
