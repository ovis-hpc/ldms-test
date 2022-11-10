#!/usr/bin/python3
#
# Like `client` but with 'push'

import os
import sys
import json
import threading

from ovis_ldms import ldms
from ldms_rail_common import SCHEMA, verify_set, xprt_pool_idx

ldms.init(16*1024*1024)

# set_info['name'] := dict containing information about the set 'name'
set_info = dict()
push_count = 0
push_cond = threading.Condition()

RAIL_EPS = 8

def push_cb(lset, flags, arg):
    global push_count
    global push_cond
    d = set_info.setdefault(lset.name, dict())
    thread_self = threading.get_ident() # this is pthread_self()
    d['thread'] = thread_self
    push_cond.acquire()
    push_count += 1
    push_cond.notify()
    push_cond.release()

r = ldms.Xprt(auth="munge", rail_eps = RAIL_EPS)
r.connect("node-1", 10000)
dsets = r.dir()
lsets = list()
for d in dsets:
    l = r.lookup(d.name)
    l.register_push(cb = push_cb, cb_arg = None)
    lsets.append(l)

set_threads = None

# threads associated to endpoints in the rail
ep_threads = set( r.get_threads() )
pool_indices = xprt_pool_idx(r)
# pool index in used for ep_threads
pool_idx_set = set( pool_indices )


# the `ldms_rail_test` script will update after we wait
def wait_push():
    global push_count
    global push_cond
    print("wait_push")
    push_cond.acquire()
    while push_count < RAIL_EPS:
        push_cond.wait()
    # reset the counter
    push_count = 0
    push_cond.release()

    global set_threads
    # threads processing the set callback
    set_threads = set( e['thread'] for e in set_info.values() )

xprt_free_list = list()
def xprt_free_cb(x):
    global xprt_free_list
    xprt_free_list.append(str(x))
r.set_xprt_free_cb(xprt_free_cb)

def rm_sets():
    global lsets
    global r
    while lsets:
        l = lsets.pop()
        l.delete()
        del(l)
