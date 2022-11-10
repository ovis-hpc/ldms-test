#!/usr/bin/python3

import os
import sys
import json
import threading

from ovis_ldms import ldms
from ldms_rail_common import SCHEMA, verify_set, xprt_pool_idx

ldms.init(16*1024*1024)

# set_info['name'] := dict containing information about the set 'name'
set_info = dict()
update_count = 0
update_cond = threading.Condition()

RAIL_EPS = 8

def update_cb(lset, flags, arg):
    global update_count
    global update_cond
    d = set_info.setdefault(lset.name, dict())
    thread_self = threading.get_ident() # this is pthread_self()
    d['thread'] = thread_self
    update_count += 1
    update_cond.acquire()
    update_cond.notify()
    update_cond.release()

r = ldms.Xprt(auth="munge", rail_eps = RAIL_EPS)
r.connect("node-1", 10000)
dsets = r.dir()
lsets = list()
for d in dsets:
    l = r.lookup(d.name)
    lsets.append(l)

xprt_free_list = list()
def xprt_free_cb(x):
    global xprt_free_list
    xprt_free_list.append(str(x))
r.set_xprt_free_cb(xprt_free_cb)

def update_sets():
    global update_count
    global update_cond
    global lsets
    update_count = 0
    for l in lsets:
        l.update(cb = update_cb)
    update_cond.acquire()
    while update_count < len(lsets):
        update_cond.wait()
    update_cond.release()

update_sets()

# threads processing the set callback
set_threads = set( e['thread'] for e in set_info.values() )

# threads associated to endpoints in the rail
ep_threads = set( r.get_threads() )

# pool index in used for ep_threads
pool_indices = xprt_pool_idx(r)
pool_idx_set = set( pool_indices )

def rm_sets():
    global lsets
    global r
    while lsets:
        l = lsets.pop()
        l.delete()
        del(l)
