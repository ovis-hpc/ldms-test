#!/usr/bin/python3

import os
import sys
import json
import time
import argparse as ap

from threading import Condition, Lock
from ovis_ldms import ldms

if __name__ != "__main__":
    raise ImportError("Not a module!")

#####################
# Argument Handling #
#####################
p = ap.ArgumentParser(description = "ldms_ls in Python, output in JSON format",
                      add_help = False)

p.add_argument("-?", action = 'help', help = "Show help message and exit")
p.add_argument("--xprt", "-x", default = "sock",
               help = "Transport (sock|rdma|ugni ; default: sock)")
p.add_argument("--port", "-p", default = 411, type = int,
               help = "Port (default: 411)")
p.add_argument("--host", "-h", default = "localhost",
               help = "Host (default: localhost)")
p.add_argument("--lookup", "-l", default = False, action = "store_true",
               help = "Also lookup, update and print metric data (default: False)")
p.add_argument("--auth", "-a", help = "Authentication method.")
p.add_argument("--auth_opts", "-A", action = "append",
               help = "Authentication arguments (name=value).")
args = p.parse_args()

ldms.init(64*1024*1024)

#################################
# Helping functions and objects #
#################################
def as_dict(o):
    """Get attributes of object `o` and put into a dictionary"""
    return { a: getattr(o, a) for a in dir(o) if not a.startswith('_') and not callable(getattr(o, a)) }

def dir2dict(o):
    """Converts ldms.DirSet `d` to `dict`"""
    d = as_dict(o)
    # convert timestamp format for consistency
    for k in ['timestamp', 'duration']:
        t = d.pop(k)
        d[k] = { 'sec': t[0], 'usec': t[1] }
    return d

def set2dict(s):
    """Convert ldms set `s` to `dict`"""
    d = as_dict(s)
    # convert timestamp format for consistency
    for k in ['transaction_duration', 'transaction_timestamp']:
        t = d.pop(k)
        d[k.replace('transaction_', '')] = t
    d['metrics'] = s.as_dict()
    return d

class RefCount(object):
    """Thread-safe reference counter"""
    def __init__(self):
        self.ref_count = 0
        self.lock = Lock()
        self.cond = Condition(self.lock)

    def get(self):
        """Getting a reference (increase the reference counter)"""
        self.lock.acquire()
        self.ref_count += 1
        self.lock.release()

    def put(self):
        """Putting down a refernce (decrease the reference counter)"""
        self.lock.acquire()
        assert(self.ref_count > 0)
        self.ref_count -= 1
        if self.ref_count == 0:
            self.cond.notify()
        self.lock.release()

    def wait_zero(self, timeout = None):
        """Wait until the reference counter reaches 0, or timeout (if given)

        Returns `True` if the reference counter reaches 0, or
        returns `False` if the timeout occurs.
        """
        tout = time.time() + timeout if timeout else None
        self.lock.acquire()
        while self.ref_count > 0:
            dt = tout - time.time() if tout else None
            if dt is not None and dt <= 0:
                self.lock.release()
                return False # timeout
            self.cond.wait(dt)
        self.lock.release()
        return True

def update_cb(lset, flags, ctxt):
    global ref_count
    sets[lset.name]['lset'] = lset
    ref_count.put()

def lookup_cb(xprt, status, more, lset, ctxt):
    global ref_count
    ref_count.get()
    lset.update(cb = update_cb)
    if not more:
        ref_count.put() # corresponds to the `lookup()` call

##########################
# `ldms_ls` main routine #
##########################
auth_opts = {}
if args.auth_opts is not None:
    for a in args.auth_opts:
        k, v = a.split("=")
        auth_opts[k] = v
else:
    auth_opts = None
auth = "none"
if args.auth is not None:
    auth = args.auth
x = ldms.Xprt(args.xprt, auth = auth, auth_opts = auth_opts)
x.connect(host = args.host, port = args.port)
sets = x.dir()
sets = { s.name: dir2dict(s) for s in sets }

if args.lookup:
    ref_count = RefCount()
    ref_count.get()
    x.lookup(".*", flags = ldms.LOOKUP_RE, cb = lookup_cb)
    ref_count.wait_zero()
    for k, d in sets.items():
        lset = d.pop('lset', None)
        if not lset:
            continue
        l = lset.json_obj()
        d.update(l)
print(json.dumps(sets, indent=2))
sys.exit(0)
