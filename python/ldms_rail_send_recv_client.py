#!/usr/bin/python3

import os
import sys
import time
import ctypes
import signal
import atexit
import threading

from ovis_ldms import ldms

ldms.init(16*1024*1024)

# Request SIGHUP our process when parent exited
libc = ctypes.CDLL(None)
# prctl(PR_SET_PDEATHSIG, SIGHUP)
libc.prctl(1, 1)

@atexit.register
def at_exit():
    print("terminated", flush=True)
    os._exit(0)

class Blocker(object):
    def __init__(self):
        self.cond = threading.Condition()
        self.is_blocking = False

    def block(self):
        self.cond.acquire()
        self.is_blocking = True
        while self.is_blocking:
            self.cond.wait()
        self.cond.release()

    def unblock(self):
        self.cond.acquire()
        self.is_blocking = False
        self.cond.notify()
        self.cond.release()

blocker = Blocker()

xprt_free_list = list()
def xprt_free_cb(x):
    global xprt_free_list
    xprt_free_list.append(str(x))

recv_data = list()
quota_list = []
quota_amount_list = []

def msg_cb(sc, sdata, arg):
    global blocker, recv_data
    recv_data.append(sdata.raw_data)
    blocker.block()

sc = ldms.MsgClient('.*', True, msg_cb, None)

def xprt_cb(x, ev, arg):
    global blocker, recv_data
    if ev.type == ldms.LDMS_XPRT_EVENT_RECV:
        recv_data.append(ev.data)
        blocker.block()
    elif ev.type == ldms.LDMS_XPRT_EVENT_SEND_QUOTA_DEPOSITED:
        quota_list.append(ev.quota)
        quota_amount_list.append(ev.quota.quota)

PRDCR = "node-1"
# Create a transport
r = ldms.Xprt(auth="munge", rail_eps=8, rail_recv_limit = 32)

r.connect(host = PRDCR, port = 10000, cb = xprt_cb, cb_arg = None)
