#!/usr/bin/python3
#
# Only used by ldmsd_rail_test and will be run interactively

import os
import sys
import time
import ctypes
import signal
import atexit

from ovis_ldms import ldms
from ldms_rail_common import SCHEMA, sample, verify_set, xprt_pool_idx

ldms.init(16*1024*1024)

# Request SIGHUP our process when parent exited
libc = ctypes.CDLL(None)
# prctl(PR_SET_PDEATHSIG, SIGHUP)
libc.prctl(1, 1)

# Create a transport
lx = ldms.Xprt(auth="none", rail_eps=8)

xset = list() # transport collection

@atexit.register
def at_exit():
    global xset
    if xset: # If there is at least one connected transports, exit with an error.
        print("XPRT collection not empty:", xset, flush=True)
        os._exit(1)
    print("server terminated", flush=True)
    os._exit(0)

xprt_free_list = list()
def xprt_free_cb(x):
    global xprt_free_list
    xprt_free_list.append(str(x))

def listen_cb(x, ev, arg):
    global xset
    # x is the new transport for CONNECTED event
    if ev.type == ldms.EVENT_CONNECTED:
        # asserting that the newly connected transport is a new one.
        assert(x.ctxt == None)
        x.ctxt = "some_context {}".format(x)
        xset.append(x)
        x.set_xprt_free_cb(xprt_free_cb)
    elif ev.type == ldms.EVENT_DISCONNECTED:
        # also asserting that this is the transport from the earlier CONNECTED
        # event.
        assert(x.ctxt == "some_context {}".format(x))
        xset.remove(x)
    elif ev.type == ldms.EVENT_REJECTED:
        assert(0 == "Unexpected event!")

# Listen with callback
lx.listen(host="0.0.0.0", port=411, cb=listen_cb, cb_arg=None)

# run interactively
