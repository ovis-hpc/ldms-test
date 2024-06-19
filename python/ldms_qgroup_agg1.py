#!/usr/bin/python3
#
# For `ldms_qgroup_samp`. This script is meant to be invoked from nodes:
# agg11, agg12, and agg13.

import os
import re
import sys
import time
import socket
import logging

from ovis_ldms import ldms
from threading import Thread
from ldms_qgroup_common import Prdcr, PrdcrState, QGROUP

LOG_FMT = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s"
DATE_FMT = "%F %T"

logging.basicConfig(filename="/var/log/agg.log",
                    level=logging.INFO,
                    datefmt=DATE_FMT,
                    format=LOG_FMT)
rlog = logging.getLogger() # root logger
rlog.setLevel(logging.INFO)
slog = logging.getLogger("stream")
slog.setLevel(logging.INFO)

xset = list()
quota_list = list()
ldms.init(64*1024*1024)

INTERVAL = 1.0 # sec

hostname = socket.gethostname()

nodes = [ f"agg1{i}" for i in range(1,4) ]
nodes = list(filter(lambda x: x != hostname, nodes)) # remove self

qgroup_members = [
        # xprt, host, port, auth, auth_opt
        ("sock", n, 411, "munge", None) for n in nodes
    ]

def get_samps(a):
    istr = a.removeprefix("agg1")
    i = int(istr)
    i2 = 2*i
    return [ f"samp{i2-1}", f"samp{i2}" ]

def cbfn(x, ev, arg):
    global xset
    # x is the new transport for CONNECTED event
    if ev.type == ldms.EVENT_CONNECTED:
        # asserting that the newly connected transport is a new one.
        assert(x.ctxt == None)
        x.ctxt = "some_context {}".format(x)
        xset.append(x)
    elif ev.type == ldms.EVENT_DISCONNECTED:
        # also asserting that this is the transport from the earlier CONNECTED
        # event.
        assert(x.ctxt == "some_context {}".format(x))
        xset.remove(x)
    elif ev.type == ldms.EVENT_REJECTED:
        assert(0 == "Unexpected event!")
    elif ev.type == ldms.LDMS_XPRT_EVENT_SEND_QUOTA_DEPOSITED:
        quota_list.append(ev.quota)

lx = ldms.Xprt(auth="munge", rail_eps=1, rail_recv_limit = 32)
lx.listen(cb=cbfn) # default 0:411

if True:
    ldms.qgroup.cfg_quota      = QGROUP.CFG_QUOTA
    ldms.qgroup.cfg_ask_mark   = QGROUP.CFG_ASK_MARK
    ldms.qgroup.cfg_ask_amount = QGROUP.CFG_ASK_AMOUNT
    ldms.qgroup.cfg_ask_usec   = QGROUP.CFG_ASK_USEC
    ldms.qgroup.cfg_reset_usec = QGROUP.CFG_RESET_USEC
else:
    ldms.qgroup.cfg_quota = 32
    ldms.qgroup.cfg_ask_mark = 8
    ldms.qgroup.cfg_ask_amount = 8
    ldms.qgroup.cfg_ask_usec = 1000000 # 1.0 sec
    ldms.qgroup.cfg_reset_usec = 5000000 # 5.0 sec

for m in qgroup_members:
    ldms.qgroup.member_add(*m)

# ldms.qgroup.start()

# stream logging
def stream_cb(cli, data: ldms.StreamData, cb_arg):
    t = time.clock_gettime(time.CLOCK_REALTIME)
    slog.info(f"{t:.6f} {data.src} {data.name}: {data.data}")

cli = ldms.StreamClient(".*", True, stream_cb, None)

# prdcrs
samps = get_samps(hostname)
prdcrs = [ Prdcr(samp, auth="munge", rail_recv_limit=32) for samp in samps ]

def prdcr_thread():
    while True:
        for p in prdcrs:
            if p.state == PrdcrState.DISCONNECTED:
                p.connect()
        rlog.info(f"qgroup quota_probe: {ldms.qgroup.quota_probe}")
        for x in xset:
            rlog.info(f"pending_ret_quota {x}: {x.pending_ret_quota}")
        time.sleep(INTERVAL)

thr = Thread(target = prdcr_thread)
thr.start()
