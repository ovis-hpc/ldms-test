#!/usr/bin/python3
#
# For `ldms_qgroup_samp`. This script is meant to be invoked from nodes:
# agg2.

import os
import re
import sys
import time
import socket
import logging

from ovis_ldms import ldms
from threading import Thread
from ldms_qgroup_common import Prdcr, PrdcrState, StreamThroughput

LOG_FMT = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s"
DATE_FMT = "%F %T"

logging.basicConfig(filename="/var/log/agg.log",
                    level=logging.INFO,
                    datefmt=DATE_FMT,
                    format=LOG_FMT)
rlog = logging.getLogger() # root logger
slog = logging.getLogger("stream")
slog.setLevel(logging.INFO)

xset = list()
quota_list = list()
ldms.init(64*1024*1024)

INTERVAL = 1.0 # sec

hostname = socket.gethostname()

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

lx = ldms.Xprt(auth="munge")
lx.listen(cb=cbfn) # default 0:411

# prdcrs
prdcrs = [ Prdcr(h, auth="munge", rail_recv_limit=32) for h in ["agg11", "agg12", "agg13"] ]

def prdcr_thread():
    while True:
        for p in prdcrs:
            if p.state == PrdcrState.DISCONNECTED:
                p.connect()
        time.sleep(INTERVAL)

thr = Thread(target = prdcr_thread)
thr.start()
sdata = list()

st = StreamThroughput()

def stream_cb(cli:ldms.StreamClient, data:ldms.StreamData, arg):
    global sdata
    slog.info(f"recv: {data.src} [{data.name}]: {data.data}")
    ts = time.time()
    sdata.append( (ts, data.name, data.data, data) )
    st.add_data(data)

cli = ldms.StreamClient('.*', True, stream_cb, None)
