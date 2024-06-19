#!/usr/bin/python3

import os
import sys
import time
import logging

from threading import Thread

from ovis_ldms import ldms

xset = list()
quota_list = list()

LOG_FMT = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s"
DATE_FMT = "%F %T"

logging.basicConfig(filename="/var/log/samp.log",
                    level=logging.INFO,
                    datefmt=DATE_FMT,
                    format=LOG_FMT)
rlog = logging.getLogger() # root logger
rlog.setLevel(logging.INFO)
slog = logging.getLogger("stream")
slog.setLevel(logging.INFO)

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
        rlog.info(f"quota deposited: {ev.quota}, x: {x}")
        quota_list.append(ev.quota)

lx = ldms.Xprt(auth="munge", rail_eps=1, rail_recv_limit = 32)
lx.listen(cb=cbfn) # default 0:411

def stream_cb(cli, data: ldms.StreamData, cb_arg):
    global xset
    t = time.clock_gettime(time.CLOCK_REALTIME)
    slog.info(f"{t:.6f} {data.src} {data.name}: {data.data}")
    for x in xset:
        rlog.info(f"send_quota {x}: {x.send_quota}")

cli = ldms.StreamClient(".*", True, stream_cb, None)

def probe_proc():
    while True:
        for x in xset:
            rlog.info(f"quota {x}: {x.send_quota}")
        time.sleep(1.0)

thr = Thread(target = probe_proc)
thr.start()
