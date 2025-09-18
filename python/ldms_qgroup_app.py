#!/usr/bin/python3

import os
import sys
import time
import socket
import logging

from ovis_ldms import ldms
from threading import Thread, Lock, Condition

HOSTNAME = socket.gethostname()

LOG_FMT = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s"
DATE_FMT = "%F %T"

logging.basicConfig(filename="/var/log/app.log",
                    level=logging.INFO,
                    datefmt=DATE_FMT,
                    format=LOG_FMT)
rlog = logging.getLogger() # root logger
rlog.setLevel(logging.INFO)
slog = logging.getLogger("msg")
slog.setLevel(logging.INFO)

xset = list()
quota_list = list()

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

x = ldms.Xprt(auth="munge", rail_eps=1)
x.connect("localhost", 411, cb=cbfn, cb_arg=x)


class Publisher(object):
    def __init__(self, x:ldms.Xprt, name, interval):
        self.x = x
        self.name = name
        self.interval = interval
        self.thr = None
        self.lock = Lock()
        self.cond = Condition(self.lock)
        self.is_running = False
        self.count = 0

    def msg_gen(self):
        t = time.clock_gettime(time.CLOCK_REALTIME)
        return f"{HOSTNAME} - {t:.3f} {self.count}"

    def proc(self):
        self.lock.acquire()
        while self.is_running:
            data = self.msg_gen()
            try:
                self.x.msg_publish(self.name, data,
                                      msg_type = ldms.LDMS_MSG_STRING)
                slog.info(f"published: [{self.name}]: {data}")
                self.count += 1
            except Exception as e:
                slog.info(f"pub_failed: {e}")
                pass
            t0 = time.time()
            t1 = (t0 // self.interval * self.interval) + self.interval
            self.cond.wait(timeout = (t1 - t0))
        self.lock.release()

    def start(self):
        self.lock.acquire()
        if self.is_running:
            self.lock.release()
            raise RuntimeError("Thread is already running ...")
        try:
            self.is_running = True
            self.thr = Thread(target = self.proc)
            self.thr.start()
        except:
            self.thr = None
            self.is_running = False
            self.lock.release()
            raise
        self.lock.release()

    def stop(self):
        self.lock.acquire()
        if not self.is_running:
            self.lock.release()
            raise RuntimeError("Thread is NOT running")
        self.is_running = False
        self.cond.notify()
        self.lock.release()
        self.thr.join()

p = Publisher(x, name='app', interval=0.1)
