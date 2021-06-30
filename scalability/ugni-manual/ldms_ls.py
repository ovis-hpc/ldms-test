#!/usr/bin/python3

import os
import io
import pdb
import sys
import time
import socket
import logging
import argparse as ap
import subprocess as sp
import threading as thread
from queue import Queue
from ovis_ldms import ldms

logging.basicConfig(level=logging.INFO, datefmt="%F %T",
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s")

log = logging.getLogger()

psr = ap.ArgumentParser(description="Python LDMS dir, lookup and peridically updates", add_help=False)
psr.add_argument("-x", "--xprt", default="sock",
                 help="Transport type (default: sock)")
psr.add_argument("-p", "--port", default="20001",
                 help="Port to listen or to connect (default: 20001)")
psr.add_argument("-h", "--host", default=socket.gethostname(),
                 help="Port to listen or to connect (default: ${HOSTNAME})")
psr.add_argument("-m", "--mem", type=int, default=16*1024*1024,
                 help="Memory allocation (default: 16000000)")
psr.add_argument("-i", "--interval", default=1.0, type=float,
                 help="Interval (1.0 sec)")
psr.add_argument("-?", "--help", action="help",
                 help="Show help message")
g = psr.parse_args()
ldms.init(g.mem)

g.x = ldms.Xprt(name=g.xprt)
g.sets = dict()
g.cond = thread.Condition()
g.num_lookups = 0
g.num_updates = 0
g.num_sets = 0
g.done_dir = False

def interval_block(interval, offset):
        t0 = time.time()
        t1 = (t0 + interval)//interval*interval + offset
        dt = t1 - t0
        time.sleep(dt)

def client_lookup_cb(x, status, more, lset, arg):
    g.sets[lset.name] = lset
    g.num_lookups += 1
    g.cond.acquire()
    g.cond.notify()
    g.cond.release()

def client_dir_cb(x, status, dir_data, arg):
    g.dir_data = dir_data
    for d in dir_data.set_data:
        g.num_sets += 1
        x.lookup(name=d.name, cb=client_lookup_cb)
    if not dir_data.more:
        g.done_dir = True
        g.cond.acquire()
        g.cond.notify()
        g.cond.release()

def on_client_connected(x, ev, arg):
    x.dir(cb = client_dir_cb)

def on_client_rejected(x, ev, arg):
    log.error("Rejected")

def on_client_error(x, ev, arg):
    log.error("Connect error")

def on_client_disconnected(x, ev, arg):
    log.error("Disconnected")

def on_client_recv(x, ev, arg):
    log.error("Receiving unexpected message ...")

def on_client_send_complete(x, ev, arg):
    #log.error("Unexpected send completion")
    pass

EV_TBL = {
        ldms.EVENT_CONNECTED: on_client_connected,
	ldms.EVENT_REJECTED: on_client_rejected,
	ldms.EVENT_ERROR: on_client_error,
	ldms.EVENT_DISCONNECTED: on_client_disconnected,
	ldms.EVENT_RECV: on_client_recv,
	ldms.EVENT_SEND_COMPLETE: on_client_send_complete,
    }

def client_cb(x, ev, arg):
    fn = EV_TBL.get(ev.type)
    assert(fn != None)
    fn(x, ev, arg)

def client_update_cb(lset, flags, arg):
    g.num_updates += 1
    if g.num_updates == g.num_sets:
        g.cond.acquire()
        g.cond.notify()
        g.cond.release()

def client_proc():
    # async connect
    g.x.connect(host=g.host, port=g.port, cb=client_cb, cb_arg=None)
    # wait for dir
    g.cond.acquire()
    while not g.done_dir:
        g.cond.wait()
    g.cond.release()
    # wait for lookup
    g.cond.acquire()
    while g.num_lookups < g.num_sets:
        g.cond.wait()
    g.cond.release()
    assert(g.num_lookups == g.num_sets)
    log.info("lookup completed")

    # periodically update sets
    while True:
        interval_block(g.interval, 0.2)
        g.num_updates = 0
        for s in g.sets.values():
            s.update(client_update_cb, None)
        # wait for update completions
        g.cond.acquire()
        while g.num_updates < g.num_sets:
            g.cond.wait()
        g.cond.release()
        v = 0
        for s in g.sets.values():
            v += s.is_consistent
        log.info("{}/{} sets are consistent".format(v, g.num_sets))

client_proc()
