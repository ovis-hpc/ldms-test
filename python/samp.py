#!/usr/bin/python3

import os
import sys
import time
import logging

from ovis_ldms import ldms

LOG_FMT = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s"
logging.basicConfig(datefmt="%F-%T", format=LOG_FMT, level=logging.INFO)
log = logging.getLogger()

class Global(object): pass
G = Global()

ldms.init(16*1024*1024)

schema = ldms.Schema("test", metric_list = [
        ("x", int),
        ("y", int),
        ("z", int),
    ])

set0 = ldms.Set("node-1/set0", schema)
set0.transaction_begin()
set0.transaction_end()
set0.publish()

G.sets = { set0.name: set0 }

def add_set(i):
    global G, schema
    _set = ldms.Set("node-1/set{}".format(i), schema)
    _set.transaction_begin()
    _set.transaction_end()
    _set.publish()
    G.sets[_set.name] = _set

def rm_set(i):
    global G, schema
    key = "node-1/set{}".format(i)
    _set = G.sets.pop(key)
    _set.delete()

x = ldms.Xprt(name="sock")
def listen_cb(ep, ev, arg):
    log.debug("{}: {}".format(ev.type.name, ep))
    G.ep = ep
    G.ev = ev
rc = x.listen(host="node-1", port=411, cb=listen_cb)
