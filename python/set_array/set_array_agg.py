#!/usr/bin/python3 -i

import os
import sys
import time
import logging
import json
import threading

from set_array_common import G, set_dict, print_set, jprint, PORT, dict_list

from ovis_ldms import ldms

LOG_FMT = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s"
logging.basicConfig(datefmt="%F-%T", format=LOG_FMT, level=logging.INFO)
log = logging.getLogger()

ldms.init(16*1024*1024)

x = ldms.Xprt("sock")
x.connect(host="localhost", port=PORT)
dirs = x.dir()
names = [ d.name for d in dirs ]
assert( set(names) == set([ "set1", "set3" ]) )

s1 = x.lookup("set1")
s3 = x.lookup("set3")

update_rec = list()
sem = threading.Semaphore(1)

def update_cb(s, fl, a):
    #print("flag:", fl)
    _set = set_dict(s)
    update_rec.append(_set)
    if fl == 0:
        sem.release()

def update_set(s):
    global sem
    sem.acquire()
    s.update(cb=update_cb)
