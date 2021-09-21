#!/usr/bin/python3 -i

import os
import sys
import time
import logging

from set_array_common import G, set_dict, print_set, jprint, PORT, dict_list
from ovis_ldms import ldms

LOG_FMT = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s"
logging.basicConfig(datefmt="%F-%T", format=LOG_FMT, level=logging.INFO)
log = logging.getLogger()

ldms.init(16*1024*1024)

SCHEMA = ldms.Schema("set_array", metric_list = [
        ("x", ldms.V_S64),
        ("y", ldms.V_S64),
        ("z", ldms.V_S64),
    ])

SCHEMA.set_array_card(1)
s1 = ldms.Set("set1", SCHEMA)
s1.publish()
SCHEMA.set_array_card(3)
s3 = ldms.Set("set3", SCHEMA)
s3.publish()

x = ldms.Xprt(name="sock")
def listen_cb(ep, ev, arg):
    log.debug("{}: {}".format(ev.type.name, ep))
    G.ep = ep
    G.ev = ev
rc = x.listen(host="0.0.0.0", port=PORT, cb=listen_cb)

sample_rec = list()

def sample(s, v):
    s.transaction_begin()
    s[:] = range(v, v+3)
    s.transaction_end()
    _set = set_dict(s)
    sample_rec.append(_set)
