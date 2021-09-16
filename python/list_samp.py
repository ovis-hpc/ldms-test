#!/usr/bin/python3 -i

import os
import sys
import time
import logging

from ovis_ldms import ldms
from list_common import G, SCHEMA, add_set, print_set, print_list, gen_data, \
                   update_set, DirMetricList, verify_set, ARRAY_CARD

LOG_FMT = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s"
logging.basicConfig(datefmt="%F-%T", format=LOG_FMT, level=logging.INFO)
log = logging.getLogger()

ldms.init(16*1024*1024)

set1 = add_set("node-1/set1")
set3_p = add_set("node-1/set3_p", array_card=ARRAY_CARD)
set3_c = add_set("node-1/set3_c", array_card=ARRAY_CARD)
set3_c.data_copy_set(1) # turn on data copy

update_set(set1, 1)
update_set(set3_p, 2)
update_set(set3_c, 3)

verify_set(set1)
verify_set(set3_p)
verify_set(set3_c)

x = ldms.Xprt(name="sock")
def listen_cb(ep, ev, arg):
    log.debug("{}: {}".format(ev.type.name, ep))
    G.ep = ep
    G.ev = ev

rc = x.listen(host="0.0.0.0", port=412, cb=listen_cb)
