#!/usr/bin/python3 -i

import os
import sys

from ovis_ldms import ldms
from record_common import *

ldms.init(16*1024*1024)

x0 = ldms.Xprt(name="sock")
x0.connect(host="localhost", port=411)

x1 = ldms.Xprt(name="sock")
x1.connect(host="localhost", port=412)

d0 = x0.dir()
d1 = x1.dir()

record_sampler = x0.lookup(d0[0].name)
set1 = x1.lookup("node-1/set1")
set3_p = x1.lookup("node-1/set3_p")
set3_c = x1.lookup("node-1/set3_c")

record_sampler.update()
set1.update()
set3_p.update()
set3_c.update()

verify_set(record_sampler)
verify_set(set1)
verify_set(set3_p)
verify_set(set3_c)
