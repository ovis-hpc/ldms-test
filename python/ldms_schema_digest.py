#!/usr/bin/python3
#
# This script is meant to be used only by `ldms_schema_digest_test` script.

import os
import sys
import json

from ovis_ldms import ldms

ldms.init(1024*1024)

obj = dict()

x = ldms.Xprt("sock")
x.connect("agg-1", "10000")
dlist = x.dir()

assert(len(dlist) == 1)

obj["dir_digest_str"] = dlist[0].digest_str

s0 = x.lookup(dlist[0].name)
s0.update()

obj['set_digest_str'] = s0.digest_str

print(json.dumps(obj, indent=2))
