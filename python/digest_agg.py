#!/usr/bin/python3
#
# This script is meant to be used only by `ldms_schema_digest_test` script.

import os
import sys
import time
import json
import argparse

from ovis_ldms import ldms

ldms.init(16*1024*1024)

class Global(object): pass
G = Global()

p = argparse.ArgumentParser()
p.add_argument("port", metavar="PORT", type=int, nargs='+')
args = p.parse_args()

xlist = list()
dir_dict = dict()
set_dict = dict()
digest_dict = dict()
for port in args.port:
    x = ldms.Xprt(name="sock")
    x.connect("localhost", port=port)
    xlist.append(x)
    _dir = x.dir()
    dir_dict[port] = _dir
    _sets = [ x.lookup(d.name) for d in _dir ]
    set_dict[port] = _sets
    for s in _sets:
        digest_dict[s.name] = s.digest_str
print(json.dumps(digest_dict, indent=2))
