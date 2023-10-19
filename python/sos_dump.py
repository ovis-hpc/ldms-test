#!/usr/bin/python3

import os
import sys
import json

from sosdb import Sos as sos

try:
    path = sys.argv[1]
except:
    print("Usage: sos_dump.py PATH")
    sys.exit(-1)

cont = sos.Container()
cont.open(path)

data = dict()

def itr_wrap(itr):
    b = itr.begin()
    while b:
        o = itr.item()
        yield(o)
        b = itr.next()

for sch in cont.schema_iter():
    # use the first indexed attribute to iterate through all objects
    for attr in sch:
        if attr.is_indexed():
            break
    else: # no indexed attribute
        data[sch.name()] = list()
        continue
    itr = attr.attr_iter()
    data[sch.name()] = [ tuple(o) for o in itr_wrap(itr) ]

print(json.dumps(data, indent=1))
