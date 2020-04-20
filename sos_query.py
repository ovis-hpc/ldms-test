#!/usr/bin/python3

import os
import sys
import argparse as ap
import numpy
import json

from sosdb import Sos

parser = ap.ArgumentParser(description='sos_query utility')
parser.add_argument('--container', '-C', metavar='PATH',
                    help='The path to the SOS container.')
parser.add_argument('--schema', '-S', metavar='SCHEMA',
                    help='The schema name.')
parser.add_argument('--index', '-X', metavar='INDEX',
                    help='The name of the index to iterate through.')
args = parser.parse_args()

cont = Sos.Container()
cont.open(args.container)
schema = cont.schema_by_name(args.schema)
index = schema[args.index]

itr = index.attr_iter()
def obj_iter(itr):
    b = itr.begin()
    while b:
        yield itr.item()
        b = itr.next()

print('[')
names = [ a.name().decode() for a in schema ]
first = True
for obj in obj_iter(itr):
    values = map(lambda v: v.tolist() if type(v) == numpy.ndarray else v, obj)
    _dict = dict(zip(names, values))
    _s = json.dumps(_dict, indent=2)
    if not first:
        print(',')
    first = False
    print(_s)
print(']')
