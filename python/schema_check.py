#!/usr/bin/python3
# This is a part of maestro_schema_registry_test

import os
import pdb

from ovis_ldms import ldms
from maestro.schema_registry import Schema, SchemaRegistryClient

ldms.init(16*1024*1024)

def simple_connect(host, port):
    x = ldms.Xprt("sock")
    x.connect(host, port)
    return x

def get_sets(x):
    dirs = x.dir()
    lst  = []
    for d in dirs:
        s = x.lookup(d.name)
        s.update()
        lst.append(s)
    return lst

x411 = simple_connect("localhost", 411)
x412 = simple_connect("localhost", 412)

s411 = get_sets(x411)
s412 = get_sets(x412)

sets = s411 + s412
s0, s1 = sets

_id0 = s0.schema_name + "-" + s0.digest_str.lower()
_id1 = s1.schema_name + "-" + s1.digest_str.lower()

# comparing information from sets and the schema registry

cli = SchemaRegistryClient(["https://someone:something@cfg1:8080"],
                           ca_cert = "/db/cert.pem")
sch0 = cli.get_schema(_id0)
sch1 = cli.get_schema(_id1)

r0 = Schema.from_ldms_set(s0)
r1 = Schema.from_ldms_set(s1)

assert(sch0.compatible(r0))
assert(sch1.compatible(r1))
assert(not sch0.compatible(r1))
assert(not sch1.compatible(r0))

print("OK")
