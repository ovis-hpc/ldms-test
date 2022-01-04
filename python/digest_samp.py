#!/usr/bin/python3
#
# This script is meant to be used only by `ldms_schema_digest_test` script.

import os
import sys
import time
import argparse

from ovis_ldms import ldms

ldms.init(16*1024*1024)

class Global(object): pass
G = Global()

p = argparse.ArgumentParser()
p.add_argument("--port", "-p", type=int)
p.add_argument("--schema-number", "-S", type=int)
args = p.parse_args()

x = ldms.Xprt(name="sock")
def listen_cb(ep, ev, arg):
    G.ep = ep
    G.ev = ev

SCHEMA = ldms.Schema(
            name = "schema",
            metric_list = [
                ( "component_id", "u64", 1 ),
                (       "job_id", "u64", 1 ),
                (       "app_id", "u64", 1 ),
                (           "m0", "u32", 1 ),
                (           "m1", "u32", 1 ),
            ],
         )

if args.schema_number == 2:
    SCHEMA.add_metric("m2", "u32")
_set = ldms.Set("port_{}".format(args.port), SCHEMA)
_set.publish()
rc = x.listen(host="0.0.0.0", port=args.port, cb=listen_cb)

while True:
    time.sleep(1)
