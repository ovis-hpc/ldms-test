#!/usr/bin/python3
import os
import sys
import pdb
import json
import time
import socket
import argparse

from ovis_ldms import ldms

from maestro.schema_registry import SchemaRegistryClient, Schema

p = argparse.ArgumentParser(description = "Schema Registry Client test script")
p.add_argument("--urls", "-U", nargs="+", metavar="URLs",
               help="Schema Registry URLs")
p.add_argument("--ca-cert", "-C", metavar="PATH",
               help="CA certificate file")
p.add_argument("--add", "-a", metavar="JSON_PATH", help="Add schema")
p.add_argument("--delete", "--del", "-d", metavar="SCHEMA_ID", help="Add schema")
p.add_argument("--list-names", "-N", action="store_true",
               help="List schema by name")
p.add_argument("--list-digests", "-D", action="store_true",
               help="List schema by digest")
p.add_argument("--xprt", "-x", metavar="XPRT[:PORT[:ADDR]]",
               help="XPRT to listen to (optionally restricted to specified PORT/ADDR)")
p.add_argument("--set-schema", "-s", metavar="SCHEMA_ID",
               help="Schema ID for creating a test set (served over --xprt)")


args = p.parse_args()

cli = SchemaRegistryClient(urls = args.urls, ca_cert = args.ca_cert)

if args.add:
    f = open(args.add)
    obj = json.load(f)
    sch = Schema.from_dict(obj)
    resp = cli.add_schema(sch)
    print(resp.text)

if args.delete:
    resp = cli.delete_schema(args.delete)
    print(resp.text)

if args.list_names:
    objs = dict()
    names = cli.list_names()
    for name in names:
        objs[name] = cli.list_versions(name = name)
    _str = json.dumps(objs, indent=1)
    print(_str)

if args.list_digests:
    objs = dict()
    digests = cli.list_digests()
    for digest in digests:
        objs[digest] = cli.list_versions(digest = digest)
    _str = json.dumps(objs, indent=1)
    print(_str)

if args.xprt:
    if not args.set_schema:
        print("-s SCHEMA_ID is required for 'listening' mode")
        sys.exit(-1)
    ldms.init(16*1024*1024)
    sch = cli.get_schema(args.set_schema)
    hname = socket.gethostname()
    lset = ldms.Set(f"{hname}/{sch.name}", sch.as_ldms_schema())
    lset.publish()
    tkns = args.xprt.split(":", 2) + [None]*2 # make sure it has at least 3 elements
    xprt, port, host = tkns[:3]
    kwargs = dict()
    if port:
        kwargs["port"] = port
    if host:
        kwargs["host"] = host
    x = ldms.Xprt(xprt)
    x.listen(**kwargs)
    while True:
        lset.transaction_begin()
        lset.transaction_end()
        time.sleep(1)
