#!/usr/bin/python

import os
import sys
import pwd
from LDMS_Test import LDMSDCluster, jprint, deep_copy

if __name__ != "__main__":
    raise RuntimeError("Do not import this! (not a dmodule)")

USER = pwd.getpwuid(os.geteuid())[0]
CNAME = USER + "-test_get_create"

spec0 = {
    "name": CNAME,
    "nodes": [
        {
            "hostname": "node-1",
            "daemons": [
                { "name": "sshd", "type": "sshd" },
            ],
        },
    ],
}

print "Creating/Getting a cluster"
cluster0 = LDMSDCluster.get(CNAME, create = True, spec = spec0)

spec1 = deep_copy(spec0)
spec1["nodes"].append({ "hostname": "node-2"})

print "Checking get() with same spec ...",
cluster00 = LDMSDCluster.get(CNAME, create = True, spec = spec0)
print "OK"

print "Checking get() with modified spec...",
try:
    cluster1 = LDMSDCluster.get(CNAME, create = True, spec = spec1)
except RuntimeError, e:
    assert(e.message == "spec mismatch")
    print "OK (get correct exception)"
else:
    raise RuntimeError("Expecting an exception")

cluster0.remove()
