#!/usr/bin/python3

import os
import sys
from ovis_ldms import ldms

x = ldms.Xprt("sock")
x.connect("localhost", 10000)
dirs = x.dir()
schemas = set()
for d in dirs:
    s = f"{d.schema_name}_{d.digest_str.lower()[:7]}"
    schemas.add(s)
for s in schemas:
    print(s)
