#!/usr/bin/python3

import re
import os
import sys
import json

_usage = """\
USAGE:
   cfgobj_ref_log_extract.py LDMSD_LOG_FILE
"""

if len(sys.argv) != 2:
    print(_usage)
    sys.exit(-1)

try:
    f = open(sys.argv[1])
except:
    print(f"ERROR: Cannot open file: {sys.argv[1]}")
    print(_usage)
    sys.exit(-1)

lines = f.readlines()

R = re.compile(r"""
        ^
        .*\ ldmsd_cfgobj_ref_debug:\ cfgobj
        \ (?:
            (?: # ref_init group
              ref_init:\ (?P<ref_init_addr>0x[0-9a-f]+)
              \ (?P<ref_init_type>\w+)
              \ \( (?P<ref_init_name>.+) \)
            )|
            (?: # ref_free group
              ref_free:\ (?P<ref_free_addr>0x[0-9a-f]+)
            )
          )
        $
    """, re.VERBOSE)

recs = list()

for l in lines:
    l = l.strip()
    m = R.match(l)
    if m is None:
        continue
    ref_init_addr, ref_init_type, ref_init_name, ref_free_addr = m.groups()
    if ref_init_addr is not None:
        recs.append( ( "ref_init", ref_init_addr, ref_init_type, ref_init_name) )
    elif ref_free_addr is not None:
        recs.append( ( "ref_free", ref_free_addr, None, None) )
print(json.dumps(recs, indent=1))
