#!/usr/bin/python

import os
import sys
import argparse

from LDMS_Test import add_common_args, get_cluster_name

execfile(os.getenv("PYTHONSTARTUP", "/dev/null"))
parser = argparse.ArgumentParser()
add_common_args(parser)

print "(test parse) ./test_common_arg.py --tada-addr somewhere"
n0 = parser.parse_args([ "--tada-addr", "somewhere" ])
assert(n0.tada_addr == "somewhere:9862")

cname = get_cluster_name(n0)
print "clustername:", cname

print "parsed args:", n0
