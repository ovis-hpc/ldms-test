#!/usr/bin/python3

import os
import sys
import json

from ldmsdutils import *

ldms.init(1024*1024*1024)

def good_dirs(dirs):
    return [ d for d in dirs if d.flags[0] == 'C' ]

def ready_sets(prdcr_set_status_resp):
    resp = prdcr_set_status_resp
    return [ s for s in resp['sets'] if s['state'] == 'READY' ]

def do_dir(daemon, label = None):
    _dirs = daemon.dir()
    _good = good_dirs(_dirs)
    print(label)
    print("  dirs: {}".format(len(_dirs)))
    print("  good: {}".format(len(_good)))
    return (_dirs, _good)

samp_list = LDMSDSampler.allDaemons()
l1_list = LDMSD_L1.allDaemons()
l2_list = LDMSD_L2.allDaemons()
l3_list = LDMSD_L3.allDaemons()
aggs = l1_list + l2_list + l3_list
daemons = { a.name: a for a in samp_list + l1_list + l2_list + l3_list }

if True:
    samp = samp_list[0]
    samp_dirs, samp_good = do_dir(samp, "samp")
    if False:
        samp.lookup()
        samp.update()

if True:
    l1 = l1_list[0]
    l1_dirs, l1_good = do_dir(l1, "L1")
    if False:
        l1.lookup()
        l1.update()

if True:
    l2 = l2_list[0]
    l2_dirs, l2_good = do_dir(l2, "L2")
    if False:
        l2.lookup()
        l2.update()
