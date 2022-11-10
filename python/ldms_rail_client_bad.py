#!/usr/bin/python3

import os
import sys
import json
import threading

from ovis_ldms import ldms
from ldms_rail_common import SCHEMA, verify_set, xprt_pool_idx

RAIL_EPS = 8

ldms.init(16*1024*1024)
r = ldms.Xprt(auth="munge", rail_eps = RAIL_EPS)
r.connect("node-1", 10000)
