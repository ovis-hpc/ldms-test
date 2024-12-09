#!/usr/bin/python3
#
# SYNOPSIS
# --------
# ldms_stream_publish.py - stream publisher script for using in `ldms_stream_test`

import os
import sys
import socket

from ovis_ldms import ldms
from ldms_stream_common import *

# forger
def forge_msg(uid:int, gid:int):
    u0, u1 = uid.to_bytes(2, "big")
    g0, g1 = gid.to_bytes(2, "big")
    msg = bytes([
        0,       2,       0,       0,       0,       0,       0,       137,
        4,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       0,       0,       0,       0,       113,
        0,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       11,      0,       0,       0,       113,
        0,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       0,       2,       0,       0,       0,
        0,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       0,       85,      85,      0,       0,
        0,       0,       0,       0,       0,       0,       0,       0,
        0,       0,       0,       9,       0,       0,       0,       0,
        0,       0,       u0,      u1,      0,       0,       g0,      g1,
        0,       0,       1,       36,      216,     92,      13,      166,
        110,     97,      109,     101,     0,       100,     97,      116,
        97,
    ])
    return msg

r = stream_connect(HOSTNAME, rail_eps = 1)
