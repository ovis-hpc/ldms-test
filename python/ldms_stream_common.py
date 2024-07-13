#!/usr/bin/python3

import os
import socket

from ovis_ldms import ldms

RAIL_EPS = 8
HOSTNAME = socket.gethostname()

class Global(object): pass
G = Global()

def stream_connect(host, port = 411, cb=None, cb_arg=None, rail_eps=4):
    """stream_connect(host, port=411, cb=None, cb_arg=None)"""
    r = ldms.Xprt(name = "sock", auth = "munge", rail_eps = rail_eps,
                  rail_recv_limit = 128 )
    r.connect(host, port, cb, cb_arg)
    return r
