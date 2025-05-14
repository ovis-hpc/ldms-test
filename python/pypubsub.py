#!/usr/bin/python3
#
# pypubsub.py - LDMS stream publisher / subscriber interactive script for using
#               in ldmsd_stream_test
import os

from ovis_ldms import ldms
import socket

ldms.init(128 * 1024 * 1024)

scli = ldms.MsgClient('.*', is_regex = True)

PORT = int(os.environ.get("LDMSD_PORT", "10000"))

events = list()
def cb_fn(x, ev, arg):
    events.append(ev)

r = ldms.Xprt("sock")
r.connect(host=socket.gethostname(), port=PORT, cb = cb_fn)
