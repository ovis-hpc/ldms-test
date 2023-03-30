#!/usr/bin/python3
#
# pypubsub.py - LDMS stream publisher / subscriber interactive script for using
#               in ldmsd_stream_test
from ovis_ldms import ldms
import socket

ldms.init(128 * 1024 * 1024)

scli = ldms.StreamClient('.*', is_regex = True)

r = ldms.Xprt("sock")
r.connect(host=socket.gethostname(), port=10000)
