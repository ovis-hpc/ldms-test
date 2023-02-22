#!/usr/bin/python3
#
# SYNOPSIS
# --------
# ldms_stream_server.py - stream server script (interactive) for using in `ldms_stream_test`.

from ovis_ldms import ldms
from ldms_stream_common import *

ldms.init(64*1024*1024)
ldms.stream_stats_level_set(2)
xlist = list()

xprt_free_list = list()
def xprt_free_cb(x):
    global xprt_free_list
    xprt_free_list.append(str(x))

def listen_fn(x, ev, arg):
    global xlist
    if ev.type == ldms.EVENT_CONNECTED:
        x.set_xprt_free_cb(xprt_free_cb)
        xlist.append(x)
    elif ev.type == ldms.EVENT_DISCONNECTED:
        xlist.remove(x)

r = ldms.Xprt(name = "sock", auth = "munge", rail_eps = 4, rail_recv_limit = 128 )
r.listen(cb = listen_fn, cb_arg = None)

# setting up local stream clients

dot_star_cb_data = list()
l1_cb_data = list()
l2_cb_data = list()
l3_cb_data = list()
dot_star_stream_cb_data = list()

dot_star_data = list()
l1_data = list()
l2_data = list()
l3_data = list()
dot_star_stream_data = list()

def stream_cb(client, sdata, arg):
    _list = arg
    _list.append(sdata)

def get_data(client, _list):
    d = client.get_data()
    if d is not None:
        _list.append(d)
    return d

def compare_data():
    """Compare async and blocking data"""
    assert( dot_star_cb_data == dot_star_data )
    assert( l1_cb_data == l1_data )
    assert( l2_cb_data == l2_data )
    assert( l3_cb_data == l3_data )
    assert( dot_star_stream_cb_data == dot_star_stream_data )
    return True

# clients without and with callback
dot_star = ldms.StreamClient('.*', is_regex = True)
dot_star_cb = ldms.StreamClient('.*', is_regex = True, cb = stream_cb, cb_arg = dot_star_cb_data)
l1 = ldms.StreamClient('l1-stream', is_regex = False)
l1_cb = ldms.StreamClient('l1-stream', is_regex = False, cb = stream_cb, cb_arg = l1_cb_data)
l2 = ldms.StreamClient('l2-stream', is_regex = False)
l2_cb = ldms.StreamClient('l2-stream', is_regex = False, cb = stream_cb, cb_arg = l2_cb_data)
l3 = ldms.StreamClient('l3-stream', is_regex = False)
l3_cb = ldms.StreamClient('l3-stream', is_regex = False, cb = stream_cb, cb_arg = l3_cb_data)
dot_star_stream = ldms.StreamClient('.*-stream', is_regex = True)
dot_star_stream_cb = ldms.StreamClient('.*-stream', is_regex = True,
                        cb = stream_cb, cb_arg = dot_star_stream_cb_data)

A = int(HOSTNAME[-1])
G.prdcrs = []
if A > 3:
    L = 0
    R = 0
else:
    L = A * 2
    R = L + 1

def right_connect():
    if not R:
        return None
    try:
        con = stream_connect(f"node-{R}")
        con.set_xprt_free_cb(xprt_free_cb)
    except:
        return None
    if A == 1:
        con.stream_subscribe(".*-stream", True)
    elif A == 2:
        con.stream_subscribe("l2-stream", False)
        con.stream_subscribe("l3-stream", False)
    elif A == 3:
        con.stream_subscribe(".*-stream", True)
    else:
        raise RuntimeError(f"Invalid node ID {A}")
    return con

def left_connect():
    if not L:
        return None
    try:
        con = stream_connect(f"node-{L}")
        con.set_xprt_free_cb(xprt_free_cb)
    except:
        return None
    if A == 1:
        con.stream_subscribe("l3-stream", False)
    elif A == 2:
        con.stream_subscribe("l2-stream", False)
        con.stream_subscribe("l3-stream", False)
    elif A == 3:
        con.stream_subscribe(".*-stream", True)
    else:
        raise RuntimeError(f"Invalid node ID {A}")
    return con

def server_connect_routine():
    if A > 3:
        return # this is the leaf
    pl = left_connect()
    pr = right_connect()
    G.prdcrs = [pl, pr]


server_connect_routine()
