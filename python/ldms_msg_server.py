#!/usr/bin/python3
#
# SYNOPSIS
# --------
# ldms_msg_server.py - msg server script (interactive) for using in `ldms_msg_test`.

from ovis_ldms import ldms
from ldms_msg_common import *

ldms.init(64*1024*1024)
ldms.msg_stats_level_set(2)
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

# setting up local msg clients

dot_star_cb_data = list()
l1_cb_data = list()
l2_cb_data = list()
l3_cb_data = list()
dot_star_msg_cb_data = list()

dot_star_data = list()
l1_data = list()
l2_data = list()
l3_data = list()
dot_star_msg_data = list()

def msg_cb(client, sdata, arg):
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
    assert( dot_star_msg_cb_data == dot_star_msg_data )
    return True

# clients without and with callback
dot_star = ldms.MsgClient('.*', is_regex = True)
dot_star_cb = ldms.MsgClient('.*', is_regex = True, cb = msg_cb, cb_arg = dot_star_cb_data)
l1 = ldms.MsgClient('l1-msg', is_regex = False)
l1_cb = ldms.MsgClient('l1-msg', is_regex = False, cb = msg_cb, cb_arg = l1_cb_data)
l2 = ldms.MsgClient('l2-msg', is_regex = False)
l2_cb = ldms.MsgClient('l2-msg', is_regex = False, cb = msg_cb, cb_arg = l2_cb_data)
l3 = ldms.MsgClient('l3-msg', is_regex = False)
l3_cb = ldms.MsgClient('l3-msg', is_regex = False, cb = msg_cb, cb_arg = l3_cb_data)
dot_star_msg = ldms.MsgClient('.*-msg', is_regex = True)
dot_star_msg_cb = ldms.MsgClient('.*-msg', is_regex = True,
                        cb = msg_cb, cb_arg = dot_star_msg_cb_data)

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
        con = msg_connect(f"node-{R}")
        con.set_xprt_free_cb(xprt_free_cb)
    except:
        return None
    if A == 1:
        con.msg_subscribe(".*-msg", True)
    elif A == 2:
        con.msg_subscribe("l2-msg", False)
        con.msg_subscribe("l3-msg", False)
    elif A == 3:
        con.msg_subscribe(".*-msg", True)
    else:
        raise RuntimeError(f"Invalid node ID {A}")
    return con

def left_connect():
    if not L:
        return None
    try:
        con = msg_connect(f"node-{L}")
        con.set_xprt_free_cb(xprt_free_cb)
    except:
        return None
    if A == 1:
        con.msg_subscribe("l3-msg", False)
    elif A == 2:
        con.msg_subscribe("l2-msg", False)
        con.msg_subscribe("l3-msg", False)
    elif A == 3:
        con.msg_subscribe(".*-msg", True)
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
