#!/usr/bin/python3
#
# SYNOPSIS
# --------
# ldms_stream_client.py - stream client script (interactive) for using in `ldms_stream_test`.

from ovis_ldms import ldms
from ldms_stream_common import *

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

def get_data(client, _list):
    d = client.get_data()
    if d is not None:
        _list.append(d)
    return d

def stream_cb(client, sdata, arg):
    _list = arg
    _list.append(sdata)

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

r = stream_connect("node-1")
r.stream_subscribe('.*', True)
