#!/usr/bin/python3
#
# SYNOPSIS
# --------
# ldms_msg_client.py - msg client script (interactive) for using in `ldms_msg_test`.

from ovis_ldms import ldms
from ldms_msg_common import *

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

def get_data(client, _list):
    d = client.get_data()
    if d is not None:
        _list.append(d)
    return d

def msg_cb(client, sdata, arg):
    _list = arg
    _list.append(sdata)

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

r = msg_connect("node-1")
r.msg_subscribe('.*', True)
