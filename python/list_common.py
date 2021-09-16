#!/usr/bin/python3

import os
import io
import sys
import time
import socket
import logging

from ovis_ldms import ldms

class Global(object): pass
G = Global()

class Seq(object):
    def __init__(self, start):
        self.x = start - 1
    def next(self):
        self.x += 1
        return self.x

ARRAY_CARD = 3
HOSTNAME = socket.gethostname()
SCHEMA = ldms.Schema("test", array_card=ARRAY_CARD, metric_list = [
        ("x", ldms.V_S64),
        ("y", ldms.V_S64),
        ("z", ldms.V_S64),
        ("list", ldms.V_LIST, 2048),
        ("dir",  ldms.V_LIST, 4096),
    ])

def char(i):
    """Convert int i to char (a single character str)"""
    return bytes([i]).decode()

def add_set(name, array_card=1):
    SCHEMA.set_array_card(array_card)
    _set = ldms.Set(name, SCHEMA)
    _set.publish()
    return _set

def gen_data(seed):
    seq = Seq(seed)
    return [
        (ldms.V_S64, seq.next()), # x
        (ldms.V_S64, seq.next()), # y
        (ldms.V_S64, seq.next()), # z
        (ldms.V_LIST, [ # list
            (ldms.V_CHAR, [ 'a', 'b' ][seq.next()%2]), # char, 'a' or 'b'
            (ldms.V_U8, seq.next()), # u8
            (ldms.V_S8, seq.next()), # s8
            (ldms.V_U16, seq.next()), # u16
            (ldms.V_S16, seq.next()), # s16
            (ldms.V_U32, seq.next()), # u32
            (ldms.V_S32, seq.next()), # s32
            (ldms.V_U64, seq.next()), # u64
            (ldms.V_S64, seq.next()), # s64
            (ldms.V_F32, seq.next()), # float
            (ldms.V_D64, seq.next()), # double
            (ldms.V_CHAR_ARRAY, str(seq.next())), # str
            (ldms.V_U8_ARRAY,  tuple(seq.next() for i in range(3)) ), # u8
            (ldms.V_S8_ARRAY,  tuple(seq.next() for i in range(3)) ), # s8
            (ldms.V_U16_ARRAY, tuple(seq.next() for i in range(3)) ), # u16
            (ldms.V_S16_ARRAY, tuple(seq.next() for i in range(3)) ), # s16
            (ldms.V_U32_ARRAY, tuple(seq.next() for i in range(3)) ), # u32
            (ldms.V_S32_ARRAY, tuple(seq.next() for i in range(3)) ), # s32
            (ldms.V_U64_ARRAY, tuple(seq.next() for i in range(3)) ), # u64
            (ldms.V_S64_ARRAY, tuple(seq.next() for i in range(3)) ), # s64
            (ldms.V_F32_ARRAY, tuple(seq.next() for i in range(3)) ), # float
            (ldms.V_D64_ARRAY, tuple(seq.next() for i in range(3)) ), # double
        ]),
        (ldms.V_LIST, [ # dir
            (ldms.V_CHAR_ARRAY, "/"),
            (ldms.V_LIST, [
                (ldms.V_CHAR_ARRAY, "bin/"), # dir name
                (ldms.V_LIST, [   # dir content
                    (ldms.V_CHAR_ARRAY, "bash"), # file
                    (ldms.V_CHAR_ARRAY, "ls"),   # file
                ]),
                (ldms.V_CHAR_ARRAY, "var/"), # dir name
                (ldms.V_LIST, [
                    (ldms.V_CHAR_ARRAY, "run/"), # dir name
                    (ldms.V_LIST, [
                        (ldms.V_CHAR_ARRAY, "sshd.pid"),
                        (ldms.V_CHAR_ARRAY, "lock/"),
                        (ldms.V_LIST, [
                            (ldms.V_CHAR_ARRAY, "file")
                        ]),
                    ]),
                    (ldms.V_CHAR_ARRAY, "log/"), # dir name
                    (ldms.V_LIST, [
                        (ldms.V_CHAR_ARRAY, "sshd.log"),
                    ]),
                ]),
            ]),
        ]),
    ]


def list_append(mlst, data):
    # mlst is MetricList object
    # data is [ (type, obj) ]
    for t, v in data:
        o = mlst.append(t, v)
        if t == ldms.V_LIST:
            list_append(o, v)

def list_update(mlst, data):
    for m, (t, v) in zip(mlst, data):
        m.set(v)

def update_set(_set, i):
    x, y, z, lst, dr = gen_data(i)
    _set.transaction_begin()
    _set['x'] = x[1]
    _set['y'] = y[1]
    _set['z'] = z[1]
    mlst = _set['list']
    mdr  = _set['dir']
    if len(mlst): # only update the values if list has been populated
        list_update(mlst, lst[1])
    else:
        list_append(mlst, lst[1])
    # append `dir` values
    if len(mdr) == 0:
        list_append(mdr, dr[1])
    _set.transaction_end()

def print_list(l, indent=4, _file=sys.stdout):
    spc = " " * (indent - 1)
    print(spc, "{")
    for v in l:
        if type(v) == ldms.MetricList:
            print_list(v, indent+2, _file)
        else:
            print(spc, v, file=_file)
    print(spc, "}")

def print_set(s):
    print(s.name)
    n = len(s)
    for k, v in s.items():
        if type(v) == ldms.MetricList:
            print("  {}:".format(k))
            print_list(v)
            continue
        print("  {}: {}".format(k, v))

def verify_value(t, m, v):
    if t == ldms.V_CHAR:
        if type(m) == ldms.MVal:
            m = m.get()
        m = bytes([m]).decode()
        # print("v:", v, "m:", m)
        assert(m == v)
    elif t == ldms.V_LIST:
        verify_list(m, v)
    else:
        # print("v:", v, "m:", m)
        if type(m) == ldms.MVal:
            m = m.get()
        if type(m) is tuple:
            v = tuple(v) # convert to tuple for comparison
        assert( m == v )

def verify_list(l, d):
    for (t, v), m in zip(d, iter(l)):
        verify_value(t, m, v)

def verify_set(s, data=None):
    """Verify the data in the set `s` and raise on verification error.

    If this function finished with no exception raised, the set is verified.
    """
    if not s.is_consistent:
        raise ValueError("set `{}` is not consistent".format(s.name))
    seed = s[0]
    if data is None:
        data = gen_data(seed)
    for (t, v), (k, m) in zip(data, s.items()):
        verify_value(t, m, v)

class DirMetricList(object):
    """Python class wrapping MetricList that represents directory structure"""
    def __init__(self, name_val, mlist):
        self.name_val = name_val
        self.mlist = mlist
        self.dlist = dict()
        prev = None
        for curr in mlist:
            if type(curr) == ldms.MetricList:
                assert(prev != None)
                self.dlist[str(prev)] = DirMetricList(prev, curr)
                prev = None
            else:
                if prev is not None:
                    self.dlist[str(prev)] = prev
                prev = curr
        if prev is not None:
            self.dlist[str(prev)] = prev

    def __getitem__(self, k):
        return self.dlist[k]

    def name(self):
        return str(self.name_val)

    def keys(self):
        return self.dlist.keys()

    def items(self):
        return self.dlist.items()

    def values(self):
        return self.dlist.values()

    def delete(self, key):
        m = self.dlist.pop(key)
        if type(m) == DirMetricList:
            # recursively delete elements
            _keys = list(m.keys())
            for k in _keys:
                m.delete(k)
            self.mlist.delete(m.mlist)
            self.mlist.delete(m.name_val)
        else:
            self.mlist.delete(m)

    def __str__(self):
        sio = io.StringIO()
        print_list(self.mlist, _file=sio)
        return sio.getvalue()
