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

        ("list_char", ldms.V_LIST, 256),
        ("list_u8", ldms.V_LIST, 256),
        ("list_s8", ldms.V_LIST, 256),
        ("list_u16", ldms.V_LIST, 256),
        ("list_s16", ldms.V_LIST, 256),
        ("list_u32", ldms.V_LIST, 256),
        ("list_s32", ldms.V_LIST, 256),
        ("list_u64", ldms.V_LIST, 256),
        ("list_s64", ldms.V_LIST, 256),
        ("list_f32", ldms.V_LIST, 256),
        ("list_d64", ldms.V_LIST, 256),

        ("list_char_array", ldms.V_LIST, 256),
        ("list_u8_array", ldms.V_LIST, 256),
        ("list_s8_array", ldms.V_LIST, 256),
        ("list_u16_array", ldms.V_LIST, 256),
        ("list_s16_array", ldms.V_LIST, 256),
        ("list_u32_array", ldms.V_LIST, 256),
        ("list_s32_array", ldms.V_LIST, 256),
        ("list_u64_array", ldms.V_LIST, 256),
        ("list_s64_array", ldms.V_LIST, 256),
        ("list_f32_array", ldms.V_LIST, 256),
        ("list_d64_array", ldms.V_LIST, 256),
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

        # [a, b, a] or [b, a, b]
        (ldms.V_LIST, [  (ldms.V_CHAR, 'ab'[seq.next()%2]) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U8, seq.next() % (1<<8)) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S8, (seq.next() % (1<<8)) - (1<<7)) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U16, seq.next() % (1<<16)) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S16, (seq.next() % (1<<16)) - (1<<15)) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U32, seq.next() % (1<<32)) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S32, (seq.next() % (1<<32)) - (1<<31)) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U64, seq.next() % (1<<64)) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S64, (seq.next() % (1<<64)) - (1<<63)) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_F32, seq.next()) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_D64, seq.next()) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_CHAR_ARRAY, 'str'+('ab'[seq.next()%2])) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U8_ARRAY, tuple(seq.next() % (1<<8) for j in range(3) )) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S8_ARRAY, tuple((seq.next() % (1<<8)) - (1<<7) for j in range(3) )) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U16_ARRAY, tuple(seq.next() % (1<<16) for j in range(3) )) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S16_ARRAY, tuple((seq.next() % (1<<16)) - (1<<15) for j in range(3) )) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U32_ARRAY, tuple(seq.next() % (1<<32) for j in range(3) )) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S32_ARRAY, tuple((seq.next() % (1<<32)) - (1<<31) for j in range(3) )) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_U64_ARRAY, tuple(seq.next() % (1<<64) for j in range(3) )) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_S64_ARRAY, tuple((seq.next() % (1<<64)) - (1<<63) for j in range(3) )) for i in range(3) ]),

        (ldms.V_LIST, [  (ldms.V_F32_ARRAY, tuple((seq.next() for j in range(3)))) for i in range(3) ]),
        (ldms.V_LIST, [  (ldms.V_D64_ARRAY, tuple((seq.next() for j in range(3)))) for i in range(3) ]),

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
    data = gen_data(i)
    x, y, z = data[:3]

    _set.transaction_begin()
    _set['x'] = x[1]
    _set['y'] = y[1]
    _set['z'] = z[1]

    for md, dd in zip( SCHEMA[3:], data[3:] ):
        mlst = _set[ md.name ]
        if mlst:
            # mlist has been populated, just update the elements
            list_update(mlst, dd[1])
        else:
            # mlist is empty, add elements
            list_append(mlst, dd[1])
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
