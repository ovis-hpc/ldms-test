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

SET_BUFFER = 3
ITEM_COUNT = 3
ARRAY_COUNT = 8
HOSTNAME = socket.gethostname()

REC_DEF = ldms.RecordDef("device_record", metric_list = [
        (       "LDMS_V_CHAR",       ldms.V_CHAR,           1 ),
        (         "LDMS_V_U8",         ldms.V_U8,           1 ),
        (         "LDMS_V_S8",         ldms.V_S8,           1 ),
        (        "LDMS_V_U16",        ldms.V_U16,           1 ),
        (        "LDMS_V_S16",        ldms.V_S16,           1 ),
        (        "LDMS_V_U32",        ldms.V_U32,           1 ),
        (        "LDMS_V_S32",        ldms.V_S32,           1 ),
        (        "LDMS_V_U64",        ldms.V_U64,           1 ),
        (        "LDMS_V_S64",        ldms.V_S64,           1 ),
        (        "LDMS_V_F32",        ldms.V_F32,           1 ),
        (        "LDMS_V_D64",        ldms.V_D64,           1 ),
        ( "LDMS_V_CHAR_ARRAY", ldms.V_CHAR_ARRAY, ARRAY_COUNT ),
        (   "LDMS_V_U8_ARRAY",   ldms.V_U8_ARRAY, ARRAY_COUNT ),
        (   "LDMS_V_S8_ARRAY",   ldms.V_S8_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_U16_ARRAY",  ldms.V_U16_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_S16_ARRAY",  ldms.V_S16_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_U32_ARRAY",  ldms.V_U32_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_S32_ARRAY",  ldms.V_S32_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_U64_ARRAY",  ldms.V_U64_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_S64_ARRAY",  ldms.V_S64_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_F32_ARRAY",  ldms.V_F32_ARRAY, ARRAY_COUNT ),
        (  "LDMS_V_D64_ARRAY",  ldms.V_D64_ARRAY, ARRAY_COUNT ),
    ])

SCHEMA = ldms.Schema(
            name = "schema",
            metric_list = [
                ( "component_id", "u64",  1, True ),
                (       "job_id", "u64",  1  ),
                (       "app_id", "u64",  1  ),
                (        "round", "u32",  1  ),
                REC_DEF,
                ( "device_list", "list", ITEM_COUNT * REC_DEF.heap_size() ),
                dict(
                    name    = "device_array",
                    type    = ldms.V_RECORD_ARRAY,
                    count   = ITEM_COUNT,
                    rec_def = REC_DEF,
                ),
            ],
         )

VAL = {
        ldms.V_CHAR: lambda i: bytes( [97 + (i%2)] ).decode(),
        ldms.V_U8:   lambda i: i & 0xFF,
        ldms.V_S8:   lambda i: -(i % 128),
        ldms.V_U16:  lambda i: i + 1000,
        ldms.V_S16:  lambda i: -(i + 1000),
        ldms.V_U32:  lambda i: i + 100000,
        ldms.V_S32:  lambda i: -(i + 100000),
        ldms.V_U64:  lambda i: i + 200000,
        ldms.V_S64:  lambda i: -(i + 200000),
        ldms.V_F32:  lambda i: float(i),
        ldms.V_D64:  lambda i: float(i),
        ldms.V_CHAR_ARRAY: lambda i: "a_{}".format(i),
        ldms.V_U8_ARRAY:   lambda i: [ (i+j)&0xFF for j in range(ARRAY_COUNT)],
        ldms.V_S8_ARRAY:   lambda i: [ -((i+j)%128) for j in range(ARRAY_COUNT)],
        ldms.V_U16_ARRAY:  lambda i: [ 1000+(i+j) for j in range(ARRAY_COUNT)],
        ldms.V_S16_ARRAY:  lambda i: [ -(1000+(i+j)) for j in range(ARRAY_COUNT)],
        ldms.V_U32_ARRAY:  lambda i: [ 100000+(i+j) for j in range(ARRAY_COUNT)],
        ldms.V_S32_ARRAY:  lambda i: [ -(100000+(i+j)) for j in range(ARRAY_COUNT)],
        ldms.V_U64_ARRAY:  lambda i: [ 500000+(i+j) for j in range(ARRAY_COUNT)],
        ldms.V_S64_ARRAY:  lambda i: [ -(500000+(i+j)) for j in range(ARRAY_COUNT)],
        ldms.V_F32_ARRAY:  lambda i: [ 0.5+i+j for j in range(ARRAY_COUNT)],
        ldms.V_D64_ARRAY:  lambda i: [ 0.75+i+j for j in range(ARRAY_COUNT)],
}

def char(i):
    """Convert int i to char (a single character str)"""
    return bytes([i]).decode()

def add_set(name, array_card=1):
    SCHEMA.set_array_card(array_card)
    _set = ldms.Set(name, SCHEMA)
    _set.publish()
    return _set

def gen_data(_round):
    seq = Seq(_round)
    return [
        (ldms.V_U64, 0), # comp_id
        (ldms.V_U64, 0), # job_id
        (ldms.V_U64, 0), # app_id
        (ldms.V_U32, _round), # round
        (ldms.V_RECORD_TYPE, None), # record type
        (ldms.V_LIST, [
            # list entry: <entry_type, entry_value>
            # In our case, the entry value is a record which is a collection
            # of (record_entry_type, record_entry_value).
            (
                ldms.V_RECORD_INST,
                [ (_t, VAL[_t](_round + r)) for _s, _t, _c, _mta in REC_DEF ]
            ) for r in range(ITEM_COUNT)
        ]),
        (ldms.V_RECORD_ARRAY, [
            (
                ldms.V_RECORD_INST,
                [ (_t, VAL[_t](_round + r + ITEM_COUNT)) for _s, _t, _c, _mta in REC_DEF ]
            ) for r in range(ITEM_COUNT)
        ]),
    ]

def update_set(_set, _round):
    _set.transaction_begin()
    _set["round"] = _round
    _lst = _set["device_list"]
    if len(_lst) == 0:
        # allocate records
        for i in range(ITEM_COUNT):
            _rec = _set.record_alloc("device_record")
            _lst.append(ldms.V_RECORD_INST, _rec)
    i = 0
    for rec in _lst:
        for j in range(len(rec)):
            t = rec.get_metric_type(j)
            v = VAL[t](_round + i)
            rec[j] = v
        i += 1
    _arr = _set["device_array"]
    for rec in _arr:
        for j in range(len(rec)):
            t = rec.get_metric_type(j)
            v = VAL[t](_round + i)
            rec[j] = v
        i += 1

    _set.transaction_end()

def print_set(s):
    print(s.json(indent=2))

def verify_value(t, m, v):
    if t == ldms.V_CHAR:
        if type(m) == ldms.MVal:
            m = m.get()
        if type(m) == int:
            m = bytes([m]).decode()
        # print("v:", v, "m:", m)
        assert(m == v)
    elif t == ldms.V_LIST:
        verify_list(m, v)
    elif t == ldms.V_RECORD_INST:
        verify_record_inst(m, v)
    elif t == ldms.V_RECORD_TYPE:
        pass # application don't use record type directly
    elif t == ldms.V_RECORD_ARRAY:
        verify_record_array(m, v)
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

def verify_record_array(l, d):
    for (t, v), m in zip(d, iter(l)):
        verify_value(t, m, v)

def verify_record_inst(r, d):
    # d is a list of (type, value)
    assert(type(r) == ldms.RecordInstance)
    assert(len(r) == len(d))
    for i in range(len(r)):
        rt = r.get_metric_type(i)
        rv = r[i]
        dt, dv = d[i]
        assert( rt == dt )
        verify_value(rt, rv, dv)

def verify_set(s, data=None):
    """Verify the data in the set `s` and raise on verification error.

    If this function finished with no exception raised, the set is verified.
    """
    if not s.is_consistent:
        raise ValueError("set `{}` is not consistent".format(s.name))
    seed = s["round"]
    if data is None:
        data = gen_data(seed)
    for (t, v), (k, m) in zip(data, s.items()):
        verify_value(t, m, v)
