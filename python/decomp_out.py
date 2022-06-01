#!/bin/python3
#
# This script is meant to run inside an ldms-container to get schema or rows
# from the decomposition output (sos, csv or kafka dump). This script is used by
# `ldmsd_decomp_test` script.
#
#
# Output:
# - first row is the schema description:
#   - {
#       "cols": [ "col_names", ... ],
#       "indices": [
#          { "name": "index_name", "cols": [ "col_names", ... ] },
#          ...
#       ]
#     }
# - other rows are the row data (array in JSON format)
#   - [ row1_col1, row1_col2, ... ]

import os
import re
import pdb
import sys
import json
import argparse

KEY = re.compile('"([^"]+)":')

def kafka_proc(args):
    # returns ( SCHEMA, OBJS ) tuples
    f = open(args.path)
    lines = [ l for l in f ]
    keys = KEY.findall(lines[0])
    schema_desc = { "cols" : keys, "indices": [ ] }
    print(json.dumps(schema_desc))
    for l in lines:
        obj = json.loads(l)
        assert( set(obj.keys()) == set(keys) )
        cols = [ obj[k] for k in keys ]
        print(json.dumps(cols))

def sos_proc(args):
    from sosdb import Sos as sos
    import numpy
    cont = sos.Container(args.path)
    sch = cont.schema_by_name(args.schema)
    attr = sch["time_comp"]
    cols = list()
    idxs = list()
    for s in sch:
        name = s.name()
        idx_cols = s.join_list()
        if idx_cols is None:
            cols.append(name)
        if s.is_indexed():
            if idx_cols is not None:
                idx = { "name": name, "cols": [ sch[int(i)].name() for i in idx_cols ] }
            else:
                idx = { "name": name, "cols": [ name ] }
            idxs.append(idx)
    schema_desc = { "cols" : cols, "indices": idxs }
    print(json.dumps(schema_desc))
    N = sch.attr_count()
    itr = attr.attr_iter()
    b = itr.begin()
    while b:
        o = itr.item()
        row = list()
        for c in cols:
            v = o[c]
            t = sch[c]
            if t.type() == sos.TYPE_TIMESTAMP:
                v = v[0] + v[1] * 1e-6
            elif type(v) == numpy.ndarray:
                v = v.tolist()
            row.append(v)
        print(json.dumps(row))
        b = itr.next()

def csv_col_ts_usec(row, v):
    # no op
    pass

def val(v):
    try:
        return int(v)
    except:
        try:
            return float(v)
        except:
            return v

def csv_atomic(row, v):
    row.append(val(v))

def csv_array_first(row, v):
    row.append([ val(v) ])

def csv_array_append(row, v):
    row[-1].append(val(v))

def prefix(s0, s1):
    p = ""
    for a, b in zip(s0, s1):
        if a != b:
            break
        p += a
    return (p, s0.replace(p, "", 1), s1.replace(p, "", 1))

def is_num(v):
    try:
        i = int(v)
        return True
    except:
        return False

def csv_proc(args):
    # returns ( SCHEMA, OBJS ) tuples
    f = open(args.path)
    h = f.readline().strip()[1:].split(",")
    prev_col = ""
    nh = h[1:] + [ "" ]
    acts = list()
    cols = []
    for c, next_col in zip(h, nh):
        if c == "Time":
            acts.append(csv_atomic)
            cols.append("Time")
        elif c == "Time_usec":
            acts.append(csv_col_ts_usec)
        else:
            p, a, b = prefix(c, next_col)
            if p and a == "0":
                cols.append(p)
                acts.append(csv_array_first)
            else:
                p, a, b = prefix(prev_col, c)
                if p == cols[-1] and is_num(b):
                    acts.append(csv_array_append)
                else:
                    acts.append(csv_atomic)
                    cols.append(c)
        prev_col = c
    schema_desc = { "cols": cols, "indices": [ ] }
    print(json.dumps(schema_desc))
    for l in f:
        row = list()
        vals = l.strip().split(",")
        for v, a in zip(vals, acts):
            a(row, v)
        print(json.dumps(row))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Decomp Output Parser")
    ap.add_argument("--store", required=True, help="sos, csv, or kafka")
    ap.add_argument("--path", required=True, help="path to sos container, file")
    ap.add_argument("--schema", required=False, help="schema name (for sos)")
    args = ap.parse_args()
    proc_tbl = {
        "sos": sos_proc,
        "csv": csv_proc,
        "kafka": kafka_proc,
    }
    proc = proc_tbl.get(args.store)
    if not proc:
        print(f"Unknown store: {args.store}")
        sys.exit(-1)
    proc(args)
