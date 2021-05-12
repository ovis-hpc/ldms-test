#!/usr/bin/python3

import os
import sys

LEAD_FMT='EVENT:{{"type":"{0}","size":{1},"event":'

def consume(f, e, lead_str):
    """Consume expected string `e` from the file `f`"""
    # Consume lead string first
    s = f.read(len(lead_str))
    if len(s) != len(lead_str):
        raise RuntimeError("Expecting more data, but EOF is reached.")
    assert(s == lead_str)
    s = f.read(len(e))
    if len(s) != len(e):
        raise RuntimeError("Expecting more data, but EOF is reached.")
    assert(s == e)
    s = f.read(2) # '}\n'
    if len(s) != 2:
        raise RuntimeError("Expecting more data, but EOF is reached.")
    assert(s == '}\n')

fout = sys.argv[1]
fin = sys.argv[2]
t = sys.argv[3]
count = int(sys.argv[4])

fi = open(fin)
fo = open(fout)
in_str = fi.read()
lead_str = LEAD_FMT.format(t, len(in_str)+1) # '\0' terminated

for i in range(0, count):
    consume(fo, in_str, lead_str)

# Check if fo is depleted
pos = fo.tell()
end = fo.seek(0, 2)
if pos != end:
    raise RuntimeError("output file has more data than expected.")
