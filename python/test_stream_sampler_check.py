#!/usr/bin/python3

import os
import io
import sys
import pdb
import json

fin = sys.argv[1]
fout = sys.argv[2]
count = int(sys.argv[3])

def read_msg(_file):
    """Read a message "\x01...\x03" from `_file` file handle"""
    pos = _file.tell()
    sio = io.StringIO()
    c = _file.read(1)
    if not c:
        raise RuntimeError("End of file")
    if c != "\x01":
        _file.seek(pos)
        raise RuntimeError("not a start of message")
    c = _file.read(1)
    while c and c != "\x02":
        sio.write(c)
        c = _file.read(1)
    if c != "\x02":
        _file.seek(pos)
        raise RuntimeError("Bad message header")
    _type = sio.getvalue()
    sio = io.StringIO() # reset sio
    c = _file.read(1)
    while c and c != "\x03":
        sio.write(c)
        c = _file.read(1)
    if c != "\x03":
        _file.seek(pos)
        raise RuntimeError("incomplete message")
    text = sio.getvalue()
    text = text.strip('\x00')
    obj = None
    if _type == "json":
        obj = json.loads(text)
    return { "type": _type, "text": text, "obj": obj}

fo = open(fout)
fi = open(fin)
in_str = fi.read()
try:
    in_obj = json.loads(in_str)
except:
    in_obj = None

for i in range(0, count):
    m = read_msg(fo)
    if m["type"] == "json":
        if in_obj != m["obj"]:
            raise RuntimeError("out_obj({}) != in_obj".format(i))
    if m["type"] == "string":
        if in_str != m["text"]:
            raise RuntimeError("out_str({}) != in_str".format(i))

# Check if fo is depleted
pos = fo.tell()
end = fo.seek(0, 2)
if pos != end:
    raise RuntimeError("output file has more data than expected.")
