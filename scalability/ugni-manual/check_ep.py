#!/usr/bin/python3

import re
import os
import sys

completed = list()
posted = list()
max_posted = 0

class Entry(object):
    STATES = set(["post", "complete", "pending", "resume", "error"])
    FROM_TBL = dict(
        init     = set(["post", "pending", "error"]),
        post     = set(["complete"]),
        complete = set(),
        pending  = set(["resume", "error"]),
        resume   = set(["post", "pending", "error"]),
        error    = set(),
    )

    def __init__(self, ptr, op):
        if state not in self.STATES:
            raise ValueError("Unknown state {}".format(state))
        self.ptr = ptr
        self.grc = 0
        self.state = "init"
        self.op = op

    def change(self, op, state):
        global completed
        global posted
        global max_posted
        if op != self.op:
            raise ValueError("bad op: {}, expecting {}".format(op, self.op))
        to_states = self.FROM_TBL.get(self.state)
        if state not in to_states:
            raise ValueError("bad state change: {}, expecting {}" \
                             .format(state, to_states))
        if self.state == "post":
            posted.remove(self)
        if state == "post":
            posted.append(self)
            max_posted = max(len(posted), max_posted)
        if state == "complete":
            completed.append(self)
        self.state = state


    def __str__(self):
        return "({state}, {op}, {ptr})".format(**vars(self))

    def __repr__(self):
        return str(self)


LINE_RE = re.compile("""
        \[\d+\.\d+\] \s
        (post|complete|pending|resume) \s
        (read|write|send) \s
        (0x[0-9a-f]+)
""", flags=re.VERBOSE)
f = open("ep.log")
lines = f.readlines()
entries = dict()
for l in lines:
    m = LINE_RE.match(l)
    state, op, ptr = m.groups()
    e = entries.get(ptr)
    if not e:
        e = entries[ptr] = Entry(ptr, op)
    e.change(op, state)
    if state == "complete":
        entries.pop(ptr)

pendings = [ e for e in entries.values() if e.state == 'pending' ]
others = [ e for e in entries.values() if e.state != 'pending' ]
