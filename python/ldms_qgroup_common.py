#!/usr/bin/python3
import re
import time
import logging

from collections import deque
from ovis_ldms import ldms

mlog = logging.getLogger()

class QGROUP(object): # a place to statically hold config params
    CFG_QUOTA = 256
    CFG_ASK_MARK = 192
    CFG_ASK_AMOUNT = 64
    CFG_ASK_USEC = 500000 # 0.5 SEC
    CFG_RESET_USEC = 2000000 # 2 SEC

class PrdcrState(object):
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2

def stream_sub_cb(ev:ldms.StreamStatusEvent, prdcr):
    mlog.info(f"stream ('{ev.match}') subscribe status: {ev.status}")

class Prdcr(object):
    """Mimicking `prdcr` in ldmsd"""
    def __init__(self, host, port=411, xprt="sock", auth="none", auth_opts={},
                       rail_eps=1, rail_recv_limit=ldms.RAIL_UNLIMITED):
        self.host = host
        self.port = str(port)
        self.xprt = xprt
        self.auth = auth
        self.auth_opts = dict(auth_opts)
        self.rail_eps = rail_eps
        self.rail_recv_limit = rail_recv_limit
        self.x = None
        self.state = PrdcrState.DISCONNECTED

    @property
    def is_connected(self):
        return self.state == PrdcrState.CONNECTED

    def connect(self):
        if self.state != PrdcrState.DISCONNECTED:
            raise RuntimeError(f"Prdcr('{host}', '{port}') is {self.state}")
        self.x = ldms.Xprt(self.xprt, auth=self.auth, auth_opts=self.auth_opts,
                                      rail_eps = self.rail_eps,
                                      rail_recv_limit = self.rail_recv_limit)
        self.state = PrdcrState.CONNECTING
        try:
            self.x.connect(self.host, self.port, cb=self.ldms_cb)
        except:
            self.state = PrdcrState.DISCONNECTED
            raise

    def ldms_cb(self, xprt, e, arg):
        if e.type == ldms.EVENT_DISCONNECTED or \
           e.type == ldms.EVENT_REJECTED or \
           e.type == ldms.EVENT_ERROR:
                self.state = PrdcrState.DISCONNECTED
                return
        if e.type == ldms.EVENT_CONNECTED:
            assert(self.state == PrdcrState.CONNECTING)
            self.state = PrdcrState.CONNECTED
            # subscribe all stream
            self.x.stream_subscribe('.*', True, stream_sub_cb, self)
        # ignore SEND/RECV events


class StreamTally(object):
    def __init__(self, sec, next_tally = None):
        self.sec = sec
        self.dq = deque() # entry := (ts, load, src, data)
        self.tallies = dict() # by src
        self.total = 0 # the total tally
        self.next_tally = next_tally

    def tally_inc(self, src:tuple, v:int):
        self.tallies.setdefault(src, 0)
        self.tallies[src] += v
        self.total += v

    def tally_prune(self, ts):
        while self.dq:
            t, l, s, d = self.dq[0]
            if t + self.sec >= ts:
                break
            data = self.dq.popleft()
            self.tally_inc(s, -l)
            if self.next_tally:
                self.next_tally.dq.append(data)
        if self.next_tally:
            self.next_tally.tally_prune(ts)

    def add_data(self, data:ldms.StreamData):
        # This should be called on the first tally
        ts = time.time()
        src = str(data.src)
        load = len(data.name) + len(data.data)

        # increase our tally, and all subsequent tallies
        self.dq.append( (ts, load, src, data) ) # keep data as reference
        self.tally_inc(src, load)
        tally = self.next_tally
        while tally:
            tally.tally_inc(src, load)
            tally = tally.next_tally

        # prune the tally
        self.tally_prune(ts)


class StreamThroughput(object):
    """Simple stream throughput calculation"""
    def __init__(self, secs = [1, 5, 10, 30, 60]):
        self.secs = list(secs)
        self.secs.sort()
        self.tallies = [ StreamTally(s) for s in self.secs ]
        t = self.tallies[0]
        for i in range(1, len(self.secs)):
            t.next_tally = self.tallies[i]
            t = self.tallies[i]

    def add_data(self, data:ldms.StreamData):
        self.tallies[0].add_data(data)

    def tally_prune(self, ts):
        self.tallies[0].tally_prune(ts)

    def summary(self):
        return [ (t.sec, t.total) for t in self.tallies ]

    def details(self):
        return { t.sec : [ t.total, t.tallies ] for t in self.tallies }

def dummy_stream_data(name, data):
    return ldms.StreamData(name, ldms.LdmsAddr(), 0, 0, 0, 0o777, False, data, data)

RE = re.compile("""
        (?P<ts>[^ ]+\ [^ ]+) # timestamp
        \ (?P<logger>\w+)
        \ (?P<level>\w+)
        \ (?:
            (?:
                published:
                \ \[(?P<pub_name>\w+)\]:
                \ (?P<pub_data>.*)
            )
            |(?:
                pub_failed:
                \ (?P<pub_error>.*)
            )
            |(?:
                recv:
                \ (?P<recv_src>\S+)
                \ \[(?P<recv_name>\w+)\]:
                \ (?P<recv_data>.*)
            )
            |(?P<other>(?!(?:published|pub_failed|recv)).*)
        )
        """, re.X)

def parse_log(l):
    m = RE.match(l)
    return m.groupdict()
