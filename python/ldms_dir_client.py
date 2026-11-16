#
# LDMS client module used by `ldms_dir_test` and `ldms_dir_large_test`.
# The tests import it in an interactive interpreter and create the clients:
#   from ldms_dir_client import *
#   c = Client()
#
# Terms (see the test descriptions):
# - dir request: ldms_xprt_dir(); flags=0 asks for the dir reply only,
#   flags=DIR_F_NOTIFY also registers the client for dir updates.
# - dir reply: the answer to a dir request. It is chunked in records; each
#   record has its own JSON string, and `more`=1 marks a partial reply.
# - dir update: DIR_ADD or DIR_UPD, sent after the dir reply to a registered
#   client.
#
# Importing the module only prepares the process (like ldms_dir_server.py); a
# connection is made when the test creates a Client.
#
# Every helper returns a short value (bool, str, list) so the test can compare
# what the interactive console prints.

import os
import time
import ctypes
import threading

from ovis_ldms import ldms

ldms.init(16*1024*1024)

# Request SIGHUP our process when parent exited
libc = ctypes.CDLL(None)
libc.prctl(1, 1) # prctl(PR_SET_PDEATHSIG, SIGHUP)

TYPE = { ldms.DIR_LIST: "LIST", ldms.DIR_ADD: "ADD",
         ldms.DIR_UPD: "UPD", ldms.DIR_DEL: "DEL" }

class Client(object):
    def __init__(self, host = "node-1", port = 10000, auth = "none",
                 auth_opts = None):
        self.host = host
        self.port = port
        self.auth = auth
        self.auth_opts = auth_opts
        self.x = self.new_xprt()
        self.x.connect(self.host, self.port)
        self.events = list() # (type str, [set names], status, more)
        self.probe_status = list()  # replies to the probe in cancel_done()
        self.cond = threading.Condition()

    def new_xprt(self):
        return ldms.Xprt(auth = self.auth, auth_opts = self.auth_opts)

    def _cb(self, x, status, dd, arg):
        names = [ d.name for d in dd.set_data ] if status == 0 else []
        with self.cond:
            self.events.append((TYPE.get(dd.type, dd.type), names, status,
                                dd.more))
            self.cond.notify_all()

    def mark(self):
        return len(self.events)

    def dir(self, flags):
        """Send a dir request. Returns "OK" or the error text."""
        try:
            self.x.dir(cb = self._cb, flags = flags)
            return "OK"
        except ConnectionError as e:
            return str(e)

    def register(self):
        """Dir request with flags=NOTIFY: dir reply, then dir updates"""
        return self.dir(ldms.DIR_F_NOTIFY)

    def request(self):
        """Dir request with flags=0: dir reply only"""
        return self.dir(0)

    def cancel(self):
        """ldms_xprt_dir_cancel(): stop the dir updates"""
        try:
            self.x.dir_cancel()
            return "OK"
        except ConnectionError as e:
            return str(e)

    def has(self, kind, name, since = 0):
        return any(k == kind and name in n
                   for (k, n, s, m) in self.events[since:])

    def wait(self, kind, name, since = 0, timeout = 5):
        """True if the event shows up within `timeout` seconds"""
        end = time.time() + timeout
        with self.cond:
            while not self.has(kind, name, since):
                left = end - time.time()
                if left <= 0:
                    return False
                self.cond.wait(left)
        return True

    def wait_events(self, n, timeout = 10):
        """True once `n` events have arrived"""
        end = time.time() + timeout
        with self.cond:
            while len(self.events) < n:
                left = end - time.time()
                if left <= 0:
                    return False
                self.cond.wait(left)
        return True

    def wait_prefix(self, kind, prefix, n, since = 0, timeout = 30):
        """True once `n` `kind` events carry a set whose name starts with
        `prefix` (counted in the sets of the events, since `since`)"""
        def total():
            return sum(1 for (k, ns, s, m) in self.events[since:] if k == kind
                       for x in ns if x.startswith(prefix))
        end = time.time() + timeout
        with self.cond:
            while total() < n:
                left = end - time.time()
                if left <= 0:
                    return False
                self.cond.wait(left)
        return True

    def count(self, kind, name, since = 0):
        """Number of `kind` events that carry set `name` since `since`"""
        return sum(1 for (k, n, s, m) in self.events[since:]
                   if k == kind and name in n)

    def probe(self, kind, name, sentinel, k, since, timeout = 10):
        """Number of `kind` events for set `name` that arrived before the
        k-th DIR_UPD of the set `sentinel`.

        A dir update is sent in the order of the calls on the server, so once
        the sentinel update has arrived, an update of `name` that was sent
        before it either has arrived or was dropped by the server. No sleep
        is needed to tell a drop from a late message. Returns -1 if the
        sentinel does not arrive."""
        end = time.time() + timeout
        with self.cond:
            while self.count("UPD", sentinel, since) < k:
                left = end - time.time()
                if left <= 0:
                    return -1
                self.cond.wait(left)
        return self.count(kind, name, since)

    def quiet(self, since = 0, timeout = 3):
        """True if nothing arrives during `timeout` seconds"""
        time.sleep(timeout)
        return len(self.events) == since

    def _probe_cb(self, x, status, dd, arg):
        self.probe_status.append(status)

    def cancel_done(self, timeout = 5):
        """True once the reply to dir_cancel() has been processed.

        The library refuses any dir request with EBUSY until that reply
        arrives, so a dir request that is accepted shows the reply is done.
        Its own dir reply goes to `probe_status`, not to `events`.
        """
        end = time.time() + timeout
        while time.time() < end:
            try:
                self.x.dir(cb = self._probe_cb, flags = 0)
                return True
            except ConnectionError as e:
                if "EBUSY" not in str(e):
                    return False
                time.sleep(0.1)
        return False

    def records(self, timeout = 120):
        """Dir request with flags=0. Returns the records of the dir reply as
        [(more, [set names]), ...], or a str describing the failure. Must not
        be used on a transport registered for dir updates."""
        recs = list()
        done = threading.Event()
        err = list()
        def cb(x, status, dd, arg):
            if status:
                err.append(status)
                done.set()
                return
            recs.append((dd.more, [ d.name for d in dd.set_data ]))
            if not dd.more:
                done.set()
        try:
            self.x.dir(cb = cb, flags = 0)
        except ConnectionError as e:
            return str(e)
        if not done.wait(timeout):
            return "timeout"
        if err:
            return f"status {err[0]}"
        return recs

    def shape(self, expect_total = None):
        """The dir reply as [(more, set count), ...] one tuple per record, or
        text describing the first problem found:
        - more=1 on every record but the last, which has more=0
        - a set listed twice
        - the number of sets is not `expect_total` (when given)"""
        recs = self.records()
        if isinstance(recs, str):
            return recs
        mores = [ m for m, _ in recs ]
        if not recs or mores[-1] != 0 or any(m != 1 for m in mores[:-1]):
            return f"bad more flags {mores}"
        names = [ n for _, ns in recs for n in ns ]
        if len(names) != len(set(names)):
            return "duplicate sets"
        if expect_total is not None and len(names) != expect_total:
            return f"{len(names)} sets, expected {expect_total}"
        return [ (m, len(ns)) for m, ns in recs ]

    def listed(self):
        """Number of sets in the dir reply (or the problem text)"""
        s = self.shape()
        return s if isinstance(s, str) else sum(n for _, n in s)

    def first_record(self):
        """Number of sets in the first record (or the problem text)"""
        s = self.shape()
        return s if isinstance(s, str) else s[0][1]

    def order(self):
        """'asc' or 'desc': the order in which a dir reply lists the sets
        (needs at least two published sets that fit in one record)"""
        recs = self.records()
        names = [ n for _, ns in recs for n in ns ]
        return "asc" if names == sorted(names) else "desc"

    def loop(self, n, low, high):
        """Send n dir requests (flags=0), one after another. Every dir reply
        must be well formed (see shape()) and list between `low` and `high`
        sets. Returns 'OK' or the first problem."""
        for i in range(n):
            s = self.shape()
            if isinstance(s, str):
                return f"request {i}: {s}"
            cnt = sum(c for _, c in s)
            if not low <= cnt <= high:
                return f"request {i}: {cnt} sets, expected {low} to {high}"
        return "OK"

    def close_mid_reply(self, times = 5, timeout = 30):
        """Open a new transport, send a dir request, and close the transport
        after the first record of the dir reply arrives; `times` times.
        Returns 'OK <records received before the close>' or the problem.
        The dir reply must be a multi-record one."""
        got = list()
        for _ in range(times):
            x = self.new_xprt()
            x.connect(self.host, self.port)
            first = threading.Event()
            recs = list()
            def cb(xx, status, dd, arg):
                recs.append(dd.more if status == 0 else None)
                first.set()
            try:
                x.dir(cb = cb, flags = 0)
            except ConnectionError as e:
                return str(e)
            if not first.wait(timeout):
                return "timeout"
            x.close()
            got.append(len(recs))
        return "OK " + str(got)

    def close(self):
        self.x.close()
        return True

def snapshot(host = "node-1", port = 10000):
    """Set names in the dir reply of a fresh transport"""
    x = ldms.Xprt(auth = "none")
    x.connect(host, port)
    names = sorted(d.name for d in x.dir())
    x.close()
    return names
