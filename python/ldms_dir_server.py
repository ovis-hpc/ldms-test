#
# LDMS server module used by `ldms_dir_test` and `ldms_dir_large_test`.
# The tests import it in an interactive interpreter and start the server:
#   from ldms_dir_server import *
#   start()
#
# start() listens and publishes one set. The other functions let the test
# publish, unpublish and change sets (a published set produces a DIR_ADD and
# a set-info change a DIR_UPD for the clients registered for dir updates), and
# close the accepted transports. Importing the module only prepares the
# process (like ldms_dir_client.py); it does not listen.

import os
import time
import ctypes
import threading

from ovis_ldms import ldms

ldms.init(16*1024*1024)

# Request SIGHUP our process when parent exited
libc = ctypes.CDLL(None)
libc.prctl(1, 1) # prctl(PR_SET_PDEATHSIG, SIGHUP)

SCHEMA = ldms.Schema(name = "dir_test", metric_list = [ ( "a", int ) ])

lx = None
xset = list() # connected (passive) transports

def listen_cb(x, ev, arg):
    if ev.type == ldms.EVENT_CONNECTED:
        xset.append(x)
    elif ev.type == ldms.EVENT_DISCONNECTED:
        if x in xset:
            xset.remove(x)

def start(port = 10000):
    """Listen on `port` (auth none) and publish the set srv/set0"""
    global lx
    lx = ldms.Xprt(auth = "none")
    lx.listen(host = "0.0.0.0", port = port, cb = listen_cb, cb_arg = None)
    add_set("srv/set0")
    return True

SETS = dict()   # every Set made by the helpers (kept alive when unpublished)
PUB = set()     # names of the sets that are published
INFO = dict()   # name -> { key: value length } of the set info

def _perm_str(perm):
    """The 10-character permission string of the dir entry"""
    out = "-"
    for sh in (6, 3, 0):
        m = perm >> sh
        out += ("r" if m & 4 else "-") + ("w" if m & 2 else "-") \
               + ("x" if m & 1 else "-")
    return out

def _field(v, name):
    return v[name] if isinstance(v, dict) else getattr(v, name)

def entry_len(name):
    """Length in bytes of the entry of set `name` in the JSON string of a dir
    reply or dir update (one set: its metadata and its set info).

    It follows the format string of __ldms_format_set_meta_as_json() in
    ldms.c. The length is the same for the first set of a record (leading
    ' ') and for the others (leading ',').
    """
    s = SETS[name]
    dig = s.digest_str or ""
    ts = s.transaction_timestamp
    du = s.transaction_duration
    state = ("C" if s.is_consistent else " ") + "L" + " "
    info = ",".join('{"key":"%s","value":"%s"}' % (k, "x" * n)
                    for k, n in INFO[name].items())
    txt = ' {"name":"%s","schema":"%s","digest":"%s","flags":"%s",' \
          '"meta_size":%d,"data_size":%d,"heap_size":%d,"uid":%d,"gid":%d,' \
          '"perm":"%s","card":%d,"array_card":%d,"meta_gn":%d,"data_gn":%d,' \
          '"timestamp":{"sec":%d,"usec":%d},"duration":{"sec":%d,"usec":%d},' \
          '"info":[%s]}' % (
              s.name, s.schema_name, dig, state, s.meta_sz, s.data_sz,
              s.heap_size, s.uid, s.gid, _perm_str(s.perm), s.card,
              1, s.meta_gn, s.data_gn,
              _field(ts, "sec"), _field(ts, "usec"),
              _field(du, "sec"), _field(du, "usec"), info)
    return len(txt)

def _grow(name, target):
    """Set-info length of "k" that makes entry_len(name) equal `target`"""
    v = INFO[name]["k"]
    n = v + target - entry_len(name)
    if n < 1:
        raise ValueError(f"entry of {name} cannot be as small as {target}")
    return n

def listen_munge(sock, port = 10001):
    """Listen on another port with munge authentication (the munge socket
    `sock`): a client on it has the uid of its munge credential"""
    global lm
    lm = ldms.Xprt(auth = "munge", auth_opts = { "socket": sock })
    lm.listen(host = "0.0.0.0", port = port, cb = listen_cb, cb_arg = None)
    return True

def add_set(name, info_len = 0, entry = None, perm = 0o777):
    """Create and publish a set.
    info_len > 0: the set has a set-info value of that many bytes.
    entry: make the entry of the set `entry` bytes (it overrides info_len).
    perm: the permission of the set (owner is the user of this process)."""
    s = ldms.Set(name = name, schema = SCHEMA, perm = perm)
    SETS[name] = s
    INFO[name] = dict()
    if entry is not None:
        info_len = 1
    if info_len:
        s.info_set("k", "x" * info_len)
        INFO[name]["k"] = info_len
    if entry is not None:
        n = _grow(name, entry)
        s.info_set("k", "x" * n)
        INFO[name]["k"] = n
        assert entry_len(name) == entry
    s.publish()
    PUB.add(name)
    return True

def add_fill(n, info_len, prefix = "fill"):
    """Publish `n` sets named <prefix>/000000, <prefix>/000001 ..."""
    for i in range(n):
        add_set(f"{prefix}/{i:06d}", info_len)
    return True

def tune(name, info_len):
    """Change the set-info length of an existing set (entry size follows)"""
    SETS[name].info_set("k", "x" * info_len)
    INFO[name]["k"] = info_len
    return True

def fit(name, entry):
    """Change the set info of an existing set so that its entry is `entry`
    bytes. This is one set-info change, so one DIR_UPD if it is published."""
    n = _grow(name, entry)
    SETS[name].info_set("k", "x" * n)
    INFO[name]["k"] = n
    assert entry_len(name) == entry
    return True

def total(names = None):
    """Sum of the entry lengths of the published sets (or of `names`)"""
    return sum(entry_len(n) for n in (sorted(PUB) if names is None else names))

def expected(order, limit):
    """The records a dir reply should have, as [(more, set count), ...]:
    the published sets in listing order, a record takes sets while the sum of
    their entries is at most `limit`."""
    recs = list()
    cur = cnt = 0
    for n in sorted(PUB, reverse = (order == "desc")):
        e = entry_len(n)
        if cnt and cur + e > limit:
            recs.append((1, cnt))
            cur = cnt = 0
        cur += e
        cnt += 1
    recs.append((0, cnt))
    return recs

def n_sets():
    return len(PUB)

def unpublish(name):
    """Unpublish a set: it is no longer in any new dir reply"""
    SETS[name].unpublish()
    PUB.discard(name)
    return True

def unpublish_all():
    """Unpublish every published set (the sets stay allocated, so names must
    not be reused)"""
    for n in list(PUB):
        try:
            unpublish(n)
        except OSError:
            PUB.discard(n) # already unpublished (ENOENT)
    return True

def info(name, key, val):
    """Change the set info; this produces a DIR_UPD"""
    SETS[name].info_set(key, val)
    INFO[name][key] = len(val)
    return True

def n_xprts():
    return len(xset)

def close_xprts():
    """Close every accepted transport (server-side close)"""
    for x in list(xset):
        x.close()
    return True

def add_long(name_len, schema_len, prefix = "n"):
    """Create and publish a set with a name of `name_len` characters and a
    schema with a name of `schema_len` characters. Returns 'ok' or
    'error: <text>'"""
    try:
        sch = ldms.Schema(name = "s" * schema_len,
                          metric_list = [ ( "a", int ) ])
        name = prefix * name_len
        st = ldms.Set(name = name, schema = sch)
        st.publish()
    except Exception as e:
        return "error: %s" % e
    SETS[name] = st
    INFO[name] = dict()
    PUB.add(name)
    return "ok"

CHURN = dict(thread = None, errors = 0, stop = threading.Event())

def churn(name):
    """In a thread: unpublish and publish the set `name` again and again
    until churn_stop() is called. Returns at once."""
    def run():
        while not CHURN["stop"].is_set():
            try:
                SETS[name].unpublish()
                SETS[name].publish()
            except Exception:
                CHURN["errors"] += 1
    CHURN["errors"] = 0
    CHURN["stop"].clear()
    CHURN["thread"] = threading.Thread(target = run)
    CHURN["thread"].start()
    return True

def churn_stop(timeout = 60):
    """Stop churn(). Returns 'done errors=<n>' ('running' if it does not stop).
    The set ends up published."""
    CHURN["stop"].set()
    CHURN["thread"].join(timeout)
    if CHURN["thread"].is_alive():
        return "running"
    return "done errors=%d" % CHURN["errors"]

BURST = dict(thread = None)

def burst(n, prefix = "burst", info_len = 1):
    """In a thread: publish n sets named <prefix>/000000 ... (each one is a
    DIR_ADD). Returns at once; burst_done() tells when it ends"""
    def run():
        for i in range(n):
            add_set(f"{prefix}/{i:06d}", info_len)
    BURST["thread"] = threading.Thread(target = run)
    BURST["thread"].start()
    return True

def burst_done(timeout = 60):
    BURST["thread"].join(timeout)
    return not BURST["thread"].is_alive()

def wait_xprts(n, timeout = 10):
    """True when the number of accepted transports is n (within timeout)"""
    end = time.time() + timeout
    while time.time() < end:
        if len(xset) == n:
            return True
        time.sleep(0.1)
    return len(xset) == n
