#!/usr/bin/python3
#
# This file contains common components for python/ldms_rail_* test scripts.

from ovis_ldms import ldms

SCHEMA = ldms.Schema(name = "test", metric_list = [
        ( "a"     , int , ), # `int` is equivalent to "int64"
        ( "b"     , int , ), # `int` is equivalent to "int64"
        ( "c"     , int , ), # `int` is equivalent to "int64"
    ])

def sample(s, val):
    s.transaction_begin()
    s[0] = val
    s[1] = val + 1
    s[2] = val + 2
    s.transaction_end()

def verify_set(s, val=None):
    """Verify the values in the set"""
    if val is None:
        val = s[0]
    _exp = [ val, val+1, val+2 ]
    _val = [ s[0], s[1], s[2] ]
    if _exp != _val:
        raise RuntimeError(f"Expecting {_exp} but got {_val}")

def xprt_pool_idx(x):
    """Get pool indices for each endpoint in the rail"""
    _tmp = ldms.ZapThrStat.get_result()
    # accessible by thread_t value
    thr = { o.thread_id : o for o in _tmp }
    ep_threads = x.get_threads()
    pool_idx = [ thr[k].pool_idx for k in ep_threads ]
    return pool_idx
