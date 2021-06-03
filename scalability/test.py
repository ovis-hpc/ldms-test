#!/usr/bin/python3
#
# Execute scalability test routine. See `config.py` to modify the configuration.
#
# Scenario
# --------
# * stop all daemons and clean-up residues from previous run
# * start all daemons (sampler, L1, L2 and L3 daemons).
#   L3 daemon also store data in `store_sos`.
#   * ldms_ls (L1, L2, L3) + verify.
# * Kill half of the samplers connected to the first L1.
#   * ldms_ls (L1, L2, L3) + verify.
# * Resurrect all sampler daemons.
#   * ldms_ls (L1, L2, L3) + verify.
# * Kill the 2nd L1 daemon.
#   * ldms_ls (L1, L2, L3) + verify.
# * Resurrect the 2nd L1 daemon.
#   * ldms_ls (L1, L2, L3) + verify.
# * Kill the 2nd L2 daemon.
#   * ldms_ls (L1, L2, L3) + verify.
# * Resurrect the 2nd L2 daemon.
#   * ldms_ls (L1, L2, L3) + verify.
# * Kill the L3 daemon.
#   * ldms_ls (L1, L2, L3) + verify.
# * Resurrect the L3 daemon.
#   * ldms_ls (L1, L2, L3) + verify.
# * Kill ALL daemons.
# * Verify data in:
#   * sos store
#   * monitoring data (for fd leaks)

import os
import sys
import json
import time
import logging
import argparse
import multiprocessing
import subprocess as sp

import TADA
from LDMS_Test import get_ovis_commit_id

from ovis_ldms import ldms
from sosdb import Sos as sos
from ldmsdutils import *

ldms.init(128*1024*1024)

logging_config()
logger = logging.getLogger(__name__)

p = argparse.ArgumentParser(description = "Scalability test routine")
args = p.parse_args()

# The script shall work in the WORK_DIR
os.chdir(WORK_DIR)
SRC_DIR = sys.path[0]


# === Helper functions ======================================================= #

def run(cmd, raise_on_error=True):
    ret = sp.run(cmd, shell=True, executable="/bin/bash",
                  stdout=sp.PIPE, stderr=sp.STDOUT)
    if ret.returncode:
        raise RuntimeError("{} failed:\n{}".format(cmd, ret.stdout.decode()))

def cleanup():
    logger.info("Cleaning up test artifacts (WORK_DIR: {})".format(WORK_DIR))
    run(SRC_DIR + "/cluster.py ldmsd_cleanup")
    run(SRC_DIR + "/cluster.py mon_cleanup")

def start_all():
    logger.info("Starting all daemons")
    run(SRC_DIR + "/cluster.py mon_start")
    run(SRC_DIR + "/cluster.py ldmsd_start")

def stop_all():
    logger.info("Stopping all daemons")
    run(SRC_DIR + "/cluster.py ldmsd_stop")
    run(SRC_DIR + "/cluster.py mon_stop")

def dir_verify(ldmsd_list, assert_no, rm_sets=[], rm_level=0):
    # ldms dir to each L1, L2 and L3 aggregators
    logger.info("Verifying LDMS directories")
    rm_sets = set(rm_sets)
    for d in ldmsd_list:
        _dir = d.dir()
        _dset = set(_d.name for _d in _dir)
        _eset = set(d.getExpectedDir())
        if d.agg_level > rm_level:
            _eset -= rm_sets
        if _dset != _eset:
            test.assert_test(assert_no, False,
                         "Bad dir result on {}, use pdb.pm() to see details" \
                         .format(d.name))
            if DEBUG:
                raise RuntimeError()
    else:
        test.assert_test(assert_no, True, "dir results verified")

# ------------------------------------------------------- Helper functions --- #

test = TADA.Test(test_suite = "LDMSD",
                 test_type = "FVT",
                 test_name = "scalability on {}".format(CLUSTER_NAME),
                 test_desc = "Scalability test",
                 test_user = USER,
                 commit_id = get_ovis_commit_id(OVIS_PREFIX),
                 tada_addr = TADA_ADDR)
test.add_assertion(1, "LDMS dir to each L1, L2 and L3 aggregator")
test.add_assertion(2, "Kill 1/2 samplers of L1 agg and check all aggregators")
test.add_assertion(3, "Resurrect samplers of L1 agg and check all aggregators")
test.add_assertion(4, "Kill 2nd L1 aggregator and check all aggregators")
test.add_assertion(5, "Resurrect 2nd L1 aggregator and check all aggregators")
test.add_assertion(6, "Kill 2nd L2 aggregator and check all aggregators")
test.add_assertion(7, "Resurrect 2nd L2 aggregator and check all aggregators")
test.add_assertion(8, "Kill the L3 aggregator and check all aggregators")
test.add_assertion(9, "Resurrect the L3 aggregator and check all aggregators")
test.add_assertion(10, "Verify number of file descriptors in ldmsd")
test.add_assertion(11, "SOS verification (L3)")

test.start()

# daemon handlers (to get to the expected dir results)
samp_list = LDMSDSampler.allDaemons()
l1_list = LDMSD_L1.allDaemons()
l2_list = LDMSD_L2.allDaemons()
l3_list = LDMSD_L3.allDaemons()
aggs = l1_list + l2_list + l3_list
daemons = { a.name: a for a in samp_list + l1_list + l2_list + l3_list }


if True: # The data generation part (w/ ldms dir verification)
    # stop + cleanup first
    stop_all()
    cleanup()

    # start all daemons
    start_all()

    l3 = daemons[ L3_AGG ]

    # wait for store sos.
    def store_sos_available():
        _dir = L3_STORE_ROOT + "/test"
        return os.path.exists(_dir)
    logger.info("Wait all sets to appear on L3")
    l3.wait_set_restored(timeout = 60)
    if L3_STORE_ROOT:
        logger.info("Wait for store_sos")
        cond_wait(store_sos_available, timeout = 60) # 60 sec should be enough
    logger.info("All sets to appear on L3")

    logger.info("Wait a bit to get data for a steady run")
    time.sleep(STEADY_WAIT) # Let it run steadily for a bit

    # test.add_assertion(1, "LDMS dir to each L1, L2 and L3 aggregator")
    dir_verify(aggs, 1, [])
    time.sleep(STEADY_WAIT) # so that the steady state includes our probing connections

    # Controller
    ctrl = Control()

    #test.add_assertion(2, "Kill 1/2 samplers of L1 agg and check all aggregators")
    l10 = l1_list[0]
    prdcrs = l10.getPrdcr()
    prdcrs = prdcrs[len(prdcrs)//2:]
    logger.info("Killing 1/2 of samplers connected to {}".format(l10.name))
    ctrl.stop(prdcrs, timeout=10)
    rm_sets = set(s for d in prdcrs for s in daemons[d].getExpectedDir())
    l3.wait_set_removed(rm_sets = rm_sets, timeout = 60)
    dir_verify(aggs, 2, rm_sets, rm_level = 0)

    #test.add_assertion(3, "Resurrect samplers of L1 agg and check all aggregators")
    logger.info("Resurrecting 1/2 of samplers connected to {}".format(l10.name))
    ctrl.start(prdcrs, timeout=10)
    l3.wait_set_restored(timeout = 60)
    dir_verify(aggs, 3, [])

    #test.add_assertion(4, "Kill 2nd L1 aggregator and check all aggregators")
    l11 = l1_list[1]
    logger.info("killing L1 {}".format(l11.name))
    l11.disconnect()
    ctrl.stop([l11.name], timeout=10)
    rm_sets = set(l11.getExpectedDir())
    l3.wait_set_removed(rm_sets = rm_sets, timeout = 60)
    _aggs = [ d for d in aggs if d.name != l11.name ]
    dir_verify(_aggs, 4, rm_sets, rm_level = 1)

    #test.add_assertion(5, "Resurrect 2nd L1 aggregator and check all aggregators")
    logger.info("resurrecting L1 {}".format(l11.name))
    ctrl.start([l11.name], timeout=10)
    l3.wait_set_restored(timeout = 60)
    logger.info("Connecting to {}".format(l11.name))
    l11.connect() # reconnect
    dir_verify(aggs, 5, [])

    #test.add_assertion(6, "Kill 2nd L2 aggregator and check all aggregators")
    l21 = l2_list[1]
    logger.info("killing L2 {}".format(l21.name))
    l21.disconnect()
    ctrl.stop([l21.name], timeout=10)
    rm_sets = set(l21.getExpectedDir())
    l3.wait_set_removed(rm_sets = rm_sets, timeout = 60)
    _aggs = [ d for d in aggs if d.name != l21.name ]
    dir_verify(_aggs, 6, rm_sets, rm_level = 2)

    #test.add_assertion(7, "Resurrect 2nd L2 aggregator and check all aggregators")
    logger.info("resurrecting L2 {}".format(l21.name))
    ctrl.start([l21.name], timeout=10)
    l3.wait_set_restored(timeout = 60)
    l21.connect()
    dir_verify(aggs, 7, [])

    #test.add_assertion(8, "Kill the L3 aggregator and check all aggregators")
    logger.info("killing L3 {}".format(l3.name))
    l3.disconnect()
    ctrl.stop([l3.name], timeout = 10)
    _aggs = [ d for d in aggs if d.name != l3.name ]
    dir_verify(_aggs, 8, [], rm_level = 3)

    #test.add_assertion(9, "Resurrect the L3 aggregator and check all aggregators")
    logger.info("resurrecting L3 {}".format(l3.name))
    ctrl.start([l3.name], timeout = 10)
    l3.connect()
    l3.wait_set_restored(timeout = 60)
    dir_verify(aggs, 9, [])

    logger.info("Wait a bit to capture the last steady state")
    time.sleep(STEADY_WAIT)

    # Terminate daemons
    stop_all()

#test.add_assertion(10, "Verify number of file descriptors in ldmsd")
if True:
    HOSTS = set( [L3_HOST] + L2_HOSTS + L1_HOSTS + SAMP_HOSTS )
    logger.info("Fetching monitoring data")
    mon_data = dict() # key := daemon_name (e.g. "node-1-20000")
    for root, dirs, files in os.walk("mon"):
        for fname in files:
            if not fname.endswith(".mon"):
                continue
            hname = fname.split(".", 1)[0]
            if hname not in HOSTS:
                continue
            path = os.path.join(root, fname)
            with open(path) as f:
                for l in f.readlines():
                    e = MonStat.from_str(l)
                    r = mon_data.setdefault(e.name, list())
                    r.append(e)

    fd_data = { k: [ (d.ts, d.fd) for d in dl ] for k, dl in mon_data.items() }

    # get the time of the first kill
    l10 = l1_list[0]
    prdcrs = l10.getPrdcr()
    p = prdcrs[len(prdcrs)//2]
    ts_steady = None
    nfd_prev = 0
    for ts, nfd in fd_data[p]:
        ts_steady = ts
        if nfd == 0 and nfd_prev != 0:
            break
        nfd_prev = nfd

    # Now, the max number of fd before the first kill is the steady number of fds
    assert_id = 10
    broken = False
    steady_fds = { name: 0 for name in fd_data.keys() }
    max_fds = dict()
    leaked_fds = dict()
    last_fds = dict()
    last_fd_ents = dict()
    for name, ents in fd_data.items():
        steady_fd = 0
        for ts, nfd in ents:
            if ts <= ts_steady:
                steady_fd = max(steady_fd, nfd)
            else:
                break
        steady_fds[name] = steady_fd
        for i in range(1, len(ents) + 1):
            ts, nfd = ents[-i] # iterate from the back
            if not nfd:
                continue
            # step back 5 more sec before the kill
            last_fd_ents[name] = ents[-i-5]
            last_fds[name] = ents[-i-5][1]
            break
        max_fds[name] = max( nfd for ts, nfd in ents )
        leaked_fds[name] = max_fds[name] > steady_fds[name]
    for name in fd_data.keys():
        steady_fd = steady_fds[name]
        nfd = last_fds[name]
        if nfd > steady_fd:
            test.assert_test(assert_id, False,
                             "{}: num_fd({}) > steady_fd({})" \
                             .format(name, nfd, steady_fd))
            broken = True
            break
    else:
        test.assert_test(assert_id, True, "verified")
    # End fd check


#test.add_assertion(11, "SOS verification (L3)")
if L3_STORE_ROOT:
    cont = sos.Container()
    cont.open(L3_STORE_ROOT + "/test")
    schema = cont.schema_by_name("test")
    attr = schema.attr_by_name("comp_time_job")
    itr = attr.attr_iter()
    def each(i):
        b = i.begin()
        while b:
            o = i.item()
            if o:
                yield o
            b = i.next()

    TestObj = namedtuple("TestObj", [ "timestamp", "component_id", "job_id", "metric_0", "metric_1" ] )

    objs = [ TestObj(*o[:5]) for o in each(itr) ]
    # split objs by component_id
    ocomp = dict()
    for o in objs:
        l = ocomp.setdefault(o.component_id, list())
        l.append(o)

    def ts_float(ts):
        return ts[0] + ts[1] * 1e-6

    LAST_N = 40
    broken = False
    assert_id = 11
    for comp_id, lst in ocomp.items():
        prev = lst[-LAST_N]
        # check data
        for e in lst[-LAST_N+1:]:
            tdiff = ts_float(e.timestamp) - ts_float(prev.timestamp)
            if tdiff > 1.5:
                broken = True
                test.assert_test(assert_id, False, "data missing detected")
                break
            m0_diff = e.metric_0 - prev.metric_0
            m1_diff = e.metric_1 - prev.metric_1
            if tdiff < 0.5:
                if m0_diff or m1_diff:
                    broken = True
                    test.assert_test(assert_id, False, "bad data")
                    break
            else:
                if not m0_diff or not m1_diff:
                    broken = True
                    test.assert_test(assert_id, False, "bad data")
                    break
            prev = e
        if broken:
            break
    else:
        test.assert_test(assert_id, True, "data verified")
    # End SOS data check


test.finish()
