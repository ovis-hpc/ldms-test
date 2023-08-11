#!/usr/bin/python3

import argparse
import atexit
import importlib
import logging
import os
import sys

from time import sleep

import TADA
from LDMS_Test import process_args, add_common_args, \
                      LDMSDCluster, ContainerTest

LDMS_TEST_SRC = "/tada-src"
CONTAINER_DATA_ROOT_PATH = "/db"
INSIDE_CONT_TESTS_PATH = "inside_cont_tests"

logging.basicConfig(format = "%(asctime)s %(name)s %(levelname)s %(message)s",
                    level = logging.INFO)
log = logging.getLogger("inside_cont_test")
exec(open(os.getenv("PYTHONSTARTUP", "/dev/null")).read())

suites = []

def get_test_dir_cont(suite):
    return f"{CONTAINER_DATA_ROOT_PATH}/{suite.test_name}"

def get_test_dir_host(suite):
    global args
    return f"{args.data_root}/{suite.test_name}"

def cleanup_test_data(suite):
    test_node = suite.cluster.get_container(suite.test_node_name)
    test_node.exec_run(f"rm -fr {get_test_dir_cont(suite)}")

def make_test_dir(suite):
    test_node = suite.cluster.get_container(suite.test_node_name)
    rc, out = test_node.exec_run(f"mkdir -p {get_test_dir_cont(suite)}")
    if rc != 0:
        raise RuntimeError(f"Failed to create the directory {get_test_dir_cont(suite)} " \
                   "in side the container {suite.test_node_name}. {out}")

@atexit.register
def at_exit():
    for suite in suites:
        cleanup_test_data(suite)
        suite.cluster.remove()

def get_executable_name(suite):
    return suite.__module__.split(".")[1]

def __debug(cont):
    pyt = cont.exec_interact(["/bin/bash"])
    sleep(1.0)
    out = pyt.read(idle_timeout = 0.1)

def get_ovis_pythonpath_cont(host_prefix):
    for root, dirs, files in os.walk(f"{host_prefix}/lib/"):
        for d in dirs:
            if "python" in d:
                return f"/opt/ovis/lib/{d}/site-packages"

def run_suite(suite):
    global args
    global suites

    log.info(f"{suite.test_name}: Start testing {suite.test_name}")
    spec = suite.spec
    spec["name"] = f"{args.clustername}_{suite.test_name}"
    spec["description"] = f"{args.user}'s {spec['name']}"
    if "cap_add" in spec.keys():
        spec["cap_add"] = list(set(spec["cap_add"] + ["SYS_PTRACE", "SYS_ADMIN"]))
    else:
        spec["cap_add"] = ["SYS_PTRACE", "SYS_ADMIN"]
    spec["image"] = args.image
    spec["ovis_prefix"] = args.prefix
    if "env" not in spec.keys():
        spec["env"] = {}
    spec["env"]["TADA_ADDR"] = args.tada_addr
    spec["env"]["PYTHONPATH"] = f"{LDMS_TEST_SRC}:{get_ovis_pythonpath_cont(spec['ovis_prefix'])}"
    if "mounts" not in spec.keys():
        spec["mounts"] = []
    spec["mounts"] += args.mount + ([f"{args.src}:{args.src}:ro"] if args.src else []) + \
                     [f"{args.data_root}:{CONTAINER_DATA_ROOT_PATH}:rw"] + \
                     [f"{os.path.realpath(sys.path[0])}:/{LDMS_TEST_SRC}:ro"]

    suite.test = TADA.Test(test_suite = suite.test_suite,
                     test_type = suite.test_type,
                     test_name = suite.test_name,
                     test_desc = suite.test_desc,
                     test_user = args.user,
                     commit_id = args.commit_id,
                     tada_addr = args.tada_addr)

    for assertion_id, assertion_desc in suite.assertions:
        suite.test.add_assertion(assertion_id, assertion_desc)

    suite.test.start()

    log.info(f"{suite.test_name}: Preparing the containers")
    cluster = LDMSDCluster.get(spec["name"], create = True, spec = spec)
    suite.cluster = cluster
    suites.append(suite)
    cluster.start_daemons()
    cluster.make_known_hosts()

    sleep(1)

    cleanup_test_data(suite)
    if args.debug and sys.flags.interactive:
        input("ready ...")
    log.info(f"{suite.test_name}: Running the test script")
    test_cont = cluster.get_container(suite.test_node_name)
    if test_cont is None:
        raise Exception(f"Cannot find the test container with hostname {suite.test_node_name}")
    make_test_dir(suite)
    rc, out = test_cont.exec_run(f"python3 {LDMS_TEST_SRC}/inside_cont_tests/{get_executable_name(suite)}.py " \
                                 f"--outdir={get_test_dir_cont(suite)}")
    if rc:
        raise Exception(f"{suite.test_name}: The test script failed! Exit code {rc}: {out}")

    # Get assertion results
    try:
        for a in suite.load_assertions():
            suite.test.assert_test(a['assert_id'], a['cond'], a['cond_str'])
    except FileNotFoundError:
        log.error(f"Cannot find the result of {suite.test_name}")
    suite.test.finish()

    if args.debug and sys.flags.interactive:
        input("Remove containers? ...")
    log.info(f"{suite.test_name}: done")
    suites.remove(suite)
    cleanup_test_data(suite)
    if not suite.persist:
        suite.cluster.remove()

def find_class(m):
    for name, cls in m.__dict__.items():
        if name == "ContainerTest":
            continue
        try:
            if issubclass(cls, ContainerTest):
                yield cls
        except TypeError:
            pass

ap = argparse.ArgumentParser(description = "Run tests that need to run inside containers")
ap.add_argument("--suite", action = "append",
                help = "Test Suite name (Python Module name)")
ap.add_argument("--persist", action = "append", default = None,
                help = "Keep the suite's container. " \
                       "ALL means keeping the containers of all suites.")
add_common_args(ap)
args = ap.parse_args()
process_args(args)

def get_suite(module):
    suites = [cls for cls in find_class(module)]
    if len(suites) > 1:
        raise RuntimeError(f"Module {m} has more than one 'ContainerTest' subclasses.")
    elif len(suites) < 1:
        return None
    return suites[0]


def list_test_modules():
    l = []
    for x in os.listdir(INSIDE_CONT_TESTS_PATH):
        if x == "example.py":
            # Skip the example.py
            continue
        if not x.endswith(".py"):
            continue
        l.append(x[:-3])
    return l

# Load the test suites
mods = {}
_list = args.suite if args.suite else list_test_modules()

for m in _list:
    mods[m] = importlib.import_module(f"inside_cont_tests.{m}")
    try:
        suite = get_suite(mods[m])()
        if suite is None:
            continue
    except:
        raise
    suite.persist = False
    if args.persist and ("ALL" in args.persist or m in args.persist):
        suite.persist = True
    suite.set_outdir(get_test_dir_host(suite))
    try:
        log.info("===========================================================")
        run_suite(suite)
    except Exception as e:
        if args.debug and sys.flags.interactive:
            input("remove containers? ...")
        suite.cluster.remove()
        log.error(e)
