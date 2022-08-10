import argparse
import errno
import json
import os
import sys

from time import sleep

from ldmsd.ldmsd_communicator import Communicator

from LDMS_Test import ContainerTest

class PluginConfigCMDTst(ContainerTest):
    LDMSD_PORT = 10001
    LDMSD_XPRT = "sock"

    test_name = "plugin_config_cmd"
    test_suite = "LDMSD"
    test_type = "unittest"
    test_desc = "Verify that the config commands related to plugins work as expected"

    test_node_name = "node-1"

    spec = {
        "type" : "NA",
        "templates" : {
            "ldmsd-base" : {
                "type" : "ldmsd",
                "listen" : [
                    { "port" : LDMSD_PORT, "xprt" : LDMSD_XPRT }
                ]
            }
        },
        "nodes" : [
            {
                "hostname" : "node-1",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "ldmsd",
                        "!extends" : "ldmsd-base",
                        "config" : [
                            "load name=store_csv",
                            "load name=test_sampler",
                            "config name=test_sampler action=add_schema " \
                                    "schema=my_schema metrics=a:data:u64:1:1:unit",
                            "config name=test_sampler action=add_set " \
                                    "schema=my_schema producer=%hostname% " \
                                    "instance=%hostname%/test_sampler",
                            "load name=vmstat",
                            "load name=array_example",
                            "config name=array_example producer=%hostname% instance=%hostname%/list_test",
                            "load name=procstat",
                            "config name=procstat producer=%hostname% instance=%hostname%/procstat",
                            "load name=store_sos",
                            "config name=store_sos path=/db/",
                            "strgp_add name=try plugin=store_sos schema=try container=try",
                            "strgp_start name=try"
                        ]
                    }
                ]
            }
        ]
    }

    assertions = [
        # plugn_status
        ("status-1", "Get the plugin statuses"),
        # plugin_load
        ("load-1", "Load a non-existing plugin"),
        ("load-2", "load a plugin"),
        ("load-3", "load a loadded plugin"),
        # config
        ("config-1", "Configure a plugin that hasn't been loaded"),
        ("config-2", "Misconfigure a loadded plugin"),
        ("config-3", "Correctly configure a loaded plugin"),
        # start
        ("start-1", "Start a plugin that hasn't been loaded"),
        ("start-2", "Start a store plugin"),
        ("start-3", "Start a running sampler plugin"),
        ("start-4", "Start a sampler plugin using a negative interval"),
        ("start-5", "Start a sampler plugin without an offset"),
        ("start-6", "Start a sampler plugin with an offset larger than half of interval"),
        ("start-7", "Start a sampler plugin"),
        ("start-8", "Check the status of the plugins"),
        # stop
        ("stop-1", "Stop a p lugin that hasn't been loaded"),
        ("stop-2", "Stop a sampler plugin that is not running"),
        ("stop-3", "Stop a running sampler plugin"),
        ("stop-4", "Check the status of the plugins"),
        # term
        ("term-1", "Terminate a plugin that hasn't been loaded"),
        ("term-2", "Terminate a running sampler plugin"),
        ("term-3", "Terminate an in-used store plugin"),
        ("term-4", "Terminate a sampler plugin"),
        ("term-5", "Terminate a store plugin"),
        ("term-6", "Check the status of the plugins")
    ]

def exp_status(name, type, interval = None, offset = None):
    return {
            "name" : name if type == "sampler" else name.replace("store_", ""),
            "type" : type,
            "sample_interval_us" : 1000000 if interval is None else interval,
            "sample_offset_us" : 0 if offset is None else offset,
            "libpath" : f"/opt/ovis/lib/ovis-ldms/lib{name}.so" 
           }

def errcode_cond(resp, exp_errcode, op = "=="):
    if op == "==":
        cond = resp['errcode'] == exp_errcode
    elif op == "!=":
        cond = resp['errcode'] != exp_errcode
    return {
            "cond" : cond,
            "cond_str" :f"resp['errcode'] ({resp['errcode']}) {op} {exp_errcode}"
           }

def plugn_status_cond(status, exp):
    a = sorted(status, key = lambda item: sorted(item.items()))
    b = sorted(exp, key = lambda item: sorted(item.items()))
    cond = a == b 
    return {
                "cond" : cond,
                "cond_str": "status is as expected" if cond else f"{a} == {b}"
           }

def plugn_load(comm, name):
    errcode, msg = comm.plugn_load(name = name)
    return {'errcode': errcode, 'msg': msg}

def plugn_status(comm):
    errcode, msg = comm.plugn_status()
    return ({'errcode':errcode, 'msg': msg},
            json.loads(msg) if errcode == 0 else None)

def plugn_config(comm, name, **kwargs):
    cfg_str = ""
    for k, v in kwargs.items():
        cfg_str += f"{k}={v} "
    cfg_str = cfg_str.strip()
    errcode, msg = comm.plugn_config(name = name, cfg_str = cfg_str)
    return {'errcode': errcode, 'msg': msg}

def plugn_start(comm, name, **kwargs):
    interval_us = 1000000
    offset_us = None
    if 'interval' in kwargs.keys():
        interval_us = kwargs['interval']
    if 'offset' in kwargs.keys():
        offset_us = kwargs['offset']
    errcode, msg = comm.plugn_start(name = name,
                                    interval_us = interval_us,
                                    offset_us = offset_us)
    return {'errcode': errcode, 'msg': msg}

def plugn_stop(comm, name):
    errcode, msg = comm.plugn_stop(name = name)
    return {'errcode': errcode, 'msg': msg}

def plugn_term(comm, name):
    errcode, msg = comm.plugn_term(name = name)
    return {'errcode': errcode, 'msg': msg}

def get_comm(host, xprt, port):
    comm = Communicator(host = host, xprt = xprt, port = port)
    rc = comm.connect()
    if rc:
        raise RuntimeError(f"Failed to connect to the ldmsd. Error {rc}")
    return comm

def plugin_status_test(suite):
    comm = get_comm(host = "node-1", xprt = suite.LDMSD_XPRT, port = suite.LDMSD_PORT)
    resp, status = plugn_status(comm)
    exp = [exp_status(name = "array_example", type = "sampler"),
           exp_status(name = "store_csv", type = "store"),
           exp_status(name = "test_sampler", type = "sampler", interval = 1000000, offset = 0),
           exp_status(name = "vmstat", type = "sampler"),
           exp_status(name = "store_sos", type = "store"),
           exp_status(name = "procstat", type = "sampler",
                      interval = 1000000, offset = 0)
          ]
    suite.save_assertion("status-1", **plugn_status_cond(status, exp))
    comm.close()

def plugin_load_test(suite):
    comm = get_comm(host = "node-1", port = suite.LDMSD_PORT, xprt = suite.LDMSD_XPRT)

    # Load a non-existing plugin
    resp = plugn_load(comm, name = "aaa")
    suite.save_assertion("load-1", **errcode_cond(resp, 0, op = "!="))
    # Load a plugin
    resp = plugn_load(comm, name = "meminfo")
    suite.save_assertion("load-2", **errcode_cond(resp, 0))
    # Load a loaded plugin
    resp = plugn_load(comm, name = "meminfo")
    suite.save_assertion("load-3", **errcode_cond(resp, errno.EEXIST))
    comm.close()

def plugin_config_test(suite):
    comm = get_comm(host = "node-1", port = suite.LDMSD_PORT, xprt = suite.LDMSD_XPRT)
    # Config a plugin that hasn't been loaded
    resp = plugn_config(comm, name = "foo")
    suite.save_assertion("config-1", **errcode_cond(resp, errno.ENOENT))
    # Misconfigure
    resp = plugn_config(comm, name = "store_csv")
    suite.save_assertion("config-2", **errcode_cond(resp, 0, op = "!="))
    # Configure a loaded plugin
    resp = plugn_config(comm, name = "meminfo", producer = "node-1", instance = "node-1/meminfo")
    suite.save_assertion("config-3", **errcode_cond(resp, 0))
    comm.close()

def plugin_start_test(suite):
    comm = get_comm(host = "node-1", port = suite.LDMSD_PORT, xprt = suite.LDMSD_XPRT)
    # Start a plugin that hasn't been loaded
    resp = plugn_start(comm, name = "foo")
    suite.save_assertion("start-1", **errcode_cond(resp, errno.ENOENT))
    # Start a store plugin
    resp = plugn_start(comm, name = "store_csv", interval = 1000000)
    suite.save_assertion("start-2", **errcode_cond(resp, errno.EINVAL))
    # Start a sampler plugin with a negative interval
    resp = plugn_start(comm, name = "vmstat", interval = -1000)
    suite.save_assertion("start-4", **errcode_cond(resp, errno.EINVAL))
    # Start a sampler plugin without an offset
    resp = plugn_start(comm, name = "test_sampler", interval = 1000000)
    suite.save_assertion("start-5", resp['errcode'] == 0,
                         f"resp['errcode'] ({resp['errcode']}) == 0")
    # Start a running plugin
    resp = plugn_start(comm, name = "test_sampler", interval = 1000000)
    suite.save_assertion("start-3", **errcode_cond(resp, errno.EBUSY))
    # Start a sampler plugin with an offset larger than half of the interval
    resp = plugn_start(comm, name = "meminfo", interval = 1000000, offset = 2000000)
    suite.save_assertion("start-6", resp['errcode'] == 0,
                         f"resp['errcode'] ({resp['errcode']}) == 0")
    # Start a sampler plugin with both valid interval and offset
    resp = plugn_start(comm, name = "procstat", interval = 1000000, offset = 0)
    suite.save_assertion("start-7", **errcode_cond(resp, 0))
    sleep(1)

    # Check the status of a plugins
    resp, status = plugn_status(comm)
    exp = [exp_status(name = "store_csv", type = "store"),
           exp_status(name = "meminfo", type = "sampler",
                      interval = 1000000, offset = 500000),
           exp_status(name = "test_sampler", type = "sampler",
                      interval = 1000000, offset = 0),
           exp_status(name = "vmstat", type = "sampler"),
           exp_status(name = "store_sos", type = "store"),
           exp_status(name = "array_example", type = "sampler"),
           exp_status(name = "procstat", type = "sampler",
                      interval = 1000000, offset = 0)
          ]
    suite.save_assertion("start-8", **plugn_status_cond(status, exp))
    comm.close() 

def plugin_stop_test(suite):
    comm = get_comm(host = "node-1", port = suite.LDMSD_PORT, xprt = suite.LDMSD_XPRT)
    # Stop a plugin that hasn't been loaded
    resp = plugn_stop(comm, name = "foo")
    suite.save_assertion("stop-1", **errcode_cond(resp, errno.ENOENT))
    # Stop a sampler plugin that is not running
    resp = plugn_stop(comm, name = "vmstat")
    suite.save_assertion("stop-2", **errcode_cond(resp, 0, op = "!="))
    # Stop a running sampler plugin
    resp = plugn_stop(comm, name = "meminfo")
    suite.save_assertion("stop-3", **errcode_cond(resp, 0))
    sleep(1)
    resp, status = plugn_status(comm)
    exp = [exp_status(name = "store_csv", type = "store"),
           exp_status(name = "meminfo", type = "sampler",
                      interval = 1000000, offset = 500000),
           exp_status(name = "test_sampler", type = "sampler",
                      interval = 1000000, offset = 0),
           exp_status(name = "vmstat", type = "sampler"),
           exp_status(name = "store_sos", type = "store"),
           exp_status(name = "array_example", type = "sampler"),
           exp_status(name = "procstat", type = "sampler",
                      interval = 1000000, offset = 0)
          ]
    suite.save_assertion("stop-4", **plugn_status_cond(status, exp))
    comm.close()

def plugin_term_test(suite):
    comm = get_comm(host = "node-1", port = suite.LDMSD_PORT, xprt = suite.LDMSD_XPRT)
    # terminate a plugin that hasn't been loaded
    resp = plugn_term(comm, name = "foo")
    suite.save_assertion("term-1", **errcode_cond(resp, errno.ENOENT))
    # Terminate a running plugin
    resp = plugn_term(comm, name = "test_sampler")
    suite.save_assertion("term-2", **errcode_cond(resp, errno.EINVAL))
    # Terminate an in-used store plugin
    resp = plugn_term(comm, name = "store_sos")
    suite.save_assertion("term-3", **errcode_cond(resp, errno.EINVAL))
    # Terminate a sampler plugin
    resp = plugn_term(comm, name = "meminfo")
    suite.save_assertion("term-4", **errcode_cond(resp, 0))
    # Termiate a store plugin
    resp = plugn_term(comm, name = "store_csv")
    suite.save_assertion("term-5", **errcode_cond(resp, 0))
    # Get the status
    sleep(1)
    resp, status = plugn_status(comm)
    exp = [exp_status(name = "test_sampler", type = "sampler",
                      interval = 1000000, offset = 0),
           exp_status(name = "vmstat", type = "sampler"),
           exp_status(name = "store_sos", type = "store"),
           exp_status(name = "array_example", type = "sampler"),
           exp_status(name = "procstat", type = "sampler",
                      interval = 1000000, offset = 0)
          ]
    suite.save_assertion("term-6", **plugn_status_cond(status, exp))
    comm.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--outdir")
    args = ap.parse_args()

    suite = PluginConfigCMDTst(args.outdir)

    plugin_status_test(suite)
    plugin_load_test(suite)
    plugin_config_test(suite)
    plugin_start_test(suite)
    plugin_stop_test(suite)
    plugin_term_test(suite)
