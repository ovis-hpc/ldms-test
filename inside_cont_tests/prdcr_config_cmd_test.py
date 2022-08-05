import argparse
import errno
import json
import os
import sys

from time import sleep

from ldmsd.ldmsd_communicator import Communicator

from LDMS_Test import ContainerTest

class PrdcrConfigCMDTest(ContainerTest):

    LDMSD_PORT = 10001
    LDMSD_XPRT = "sock"

    test_name = "prdcr_config_cmd"
    test_suite = "LDMSD"
    test_type = "FVT"
    test_desc = "Verify that the handlers of the producer config commands work correctly"

    test_node_name = "samplerd"

    set_schema = "meminfo"
    set_instance = "samplerd-meminfo/meminfo"

    spec = {
        "type" : "NA",
        "templates" : {
            "ldmsd-base" : {
                "type" : "ldmsd",
                "listen" : [
                    { "port" : LDMSD_PORT, "xprt" : LDMSD_XPRT }
                ]
            },
            "prdcr" : {
                "xprt" : LDMSD_XPRT,
                "port" : LDMSD_PORT,
                "type" : "active",
                "interval" : 1000000
            },
        },
        "nodes" : [
            # compute nodes
            {
                "hostname" : "samplerd",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "samplerd",
                        "!extends" : "ldmsd-base"
                    }
                ]
            },
            {
                "hostname" : "samplerd-meminfo",
                "component_id" : 1,
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "samplerd",
                        "!extends" : "ldmsd-base",
                        "samplers" : [
                            {
                                "plugin" : "meminfo",
                                "config" : [
                                    "component_id=%component_id%",
                                    f"instance={set_instance}",
                                    "producer=%hostname%",
                                ],
                                "interval" : 1000000,
                                "start" : True
                            }
                        ]
                    }
                ]
            },
            # L1 nodes
            {
                "hostname" : "agg-status",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "prdcr-started",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-active",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-passive",
                                "host" : "samplerd-meminfo",
                                "xprt" : LDMSD_XPRT,
                                "port" : LDMSD_PORT,
                                "type" : "passive",
                                "interval" : 1000000
                            }
                        ],
                        "config" : [
                            "prdcr_start name=prdcr-started"
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg-add",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                    }
                ]
            },
            {
                "hostname" : "agg-start",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "prdcr-started",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-active",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-passive",
                                "host" : "samplerd-meminfo",
                                "xprt" : LDMSD_XPRT,
                                "port" : LDMSD_PORT,
                                "type" : "passive",
                                "interval" : 1000000
                            }
                        ],
                        "config" : [
                            "prdcr_start name=prdcr-started"
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg-start-regex",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "prdcr-started",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-active",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : [
                            "prdcr_start name=prdcr-started"
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg-stop",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "prdcr-added",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-started",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-stopped",
                                "host" : "samplerd",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : [
                            "prdcr_start name=prdcr-started",
                            "prdcr_start name=prdcr-stopped",
                            "prdcr_stop name=prdcr-stopped"
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg-stop-regex",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "prdcr-started",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-active",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : [
                            "prdcr_start name=prdcr-started"
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg-del",
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "prdcr-started",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "prdcr-active",
                                "host" : "samplerd-meminfo",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : [
                            "prdcr_start name=prdcr-started"
                        ]
                    }
                ]
            },
        ]
    }

    def exp_result(self, name, host, type = 'active',
                     interval = 1000000, xprt = LDMSD_XPRT,
                     port = LDMSD_PORT, state = "STOPPED",
                     sets = []):
        return  {
                    "name" : name,
                    "type" : type,
                    "host" : host,
                    "port" : int(port),
                    "transport" : xprt,
                    "reconnect_us" : str(interval),
                    "state" : state.upper(),
                    "sets" : sets
                }

    def exp_set(self, state, name = set_instance, schema = set_schema):
        return {
                "inst_name" : name,
                "schema_name" : schema,
                "state" : state
               }

    assertions = [
        # prdcr_status
        ("status-1", "LDMSD has no producers."),
        ("status-2", "Get prdcr_status of a non-existing producer."),
        ("status-3", "Get the result of a single producer"),
        ("status-4", "Get the result of a single producer with sets"),
        ("status-5", "Get the result of a passive producer"),
        ("status-6", "Get the results of two producers"),
        # prdcr_add
        ("add-1", "prdcr_add an active producer"),
        ("add-2", "prdcr_add a passive producer"),
        ("add-3", "prdcr_add with a string interval"),
        ("add-4", "prdcr_add with a negative interval"),
        ("add-5", "prdcr_add with zero interval"),
        ("add-6", "prdcr_add with an invalid type"),
        ("add-7", "prdcr_add with a negative port"),
        ("add-8", "prdcr_add with a non-existing host"),
        ("add-9", "prdcr_add an existing producer"),
        # prdcr_start
        ("start-1", "prdcr_start a non-existing producer"),
        ("start-2.1", "prdcr_start a stopped producer -- checking the errcode"),
        ("start-2.2", "prdcdr_start a stopped producer -- checking the status"),
        ("start-3.1", "prdcr_start a running producer -- checking the errcode"),
        ("start-3.2", "prdcr_start a running producer -- checking the status"),
        ("start-4.1", "prdcr_start a passive producer -- checking the errcode"),
        ("start-4.2", "prdcr_start a passive producer -- checking the status"),
        # prdcr_start_regex
        ("start_regex-1", "prdcr_start_regex using an invalid regex"),
        ("start_regex-2.1", "prdcr_start_regex matching no producers -- checking the errcode"),
        ("start_regex-2.2", "prdcr_start_regex matching no producers -- checking the statuses"),
        ("start_regex-3.1", "prdcr_start_regex matching running producers -- checking the errcode"),
        ("start_regex-3.2", "prdcr_start_regex matching running producers -- checking the statuses"),
        # prdcr_stop
        ("stop-1", "prdcr_stop a non-existing producer"),
        ("stop-2", "prdcr_stop a never-started producer"),
        ("stop-3.1", "prdcr_stop a connected producer -- checking the errcode"),
        ("stop-3.2", "prdcr_stop a connected producer -- checking the status"),
        ("stop-4", "prdcr_stop a stopped producer"),
        # prdcr_stop_regex
        ("stop_regex-1", "prdcr_stop_regex using an invalid regex"),
        ("stop_regex-2.1", "prdcr_stop_regex matching no producers -- checking the errcode"),
        ("stop_regex-2.2", "prdcr_stop_regex matching no producers -- checking the status"),
        ("stop_regex-3.1", "prdcr_stop_regex matching a running producer -- checking the errcode"),
        ("stop_regex-3.2", "prdcr_stop_regex matching a running producer -- checking the status"),
        # prdcr_del
        ("del-1", "prdcr_del a non-existing producer"),
        ("del-2.1", "prdcr_del a stopped producer -- checking the errcode"),
        ("del-2.2", "prdcr_del a stopped producer -- checking the status"),
        ("del-3.1", "prdcr_del a running producer -- checking the errcode"),
        ("del-3.2", "prdcr_del a running producer -- checking the status")
    ]

def prdcr_status(comm, **kwargs):
    errcode, msg = comm.prdcr_status(**kwargs)
    return ({'errcode' : errcode, 'msg': msg},
                json.loads(msg) if errcode == 0 else None)

def prdcr_add(comm, name, host = "samplerd", type = "active", interval = 1000000,
              xprt = PrdcrConfigCMDTest.LDMSD_XPRT,
              port = PrdcrConfigCMDTest.LDMSD_PORT):
    errcode, msg = comm.prdcr_add(name = name, host = host, ptype = type,
                     reconnect = interval, xprt = xprt, port = port)
    return {'errcode': errcode, 'msg' : msg }
    

def prdcr_start(comm, name):
    errcode,msg = comm.prdcr_start(name = name, regex = False)
    return {'errcode': errcode, 'msg' : msg }

def prdcr_start_regex(comm, regex):
    errcode, msg = comm.prdcr_start(name = regex)
    return {'errcode': errcode, 'msg' : msg }

def prdcr_stop(comm, name):
    errcode, msg = comm.prdcr_stop(name = name, regex = False)
    return {'errcode': errcode, 'msg': msg}

def prdcr_stop_regex(comm, regex):
    errcode, msg = comm.prdcr_stop(name = regex, regex = True)
    return {'errcode' : errcode, 'msg': msg}

def prdcr_del(comm, name):
    errcode, msg = comm.prdcr_del(name = name)
    return {'errcode' : errcode, 'msg' : msg}

def errcode_cond(resp, exp_errcode, op = "=="):
    if op == "==":
        cond = resp['errcode'] == exp_errcode
    elif op == "!=":
        cond = resp['errcode'] != exp_errcode
    return {
            "cond" : cond,
            "cond_str" :f"resp['errcode'] ({resp['errcode']}) {op} {exp_errcode}"
           }

def prdcr_status_cond(status, exp):
    cond = status == exp
    return {
                "cond" : cond,
                "cond_str": "status is as expected" if cond else f"{status} == {exp}"
           }

def prdcr_status_test(suite, comms):
    suite.log("prdcr_status_test -- start")
    # no producers
    samplerd_comm = comms["samplerd"]
    agg_comm = comms["agg-status"]
    resp, status = prdcr_status(samplerd_comm)
    suite.save_assertion("status-1", len(status) == 0, "len(status) == 0")

    # Get the status of a non-existing producer
    resp, status = prdcr_status(agg_comm, name = "foo")
    suite.save_assertion("status-2", **errcode_cond(resp, errno.ENOENT))

    # Result of a single producer
    resp, status = prdcr_status(agg_comm, name = "prdcr-active")
    exp = [suite.exp_result(name = "prdcr-active", host = "samplerd-meminfo")]
    suite.save_assertion("status-3", **prdcr_status_cond(status, exp))

    # Result of a single producer with sets
    resp, status = prdcr_status(agg_comm, name = "prdcr-started")
    exp = [suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "CONNECTED", sets = [suite.exp_set(state = "START")]
                            )
          ]
    suite.save_assertion("status-4", **prdcr_status_cond(status, exp))

    # Result of a passive producer
    resp, status = prdcr_status(agg_comm, name = "prdcr-passive")
    exp = [suite.exp_result(name = "prdcr-passive", host = "samplerd-meminfo", 
                            state = "STOPPED", type = "passive")]
    suite.save_assertion("status-5", **prdcr_status_cond(status, exp))

    # Result of two producers
    resp, status = prdcr_status(agg_comm)
    exp = []
    exp.append(suite.exp_result(name = "prdcr-active", host = "samplerd-meminfo"))
    exp.append(suite.exp_result(name = "prdcr-passive", host = "samplerd-meminfo",
                                type = "passive"))
    exp.append(suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                                state = "CONNECTED",
                                sets = [suite.exp_set(state = "START")]))
    suite.save_assertion("status-6", **prdcr_status_cond(status, exp))
    suite.log("prdcr_status_test -- end")

def prdcr_add_test(suite, comms):
    suite.log("prdcr_add_test -- start")
    agg_comm = comms["agg-add"]

    # Add an active producer
    resp = prdcr_add(agg_comm, name = "type_active")
    suite.save_assertion("add-1", **errcode_cond(resp, 0))

    # Add a passive producer
    resp = prdcr_add(agg_comm, name = "type_passive", type = "passive")
    suite.save_assertion("add-2", **errcode_cond(resp, 0))

    # String interval
    resp = prdcr_add(agg_comm, name = "interval_character", interval = "foo")
    suite.save_assertion("add-3", **errcode_cond(resp, errno.EINVAL))

    # Negative interval
    resp = prdcr_add(agg_comm, name = "negative_interval", interval = -1000000)
    suite.save_assertion("add-4", **errcode_cond(resp, errno.EINVAL))

    # zero interval
    resp = prdcr_add(agg_comm, name = "zero_interval", interval = 0)
    suite.save_assertion("add-5", **errcode_cond(resp, errno.EINVAL))

    # Invalid type
    resp = prdcr_add(agg_comm, name = "invalid_type", type = "foo")
    suite.save_assertion("add-6", **errcode_cond(resp, errno.EINVAL))

    # negative port
    resp = prdcr_add(agg_comm, name = "negative_port", port = "-10001")
    suite.save_assertion("add-7", **errcode_cond(resp, errno.EINVAL))

    # Non-existing host
    resp = prdcr_add(agg_comm, name = "not_exist_host", host = "foo")
    suite.save_assertion("add-8", **errcode_cond(resp, errno.EAFNOSUPPORT))

    # Existing host
    resp = prdcr_add(agg_comm, name = "type_active")
    suite.save_assertion("add-9", **errcode_cond(resp, errno.EEXIST))
    suite.log("prdcr_add_test -- end")

def prdcr_start_test(suite, comms):
    suite.log("prdcr_start_test -- start")
    comm = comms["agg-start"]

    # Start a non existing producer
    suite.log("prdcr_start_test -- start-1")
    resp = prdcr_start(comm, name = "foo")
    suite.save_assertion("start-1", **errcode_cond(resp, errno.ENOENT))

    # Start a stopped producer
    suite.log("prdcr_start_test -- start-2.1")
    resp = prdcr_start(comm, name = "prdcr-active")
    suite.save_assertion("start-2.1", **errcode_cond(resp, 0))
    sleep(1)
    suite.log("prdcr_start_test -- start-2.2")
    resp, status = prdcr_status(comm, name = "prdcr-active")
    exp = [suite.exp_result(name = "prdcr-active", host = "samplerd-meminfo",
                            state = "CONNECTED",
                            sets = [suite.exp_set(state = "START")]
                            )]
    suite.save_assertion("start-2.2", **prdcr_status_cond(status, exp))

    # Start a running producer
    suite.log("prdcr_start_test -- start-3.1")
    resp = prdcr_start(comm, name = "prdcr-started")
    suite.save_assertion("start-3.1", **errcode_cond(resp, errno.EBUSY))
    sleep(1)
    suite.log("prdcr_start_test -- start-3.2")
    resp, status = prdcr_status(comm, name = "prdcr-started")
    exp = [suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "CONNECTED",
                            sets = [suite.exp_set(state = "START")]
                            )]
    suite.save_assertion("start-3.2", **prdcr_status_cond(status, exp))

    # Start a passive producer

    # Skip the passive cases because there is a deadlock inside ldmsd
    # For passive producers,
    # prdcr_connect() takes the prdcr's lock and then calls prdcr_connect_cb(),
    # which takes the lock, so it is a deadlock.

    # suite.log("prdcr_start_test -- start-4.1")
    # resp = prdcr_start(comm, name = "prdcr-passive")
    # suite.save_assertion("start-4.1", **errcode_cond(resp, 0))
    # sleep(1)
    # suite.log("prdcr_start_test -- start-4.2")
    # resp, status = prdcr_status(comm, name = "prdcr-passive")
    # exp = [suite.exp_result(name = "prdcr-passive", host = "samplerd-meminfo",
    #                         state = "CONNECTED", type = "passive",
    #                         sets = [suite.exp_set(state = "START")]
    #                         )]
    # suite.save_assertion("start-4.2", **prdcr_status_cond(status,exp))
    # suite.log("prdcr_start_test -- end")

def prdcr_start_regex_test(suite, comms):
    comm = comms["agg-start-regex"]
    # Invalid regex
    resp = prdcr_start_regex(comm, "[")
    suite.save_assertion("start_regex-1", **errcode_cond(resp, 0, "!="))
    # Matched no producers
    resp = prdcr_start_regex(comm, "foo")
    suite.save_assertion("start_regex-2.1", **errcode_cond(resp, 0))
    resp, status = prdcr_status(comm)
    exp = [suite.exp_result(name = "prdcr-active", host = "samplerd-meminfo",
                            state = "STOPPED"),
           suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "CONNECTED", sets = [suite.exp_set(state = "START")])
          ]
    suite.save_assertion("start_regex-2.2", **prdcr_status_cond(status,exp))
    # Matched some running producers
    resp = prdcr_start_regex(comm, ".*")
    suite.save_assertion("start_regex-3.1", **errcode_cond(resp, 0))
    sleep(1)
    resp, status = prdcr_status(comm)
    exp = [suite.exp_result(name = "prdcr-active", host = "samplerd-meminfo",
                            state = "CONNECTED", sets = [suite.exp_set(state = "START")]),
           suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "CONNECTED", sets = [suite.exp_set(state = "START")])
          ]
    suite.save_assertion("start_regex-3.2", **prdcr_status_cond(status,exp))

def prdcr_stop_test(suite, comms):
    comm = comms["agg-stop"]

    # a non existing producer
    resp = prdcr_stop(comm, name = "foo")
    suite.save_assertion("stop-1", **errcode_cond(resp, errno.ENOENT))
    # a never started producer
    resp = prdcr_stop(comm, name = "prdcr-added")
    suite.save_assertion("stop-2", **errcode_cond(resp, 0))
    # a running producer
    resp = prdcr_stop(comm, name = "prdcr-started")
    suite.save_assertion("stop-3.1", **errcode_cond(resp, 0))
    sleep(1)
    resp, status = prdcr_status(comm, name = "prdcr-started")
    exp = [suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                             state = "STOPPED")]
    suite.save_assertion("stop-3.2", **prdcr_status_cond(status, exp))
    # a stopped producer
    resp = prdcr_stop(comm, name = "prdcr-stopped")
    suite.save_assertion("stop-4", **errcode_cond(resp, 0))

def prdcr_stop_regex_test(suite, comms):
    comm = comms["agg-stop-regex"]

    # Invalid regex
    resp = prdcr_stop_regex(comm, "[")
    suite.save_assertion("stop_regex-1", **errcode_cond(resp, 0, op = "!="))
    # not matched any producers
    resp = prdcr_stop_regex(comm, "foo")
    suite.save_assertion("stop_regex-2.1", **errcode_cond(resp, 0))
    sleep(1)
    resp, status = prdcr_status(comm)
    exp = [suite.exp_result(name = "prdcr-active", host = "samplerd-meminfo",
                            state = "STOPPED"),
           suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "CONNECTED", sets = [suite.exp_set(state = "START")])
          ]
    suite.save_assertion("stop_regex-2.2", **prdcr_status_cond(status, exp))
    # Matched a running producer
    resp = prdcr_stop_regex(comm, regex = ".*")
    suite.save_assertion("stop_regex-3.1", **errcode_cond(resp, 0))
    sleep(1)
    resp, status = prdcr_status(comm)
    exp = [suite.exp_result(name = "prdcr-active", host = "samplerd-meminfo",
                            state = "STOPPED"),
           suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "STOPPED")
          ]
    suite.save_assertion("stop_regex-3.2", **prdcr_status_cond(status, exp))

def prdcr_del_test(suite, comms):
    comm = comms["agg-del"]

    # Non-existing producer
    resp = prdcr_del(comm, name = "foo")
    suite.save_assertion("del-1", **errcode_cond(resp, errno.ENOENT))
    # a stopped producer
    resp = prdcr_del(comm, name = "prdcr-active")
    suite.save_assertion("del-2.1", **errcode_cond(resp, 0))
    sleep(1)
    resp, status = prdcr_status(comm)
    exp = [suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "CONNECTED", sets = [suite.exp_set(state = "START")])]
    suite.save_assertion("del-2.2", **prdcr_status_cond(status, exp))
    # a running producer
    resp = prdcr_del(comm, name = "prdcr-started")
    suite.save_assertion("del-3.1", **errcode_cond(resp, errno.EBUSY))
    sleep(1)
    resp, status = prdcr_status(comm)
    exp = [suite.exp_result(name = "prdcr-started", host = "samplerd-meminfo",
                            state = "CONNECTED", sets = [suite.exp_set(state = "START")])]
    suite.save_assertion("del-3.2", **prdcr_status_cond(status, exp))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--outdir")
    args = ap.parse_args()

    suite = PrdcrConfigCMDTest(args.outdir)

    COMM = {}
    COMM["samplerd"] = Communicator(host = "samplerd",
                                    xprt = suite.LDMSD_XPRT,
                                    port = suite.LDMSD_PORT)
    COMM["samplerd-meminfo"] = Communicator(host = "samplerd-meminfo", 
                                            xprt = suite.LDMSD_XPRT,
                                            port = suite.LDMSD_PORT)
    COMM.update(dict([(k, Communicator(host = f"{k}",
                                       xprt = suite.LDMSD_XPRT,
                                       port = suite.LDMSD_PORT))
                for k in ["agg-status", "agg-add",
                          "agg-start", "agg-start-regex",
                          "agg-stop", "agg-stop-regex",
                          "agg-del"]]))

    for k, comm in COMM.items():
        rc = comm.connect()
        if rc != 0:
            raise RuntimeError(f"{k}: failed to connect to LDMSD. Error {rc}")

    prdcr_status_test(suite, COMM)
    prdcr_add_test(suite, COMM)
    prdcr_start_test(suite, COMM)
    prdcr_start_regex_test(suite, COMM)
    prdcr_stop_test(suite, COMM)
    prdcr_stop_regex_test(suite, COMM)
    prdcr_del_test(suite, COMM)

    for k, comm in COMM.items():
        comm.close()
