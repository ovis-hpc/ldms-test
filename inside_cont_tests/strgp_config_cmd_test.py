import argparse
import errno
import json
import os
import sys

from time import sleep

from LDMS_Test import ContainerTest

class StrgpConfigCMDTest(ContainerTest):

    test_name = "strgp_config_cmd"
    test_suite = "LDMSD"
    test_type = "FVT"
    test_desc = "Test the handlers of strgp config commands"

    test_node_name = "samplerd-1"

    LDMSD_PORT = 10001
    LDMSD_XPRT = "sock"

    SMP1_NAME = "samplerd-1"
    SMP2_NAME = "samplerd-2"
    SMP_PLUGN_NAME = "test_sampler"
    SMP_SCHEMA_1_NAME = "schema_m3"
    SMP_SCHEMA_1_METRIC1 = {'name': "m1", 'mtype': "meta", 'vtype': "u64", 'init': "1"}
    SMP_SCHEMA_1_METRIC2 = {'name': "d1", 'mtype': "data", 'vtype': "u64", 'init': "2"}
    SMP_SCHEMA_1_METRIC3 = {'name': "d2", 'mtype': "data", 'vtype': "u64", 'init': "3"}
    SMP_SCHEMA_2_NAME = "schema_default"
    SMP_SCHEMA_2_METRIC1 = {'name': "metric_1", 'mtype': "data", 'vtype': "u64"}
    SAMPLE_INTRVL = 1000000
    SAMPLE_OFFSET = 0
    SMP_DEFAULT_NUM_METRICS = "1"

    STORE_FLATFILE = "store_flatfile"
    STORE_CSV = "store_csv"

    STRGP_ALL_NAME = "all"
    STRGP_SINGLE_PRDCR_NAME = "single_prdcr"
    STRGP_SINGLE_PRDCR_PRDCR_NAME = SMP1_NAME
    STRGP_SINGLE_METRIC_NAME = "single_metric"
    STRGP_STOPPED_NAME = "stopped"
    STRGP_INVALID_NAME = "invalid_strgp"
    STORE_PATH = "/db/%hostname%"
    STRGP_METRIC_FILTER = "d1"
    STRGP_PRDCR_FILTER = f"^{SMP1_NAME}$"

    UPDTR_INTERVAL = 1000000
    UPDTR_OFFSET = 100000

    agg_common_config = [
        "prdcr_start_regex regex=.*",
        f"updtr_add name=all interval={UPDTR_INTERVAL} offset={UPDTR_OFFSET}",
        "updtr_prdcr_add name=all regex=.*",
        "updtr_start name=all",
        f"load name={STORE_FLATFILE}",
        f"config name={STORE_FLATFILE} path={STORE_PATH}"
    ]

    def __metric_def(d):
        return f"{d['name']}:{d['mtype']}:{d['vtype']}:{d['init']}"

    spec = {
        "type" : "NA",
        "templates" : {
            "ldmsd-base" : {
                "type" : "ldmsd",
                "listen" : [
                    {"port" : LDMSD_PORT, "xprt" : LDMSD_XPRT }
                ]
            },
            "prdcr" : {
                "xprt" : LDMSD_XPRT,
                "port" : LDMSD_PORT,
                "type" : "active",
                "interval" : 1000000,
            },
            "ldmsd-sampler" : {
                "!extends" : "ldmsd-base",
                "config" : [
                    f"load name={SMP_PLUGN_NAME}",
                    f"config name={SMP_PLUGN_NAME} action=add_schema " \
                            f"schema={SMP_SCHEMA_1_NAME} " \
                            f"metrics={__metric_def(SMP_SCHEMA_1_METRIC1)}," \
                                    f"{__metric_def(SMP_SCHEMA_1_METRIC2)}," \
                                    f"{__metric_def(SMP_SCHEMA_1_METRIC3)}",
                    f"config name={SMP_PLUGN_NAME} action=add_set instance=%hostname%/{SMP_SCHEMA_1_NAME} schema={SMP_SCHEMA_1_NAME} producer=%hostname%",
                    f"config name={SMP_PLUGN_NAME} action=default schema={SMP_SCHEMA_2_NAME} num_metrics={SMP_DEFAULT_NUM_METRICS}",
                    f"config name={SMP_PLUGN_NAME} action=add_set instance=%hostname%/{SMP_SCHEMA_2_NAME} schema={SMP_SCHEMA_2_NAME} producer=%hostname%",
                    f"start name={SMP_PLUGN_NAME} interval={SAMPLE_INTRVL} offset={SAMPLE_OFFSET}"
                ]
            },
            "compute-node" : {
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "samplerd",
                        "!extends" : "ldmsd-sampler"
                    }
                ]
            },
            "prdcr" : {
                "host" : "%name%",
                "type" : "active",
                "xprt" : LDMSD_XPRT,
                "port" : LDMSD_PORT,
                "interval" : 1000000,
            },
        }, # templates
        "nodes" : [
            {
                "hostname" : f"{SMP1_NAME}",
                "component_id" : 1,
                "!extends" : "compute-node"
            },
            {
                "hostname" : f"{SMP2_NAME}",
                "component_id" : 2,
                "!extends" : "compute-node"
            },
            {
                "hostname" : "agg_status",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_SINGLE_PRDCR_NAME} container={STRGP_SINGLE_PRDCR_NAME} plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_prdcr_add name={STRGP_SINGLE_PRDCR_NAME} regex=^{STRGP_SINGLE_PRDCR_PRDCR_NAME}$",
                            f"strgp_start name={STRGP_SINGLE_PRDCR_NAME}",
                            f"strgp_add name={STRGP_SINGLE_METRIC_NAME} container={STRGP_SINGLE_METRIC_NAME} plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_metric_add name={STRGP_SINGLE_METRIC_NAME} metric={SMP_SCHEMA_1_METRIC1['name']}",
                            f"strgp_start name={STRGP_SINGLE_METRIC_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} plugin={STORE_FLATFILE} schema={SMP_SCHEMA_2_NAME}",
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_add",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            f"load name={STORE_CSV} path={STORE_PATH}/csv",
                            # strgp
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_prdcr_add",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            # strgp
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_add name={STRGP_SINGLE_PRDCR_NAME} container={STRGP_SINGLE_PRDCR_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_metric_add",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            # strgp
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_add name={STRGP_SINGLE_METRIC_NAME} container={STRGP_SINGLE_METRIC_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_start",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            # plugin
                            f"load name={STORE_CSV}",
                            # strgp
                            f"strgp_add name={STRGP_INVALID_NAME} container={STRGP_INVALID_NAME} " \
                                f"plugin={STORE_CSV} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_add name={STRGP_SINGLE_PRDCR_NAME} container={STRGP_SINGLE_PRDCR_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_prdcr_add name={STRGP_SINGLE_PRDCR_NAME} regex={STRGP_PRDCR_FILTER}",
                            f"strgp_add name={STRGP_SINGLE_METRIC_NAME} container={STRGP_SINGLE_METRIC_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_metric_add name={STRGP_SINGLE_METRIC_NAME} metric={STRGP_METRIC_FILTER}"
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_prdcr_del",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            # strgp
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_add name={STRGP_SINGLE_PRDCR_NAME} container={STRGP_SINGLE_PRDCR_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_prdcr_add name={STRGP_SINGLE_PRDCR_NAME} regex={STRGP_PRDCR_FILTER}"
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_metric_del",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            # strgp
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_add name={STRGP_SINGLE_METRIC_NAME} container={STRGP_SINGLE_METRIC_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_metric_add name={STRGP_SINGLE_METRIC_NAME} metric={STRGP_METRIC_FILTER}",
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_stop",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            # strgp
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                        ]
                    }
                ]
            },
            {
                "hostname" : "agg_del",
                "daemons" : [
                    {
                        "name" : 'sshd',
                        "type" : "sshd"
                    },
                    {
                        "name" : "agg",
                        "!extends" : "ldmsd-base",
                        "prdcrs" : [
                            {
                                "name" : "samplerd-1",
                                "!extends" : "prdcr"
                            },
                            {
                                "name" : "samplerd-2",
                                "!extends" : "prdcr"
                            }
                        ],
                        "config" : agg_common_config + [
                            # strgp
                            f"strgp_add name={STRGP_ALL_NAME} container={STRGP_ALL_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                            f"strgp_start name={STRGP_ALL_NAME}",
                            f"strgp_add name={STRGP_STOPPED_NAME} container={STRGP_STOPPED_NAME} " \
                                f"plugin={STORE_FLATFILE} schema={SMP_SCHEMA_1_NAME}",
                        ]
                    }
                ]
            }
        ]
    }

    assertions = [
        # strgp_status
        ("status-1", "LDMSD has no storage policies"),
        ("status-2", "Get the status of a non-existing storage policy"),
        ("status-3", "Get the status of a storage policy with a single producer"),
        ("status-4", "Get the status of a storage policy with a single metric"),
        ("status-5", "Sending strgp_status with no attributes"),
        ("status-6", "Get the status of a stopped storage policy"),
        # strgp_add
        ("add-1.1", "Add a new strgp -- checking the error code"),
        ("add-1.2", "Add a new strgp -- checking the status"),
        ("add-2", "Add an existing strgp"),
        # strgp_prdcr_add
        ("prdcr_add-1", "strgp_prdcr_add with an invalid regex"),
        ("prdcr_add-2", "strgp_prdcr_add to a non-existing strgp"),
        ("prdcr_add-3", "strgp_prdcr_add to a running strgp"),
        ("prdcr_add-4.1", "strgp_prdcr_add to a strgp -- checking the error code"),
        ("prdcr_add-4.2", "strgp_prdcr_add to a strgp -- checking the status"),
        # strgp_metric_add
        ("metric_add-1", "strgp_metric_add to a non existing strgp"),
        ("metric_add-2", "strgp_metric_add to a running strgp"),
        ("metric_add-3.1", "strgp_metric_add to a stopped strgp -- checking the errcode"),
        ("metric_add-3.2", "strgp_metric_add to a stopped strgp -- checking the status"),
        # strgp_start
        ("start-1", "strgp_start a non existing strgp"),
        ("start-2", "strgp_start a running strgp"),
        ("start-3", "strgp_start a strgp with a non-configured plugin"),
        ("start-4.1", "strgp_start a strgp with a producer filter -- checking the errcode"),
        ("start-4.2", "strgp_start a strgp with a producer filter -- checking the status"),
        ("start-5.1", "strgp_start a strgp with a metric filter -- checking the errcode"),
        ("start-5.2", "strgp_start a strgp with a metric filter -- checking the status"),
        ("start-6.1", "strgp_start a stopped strgp -- checking the errcode"),
        ("start-6.2", "strgp_start a stopped strgp -- checking the status"),
        ("start-6.3", "strgp_start a stopped strgp -- checking the database"),
        # strgp_prdcr_del
        ("prdcr_del-1", "strgp_prdcr_del a non existing strgp"),
        ("prdcr_del-2", "strgp_prdcr_del a running strgp"),
        ("prdcr_del-3", "strgp_prdcr_del a strgp that doesn't have the prdcr regex"),
        ("prdcr_del-4.1", "strgp_prdcr_del a strgp with a producer filter -- checking the errcode"),
        ("prdcr_del-4.2", "strgp_prdcr_del a strgp with a producer filter -- checking the status"),
        # strgp_metric_del
        ("metric_del-1", "strgp_metric_del a non-existing strgp"),
        ("metric_del-2", "strgp_metric_del a running strgp"),
        ("metric_del-3", "strgp_metric_del a strgp that doesn't contain the metric name"),
        ("metric_del-4.1", "strgp_metric_del from a strgp -- checking the errcode"),
        ("metric_del-4.2", "strgp_metric_del from a strgp -- checking the status"),
        # strgp_stop
        ("stop-1", "strgp_stop a non existing strgp"),
        ("stop-2", "strgp_stop a stopped strgp"),
        ("stop-3.1", "strgp_stop a running strgp -- checking the errcode"),
        ("stop-3.2", "strgp_stop a running strgp -- checking the status"),
        # strgp_del
        ("del-1", "Delete a non-existing strgp"),
        ("del-2", "Delete a running strgp"),
        ("del-3.1", "Delete a stopped strgp -- checking the errcode"),
        ("del-3.2", "Delete a stopped strgp -- checking the status"),
    ]

def errcode_cond(resp, exp_errcode, op = "=="):
    if op == "==":
        cond = resp['errcode'] == exp_errcode
    elif op == "!=":
        cond = resp['errcode'] != exp_errcode
    return {
            "cond" : cond,
            "cond_str" :f"resp['errcode'] ({resp['errcode']}) {op} {exp_errcode}"
           }

def strgp_status_cond(status, exp):
    a = sorted(status, key = lambda item: sorted(item.items()))
    b = sorted(exp, key = lambda item: sorted(item.items()))
    cond = a == b
    return {
                "cond" : cond,
                "cond_str": "status is as expected" if cond else f"{a} == {b}"
           }

def exp_status(name, container, schema, plugin, state, prdcrs = None,
                         metrics = None, regex = None, decomp = None):
    d = {
            "name" : name,
            "container" : container,
            "schema" : schema,
            "plugin" : plugin,
            "flush" : '0.000000',
            "state" : state.upper(),
        }
    if prdcrs:
        d['producers'] = prdcrs
    else:
        d['producers'] = []
    if metrics:
        d['metrics'] = metrics
    else:
        d['metrics'] = []
    if regex is None:
        d['regex'] = "-"
    else:
        d['regex' ] = regex
    if decomp is None:
        d['decomp'] = "-"
    else:
        d['decomp'] = decomp
    return d

def strgp_status(comm, name = None):
    errcode, msg = comm.strgp_status(name = name)
    return ({'errcode' : errcode, 'msg' : msg},
            json.loads(msg) if errcode == 0 else None)

def strgp_add(comm, name, container, schema, plugin):
    errcode, msg = comm.strgp_add(name = name, container = container, schema = schema, plugin = plugin)
    return {'errcode': errcode, 'msg': msg}

def strgp_prdcr_add(comm, name, regex):
    errcode, msg = comm.strgp_prdcr_add(name = name, regex = regex)
    return {'errcode': errcode, 'msg': msg}

def strgp_metric_add(comm, name, metric):
    errcode, msg = comm.strgp_metric_add(name = name, metric_name = metric)
    return {'errcode': errcode, 'msg': msg}

def strgp_start(comm, name):
    errcode, msg = comm.strgp_start(name = name)
    return {'errcode': errcode, 'msg' : msg}

def strgp_stop(comm, name):
    errcode, msg = comm.strgp_stop(name = name)
    return {'errcode': errcode, 'msg' : msg}

def strgp_prdcr_del(comm, name, regex):
    errcode, msg = comm.strgp_prdcr_del(name = name, regex = regex)
    return {'errcode': errcode, 'msg': msg}

def strgp_metric_del(comm, name, metric):
    errcode, msg = comm.strgp_metric_del(name = name, metric_name = metric)
    return {'errcode': errcode, 'msg': msg}

def strgp_del(comm, name):
    errcode, msg = comm.strgp_del(name = name)
    return {'errcode': errcode, 'msg': msg}

def get_comm(host, xprt, port):
    from ldmsd.ldmsd_communicator import Communicator
    comm = Communicator(host = host, xprt = xprt, port = port)
    rc = comm.connect()
    if rc != 0:
        raise RuntimeError(f"Failed to connect to the ldmsd. Error {rc}")
    return comm

def strgp_status_test(suite):
    comm = get_comm(host = suite.SMP1_NAME, xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # No storage policies
    resp, status = strgp_status(comm)
    suite.save_assertion("status-1", len(status) == 0, "len(status) == 0")
    comm.close()
    # strgp not existed
    comm = get_comm(host = "agg_status", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    resp, status = strgp_status(comm, name = "foo")
    suite.save_assertion("status-2", **errcode_cond(resp, errno.ENOENT))
    # strgp with a single producer
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_PRDCR_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_PRDCR_NAME,
                      container = suite.STRGP_SINGLE_PRDCR_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "running",
                      prdcrs = [f"^{suite.STRGP_SINGLE_PRDCR_PRDCR_NAME}$"],
                      metrics = ["m1", "d1", "d2"])]
    suite.save_assertion("status-3", **strgp_status_cond(status, exp))
    # strgp with a single metric
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_METRIC_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_METRIC_NAME,
                                container = suite.STRGP_SINGLE_METRIC_NAME,
                                schema = suite.SMP_SCHEMA_1_NAME,
                                plugin = suite.STORE_FLATFILE,
                                state = "running",
                                metrics = [suite.SMP_SCHEMA_1_METRIC1["name"]])]
    suite.save_assertion("status-4", **strgp_status_cond(status, exp))
    # strgp with no arguments
    resp, status = strgp_status(comm)
    exp = [exp_status(name = suite.STRGP_ALL_NAME,
                      container = suite.STRGP_ALL_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE, state = "running",
                      metrics = ["m1", "d1", "d2"]),
           exp_status(name = suite.STRGP_SINGLE_METRIC_NAME,
                      container = suite.STRGP_SINGLE_METRIC_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "running",
                      metrics = [suite.SMP_SCHEMA_1_METRIC1["name"]]),
           exp_status(name = suite.STRGP_SINGLE_PRDCR_NAME,
                      container = suite.STRGP_SINGLE_PRDCR_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "running",
                      prdcrs = [f"^{suite.STRGP_SINGLE_PRDCR_PRDCR_NAME}$"],
                      metrics = ["m1", "d1", "d2"]),
           exp_status(name = suite.STRGP_STOPPED_NAME,
                      container = suite.STRGP_STOPPED_NAME,
                      schema = suite.SMP_SCHEMA_2_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "stopped")
          ]
    suite.save_assertion("status-5", **strgp_status_cond(status, exp))
    # stopped strgp
    resp, status = strgp_status(comm, name = suite.STRGP_STOPPED_NAME)
    exp = [exp_status(name = suite.STRGP_STOPPED_NAME,
                      container = suite.STRGP_STOPPED_NAME,
                      schema = suite.SMP_SCHEMA_2_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "stopped",
                      prdcrs = [],
                      metrics = [])]
    suite.save_assertion("status-6", **strgp_status_cond(status, exp))
    comm.close()

def strgp_add_test(suite):
    comm = get_comm(host = "agg_add", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # non existing strgp
    resp = strgp_add(comm, name = "foo", container = "foo",
                     schema = suite.SMP_SCHEMA_1_NAME, plugin = suite.STORE_CSV)
    suite.save_assertion("add-1.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = "foo")
    exp = [exp_status(name = "foo", container = "foo",
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_CSV,
                      state = "STOPPED")]
    suite.save_assertion("add-1.2", **strgp_status_cond(status, exp))
    # existing strgp
    resp = strgp_add(comm, name = suite.STRGP_ALL_NAME,
                     container = suite.STRGP_ALL_NAME,
                     plugin = suite.STORE_CSV, schema = suite.SMP_SCHEMA_2_NAME)
    suite.save_assertion("add-2", **errcode_cond(resp, errno.EEXIST))
    comm.close()

def strgp_prdcr_add_test(suite):
    comm = get_comm(host = "agg_prdcr_add", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # Invalid regex
    resp = strgp_prdcr_add(comm, name = suite.STRGP_STOPPED_NAME, regex = "[")
    suite.save_assertion("prdcr_add-1", **errcode_cond(resp, 0, op = "!="))
    # non existing strgp
    resp = strgp_prdcr_add(comm, name = "foo", regex = ".*")
    suite.save_assertion("prdcr_add-2", **errcode_cond(resp, errno.ENOENT))
    # running strgp
    resp = strgp_prdcr_add(comm, name = suite.STRGP_ALL_NAME, regex=".*")
    suite.save_assertion("prdcr_add-3", **errcode_cond(resp, errno.EBUSY))
    # Filter with prdcr
    regex = f"^{suite.SMP1_NAME}$"
    resp = strgp_prdcr_add(comm, name = suite.STRGP_SINGLE_PRDCR_NAME, regex=regex)
    suite.save_assertion("prdcr_add-4.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_PRDCR_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_PRDCR_NAME,
                      container = suite.STRGP_SINGLE_PRDCR_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "STOPPED",
                      prdcrs = [regex])]
    suite.save_assertion("prdcr_add-4.2", **strgp_status_cond(status, exp))

    comm.close()

def strgp_metric_add_test(suite):
    comm = get_comm(host = "agg_metric_add", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)

    # non existing strgp
    resp = strgp_metric_add(comm, name = "foo", metric = "a")
    suite.save_assertion("metric_add-1", **errcode_cond(resp, errno.ENOENT))
    # running strgp
    resp = strgp_metric_add(comm, name = suite.STRGP_ALL_NAME, metric = "a")
    suite.save_assertion("metric_add-2", **errcode_cond(resp, errno.EBUSY))
    # Add a metric to a strgp
    resp = strgp_metric_add(comm, name = suite.STRGP_SINGLE_METRIC_NAME, metric = "d1")
    suite.save_assertion("metric_add-3.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_METRIC_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_METRIC_NAME,
                      container = suite.STRGP_SINGLE_METRIC_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "STOPPED", metrics = ["d1"])]
    suite.save_assertion("metric_add-3.2", **strgp_status_cond(status, exp))

    comm.close()

def strgp_start_test(suite):
    comm = get_comm(host = "agg_start", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # non existing strgp
    resp = strgp_start(comm, name = "foo")
    suite.save_assertion("start-1", **errcode_cond(resp, errno.ENOENT))
    # running strgp
    resp = strgp_start(comm, name = suite.STRGP_ALL_NAME)
    suite.save_assertion("start-2", **errcode_cond(resp, errno.EBUSY))
    # start a strgp that uses a non configured plugin
    resp = strgp_start(comm, name = suite.STRGP_INVALID_NAME)
    suite.save_assertion("start-3", **errcode_cond(resp, 0))
    # Start a strgp with a producer filter
    resp = strgp_start(comm, name = suite.STRGP_SINGLE_PRDCR_NAME)
    suite.save_assertion("start-4.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_PRDCR_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_PRDCR_NAME,
                      container = suite.STRGP_SINGLE_PRDCR_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "RUNNING",
                      prdcrs = [suite.STRGP_PRDCR_FILTER],
                      metrics = ["m1", "d1", "d2"])]
    suite.save_assertion("start-4.2", **strgp_status_cond(status, exp))
    #Start a strgp with a metric filter
    resp = strgp_start(comm, name = suite.STRGP_SINGLE_METRIC_NAME)
    suite.save_assertion("start-5.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_METRIC_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_METRIC_NAME,
                      container = suite.STRGP_SINGLE_METRIC_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "RUNNING",
                      metrics = ["d1"])]
    suite.save_assertion("start-5.2", **strgp_status_cond(status, exp))
    # Start a stopped strgp
    resp = strgp_start(comm, name = suite.STRGP_STOPPED_NAME)
    suite.save_assertion("start-6.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_STOPPED_NAME)
    exp = [exp_status(name = suite.STRGP_STOPPED_NAME,
                      container = suite.STRGP_STOPPED_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "RUNNING",
                      metrics = ["m1", "d1", "d2"])]
    suite.save_assertion("start-6.2", **strgp_status_cond(status, exp))
    sleep(1)
    with open(f"/db/agg_start/{suite.STRGP_ALL_NAME}/{suite.SMP_SCHEMA_1_NAME}/d1", "r") as fin:
        fin.seek(0, 2)
        sleep(2)
        line = fin.read()
        suite.save_assertion('start-6.3', line is not None, "Database is not empty.")

    comm.close()

def strgp_prdcr_del_test(suite):
    comm = get_comm(host = "agg_prdcr_del", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # non existing strgp
    resp = strgp_prdcr_del(comm, name = "foo", regex = ".*")
    suite.save_assertion("prdcr_del-1", **errcode_cond(resp, errno.ENOENT))
    # running strgp
    resp = strgp_prdcr_del(comm, name = suite.STRGP_ALL_NAME, regex = ".*")
    suite.save_assertion("prdcr_del-2", **errcode_cond(resp, errno.EBUSY))
    # non existing prdcr
    resp = strgp_prdcr_del(comm, name = suite.STRGP_STOPPED_NAME, regex = ".*")
    suite.save_assertion("prdcr_del-3", **errcode_cond(resp, errno.ENOENT))
    # Remove a producer regex
    resp = strgp_prdcr_del(comm, name = suite.STRGP_SINGLE_PRDCR_NAME, regex = f"^{suite.SMP1_NAME}$")
    suite.save_assertion("prdcr_del-4.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_PRDCR_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_PRDCR_NAME,
                      container = suite.STRGP_SINGLE_PRDCR_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "STOPPED")]
    suite.save_assertion("prdcr_del-4.2", **strgp_status_cond(status, exp))
    comm.close()

def strgp_metric_del_test(suite):
    comm = get_comm(host = "agg_metric_del", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # non existing strgp
    resp = strgp_metric_del(comm, name = "foo", metric = 'a')
    suite.save_assertion("metric_del-1", **errcode_cond(resp, errno.ENOENT))
    # running strgp
    resp = strgp_metric_del(comm, name = suite.STRGP_ALL_NAME, metric = "a")
    suite.save_assertion("metric_del-2", **errcode_cond(resp, errno.EBUSY))
    # non existing metric
    resp = strgp_metric_del(comm, name = suite.STRGP_STOPPED_NAME, metric = "foo")
    suite.save_assertion("metric_del-3", **errcode_cond(resp, errno.ENOENT))
    # Remove a metric filter
    resp = strgp_metric_del(comm, name = suite.STRGP_SINGLE_METRIC_NAME, metric = suite.STRGP_METRIC_FILTER)
    suite.save_assertion("metric_del-4.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_SINGLE_METRIC_NAME)
    exp = [exp_status(name = suite.STRGP_SINGLE_METRIC_NAME,
                      container = suite.STRGP_SINGLE_METRIC_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = 'STOPPED')]
    suite.save_assertion("metric_del-4.2", **strgp_status_cond(status, exp))

    comm.close()

def strgp_stop_test(suite):
    comm = get_comm(host = "agg_stop", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # non existing strgp
    resp = strgp_stop(comm, name = "foo")
    suite.save_assertion("stop-1", **errcode_cond(resp, errno.ENOENT))
    # stopped strgp
    resp = strgp_stop(comm, name = suite.STRGP_STOPPED_NAME)
    suite.save_assertion("stop-2", **errcode_cond(resp, errno.EBUSY))
    # stopped a running strgp
    resp = strgp_stop(comm, name = suite.STRGP_ALL_NAME)
    suite.save_assertion("stop-3.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm, name = suite.STRGP_ALL_NAME)
    exp = [exp_status(name = suite.STRGP_ALL_NAME,
                      container = suite.STRGP_ALL_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "STOPPED",
                      metrics = ["m1", "d1", "d2"])]
    suite.save_assertion("stop-3.2", **strgp_status_cond(status, exp))

    comm.close()

def strgp_del_test(suite):
    comm = get_comm(host = "agg_del", xprt = suite.LDMSD_XPRT,
                             port = suite.LDMSD_PORT)
    # non existing strgp
    resp = strgp_del(comm, name = "foo")
    suite.save_assertion("del-1", **errcode_cond(resp, errno.ENOENT))
    # running strgp
    resp = strgp_del(comm, name = suite.STRGP_ALL_NAME)
    suite.save_assertion("del-2", **errcode_cond(resp, errno.EBUSY))
    # Delete a stopped strgp
    resp = strgp_del(comm, name = suite.STRGP_STOPPED_NAME)
    suite.save_assertion("del-3.1", **errcode_cond(resp, 0))
    resp, status = strgp_status(comm)
    exp = [exp_status(name = suite.STRGP_ALL_NAME,
                      container = suite.STRGP_ALL_NAME,
                      schema = suite.SMP_SCHEMA_1_NAME,
                      plugin = suite.STORE_FLATFILE,
                      state = "RUNNING",
                      metrics = ["m1", "d1", "d2"])]
    suite.save_assertion("del-3.2", **strgp_status_cond(status, exp))

    comm.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--outdir")
    args = ap.parse_args()

    suite = StrgpConfigCMDTest(args.outdir)

    strgp_status_test(suite)
    strgp_add_test(suite)
    strgp_prdcr_add_test(suite)
    strgp_metric_add_test(suite)
    strgp_start_test(suite)
    strgp_prdcr_del_test(suite)
    strgp_metric_del_test(suite)
    strgp_stop_test(suite)
    strgp_del_test(suite)
