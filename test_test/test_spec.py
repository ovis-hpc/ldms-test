#!/usr/bin/python3

import os
import pdb
import sys
import json
import TADA
import argparse

from distutils.spawn import find_executable

from LDMS_Test import Spec, jprint, get_ovis_commit_id

if __name__ != "__main__":
    raise RuntimeError("This is not a module.")

# NOTE
# ----
# This test case illustrates abilities of Spec extension and substitution
# mechanics. A Spec is a JSON-compatible dictionary, its members could only be
# int, str, list of JSON-compatible objects, or dict of JSON-compatible objects.
#
# Top-level "templates" attribute of Spec is reserved to define Templates that
# could be applied to objects (dict) in the Spec using another reserved
# attribute keyword "!extends". The "%VAR%" (e.g. "/bin/%prog%") in the string
# values inside Spec will also be substituted by the attribute of the nearest
# parent objects (including self).
#
# The objects are recursively extended first, and the substitution is processed
# in those objects afterward.

exec(open(os.getenv("PYTHONSTARTUP", "/dev/null")).read())

spec = {
    "XPRT": "sock",
    "AUTH": "none",
    "templates": {
        "compute-node": {
            "daemons": [
                {
                    "name": "sshd",
                    "type": "sshd",
                },
                {
                    "name": "sampler",
                    "!extends": "ldmsd-sampler",
                },
            ],
        },
        "ldmsd-base": {
            "type" : "ldmsd",
            "listen_port" : 10000,
            "listen_xprt" : "%XPRT%",
            "listen_auth" : "%AUTH%",
        },
        "ldmsd-sampler": {
            "!extends" : "ldmsd-base",
            "samplers": [
                {
                    "plugin": "meminfo",
                    "!extends": "sampler-common",
                },
                {
                    "plugin": "vmstat",
                    "!extends": "sampler-common",
                    "interval": 2000000, # override
                },
            ],
        },
        "sampler-common": {
            "interval": 1000000,
            "offset": 0,
            "config": [
                "component_id=%component_id%",
                "instance=%hostname%/%plugin%",
                "producer=%hostname%",
            ],
            "start": True,
        },
        "prdcr-base": {
            "host" : "%name%",
            "port" : 10000,
            "xprt" : "%XPRT%",
            "type" : "active",
            "interval" : 1000000,
        },
    },
    "nodes": [
        {
            "hostname": "samp-1",
            "component_id": 10001,
            "!extends": "compute-node",
        },
        {
            "hostname": "samp-2",
            "component_id": 10002,
            "!extends": "compute-node",
        },
        {
            "hostname": "agg-1",
            "daemons": [
                {
                    "name": "agg-1",
                    "!extends": "ldmsd-base",
                    "prdcrs": [
                        {
                            "name" : "samp-1",
                            "!extends" : "prdcr-base",
                        },
                        {
                            "name" : "samp-2",
                            "!extends" : "prdcr-base",
                        },
                    ],
                },
            ],
        },
        {
            "hostname": "agg-2",
            "daemons": [
                {
                    "name": "agg-2",
                    "!extends": "ldmsd-base",
                    "prdcrs": [
                        {
                            "name" : "agg-1",
                            "!extends" : "prdcr-base",
                        },
                    ],
                },
            ],
        },
    ],
}

expected = {
    "XPRT": "sock",
    "AUTH": "none",
    "templates": {
        "compute-node": {
            "daemons": [
                {
                    "name": "sshd",
                    "type": "sshd",
                },
                {
                    "name": "sampler",
                    "!extends": "ldmsd-sampler",
                },
            ],
        },
        "ldmsd-base": {
            "type" : "ldmsd",
            "listen_port" : 10000,
            "listen_xprt" : "%XPRT%",
            "listen_auth" : "%AUTH%",
        },
        "ldmsd-sampler": {
            "!extends" : "ldmsd-base",
            "samplers": [
                {
                    "plugin": "meminfo",
                    "!extends": "sampler-common",
                },
                {
                    "plugin": "vmstat",
                    "!extends": "sampler-common",
                    "interval": 2000000, # override
                },
            ],
        },
        "sampler-common": {
            "interval": 1000000,
            "offset": 0,
            "config": [
                "component_id=%component_id%",
                "instance=%hostname%/%plugin%",
                "producer=%hostname%",
            ],
            "start": True,
        },
        "prdcr-base": {
            "host" : "%name%",
            "port" : 10000,
            "xprt" : "%XPRT%",
            "type" : "active",
            "interval" : 1000000,
        },
    },
    "nodes": [
        {
            "hostname": "samp-1",
            "component_id": 10001,
            "daemons": [
                {
                    "name": "sshd",
                    "type": "sshd",
                },
                {
                    "name": "sampler",
                    "type" : "ldmsd",
                    "listen_port" : 10000,
                    "listen_xprt" : "sock",
                    "listen_auth" : "none",
                    "samplers": [
                        {
                            "plugin": "meminfo",
                            "interval": 1000000,
                            "offset": 0,
                            "config": [
                                "component_id=10001",
                                "instance=samp-1/meminfo",
                                "producer=samp-1",
                            ],
                            "start": True,
                        },
                        {
                            "plugin": "vmstat",
                            "offset": 0,
                            "interval": 2000000,
                            "config": [
                                "component_id=10001",
                                "instance=samp-1/vmstat",
                                "producer=samp-1",
                            ],
                            "start": True,
                        },
                    ],
                },
            ],
        },
        {
            "hostname": "samp-2",
            "component_id": 10002,
            "daemons": [
                {
                    "name": "sshd",
                    "type": "sshd",
                },
                {
                    "name": "sampler",
                    "type" : "ldmsd",
                    "listen_port" : 10000,
                    "listen_xprt" : "sock",
                    "listen_auth" : "none",
                    "samplers": [
                        {
                            "plugin": "meminfo",
                            "interval": 1000000,
                            "offset": 0,
                            "config": [
                                "component_id=10002",
                                "instance=samp-2/meminfo",
                                "producer=samp-2",
                            ],
                            "start": True,
                        },
                        {
                            "plugin": "vmstat",
                            "offset": 0,
                            "interval": 2000000,
                            "config": [
                                "component_id=10002",
                                "instance=samp-2/vmstat",
                                "producer=samp-2",
                            ],
                            "start": True,
                        },
                    ],
                },
            ],
        },
        {
            "hostname": "agg-1",
            "daemons": [
                {
                    "name": "agg-1",
                    "type" : "ldmsd",
                    "listen_port" : 10000,
                    "listen_xprt" : "sock",
                    "listen_auth" : "none",
                    "prdcrs": [
                        {
                            "name" : "samp-1",
                            "host" : "samp-1",
                            "port" : 10000,
                            "xprt" : "sock",
                            "type" : "active",
                            "interval" : 1000000,
                        },
                        {
                            "name" : "samp-2",
                            "host" : "samp-2",
                            "port" : 10000,
                            "xprt" : "sock",
                            "type" : "active",
                            "interval" : 1000000,
                        },
                    ],
                },
            ],
        },
        {
            "hostname": "agg-2",
            "daemons": [
                {
                    "name": "agg-2",
                    "type" : "ldmsd",
                    "listen_port" : 10000,
                    "listen_xprt" : "sock",
                    "listen_auth" : "none",
                    "prdcrs": [
                        {
                            "name" : "agg-1",
                            "host" : "agg-1",
                            "port" : 10000,
                            "xprt" : "sock",
                            "type" : "active",
                            "interval" : 1000000,
                        },
                    ],
                },
            ],
        },
    ],
}

spec = Spec(spec)

assert(spec == expected)
