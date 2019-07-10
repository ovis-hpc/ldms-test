#!/usr/bin/env python

import os
import re
import sys
import json
import time
import docker

from LDMS_Test import LDMSDCluster, LDMSDContainer, TADATest

_pystart = os.getenv('PYTHONSTARTUP')
if _pystart:
    execfile(_pystart)

def jprint(obj):
    """Pretty print JSON object"""
    print json.dumps(obj, indent=2)

dc = docker.from_env()
USER = os.getlogin()

spec = {
    "name" : "{}-cluster".format(USER),
    "description" : "{} test cluster".format(USER),
    "type" : "NA",
    "define" : [
        {
            "name" : "sampler-daemon",
            "type" : "sampler",
            "listen_port" : 10000,
            "listen_xprt" : "sock",
            "listen_auth" : "none",
            "env" : [
                "INTERVAL=1000000",
                "OFFSET=0"
            ],
            "samplers" : [
                {
                    "plugin" : "slurm_sampler",
                    "config" : [
                        "instance=${HOSTNAME}/%plugin%",
                        "producer=${HOSTNAME}",
                    ]
                },
                {
                    "plugin" : "meminfo",
                    "config" : [
                        "instance=${HOSTNAME}/%plugin%",
                        "producer=${HOSTNAME}"
                    ],
                    "start" : True
                },
                {
                    "plugin" : "vmstat",
                    "config" : [
                        "instance=${HOSTNAME}/%plugin%",
                        "producer=${HOSTNAME}"
                    ],
                    "start" : True
                },
                {
                    "plugin" : "procstat",
                    "config" : [
                        "instance=${HOSTNAME}/%plugin%",
                        "producer=${HOSTNAME}"
                    ],
                    "start" : True
                }
            ]
        }
    ],
    "daemons" : [
        {
            "host" : "sampler-1",
            "asset" : "sampler-daemon",
            "env" : {
                "COMPONENT_ID" : "10001",
                "HOSTNAME" : "%host%",
            }
        },
        {
            "host" : "sampler-2",
            "asset" : "sampler-daemon",
            "env" : [
                "COMPONENT_ID=10002",
                "HOSTNAME=%host%"
            ]
        },
        {
            "host" : "agg-1",
            "listen_port" : 20000,
            "listen_xprt" : "sock",
            "listen_auth" : "none",
            "env" : [
                "HOSTNAME=%host%"
            ],
            "config" : [
                "prdcr_add name=sampler-1 host=sampler-1 port=10000 xprt=sock type=active interval=20000000",
                "prdcr_add name=sampler-2 host=sampler-2 port=10000 xprt=sock type=active interval=20000000",
                "prdcr_start_regex regex=.*",
                "updtr_add name=all interval=1000000 offset=0",
                "updtr_prdcr_add name=all regex=.*",
                "updtr_start name=all"
            ]
        },
        {
            "host" : "agg-2",
            "listen_port" : 20001,
            "listen_xprt" : "sock",
            "listen_auth" : "none",
            "env" : [
                "HOSTNAME=%host%"
            ],
            "config" : [
                "prdcr_add name=agg-1 host=agg-1 port=20000 xprt=sock type=active interval=20000000",
                "prdcr_start_regex regex=.*",
                "updtr_add name=all interval=1000000 offset=0",
                "updtr_prdcr_add name=all regex=.*",
                "updtr_start name=all"
            ]
        }
    ],

    #"image": "ovis-centos-build:slurm",
    "image": "ovis-centos-build",
    "ovis_prefix": "/home/narate/opt/ovis",
    "env" : { "FOO": "BAR" },
    "mounts": [
        "{}/db:/db:rw".format(os.path.realpath(sys.path[0])),
    ]
}

test = TADATest("LDMSD", "LDMSD", "agg + slurm_sampler + slurm_notifier")
test.add_assertion(1, "ldms_ls agg-2")
test.add_assertion(2, "slurm job_id verification on sampler-1")
test.add_assertion(3, "slurm job_id verification on sampler-2")

test.start()

print "-- Get or create the cluster --"
cluster = LDMSDCluster.get(spec["name"], create = True, spec = spec)

print "-- start/check sshd --"
cluster.start_sshd()
cluster.make_known_hosts()
print "-- start/check munged --"
cluster.start_munged()
# prep plugstack.conf for slurm_notifier
plugstack = "required /opt/ovis/lib/ovis-ldms/libslurm_notifier.so auth=none port=10000"
for cont in cluster.containers:
    cont.write_file("/etc/slurm/plugstack.conf", plugstack)
cluster.start_slurm()

print "-- start/check ldmsd --"
cluster.start_ldmsd()

print "... wait a bit to make sure ldmsd's are up"
time.sleep(5)

# agg1 = cluster.get_container("agg-1")
# samp1 = cluster.get_container("sampler-1")
cont = cluster.containers[-1] # the last node is the free node with no ldms daemon

print "-- ldms_ls to agg-2 --"
rc, out = cont.ldms_ls("-x sock -p 20001 -h agg-2")
print out

expect = set([ "{}/{}".format(p, s) for p in ["sampler-1", "sampler-2"] \
                                   for s in ["slurm_sampler", "vmstat",
                                             "procstat", "meminfo"] ])
result = set(out.splitlines())

def verify(num, cond, cond_str):
    a = test.assertions[num]
    print a["assert-desc"] + ": " + ("Passed" if cond else "Failed")
    test.assert_test(num, cond, cond_str)

verify(1, expect == result, "dir result verified")

# Now, test slurm_sampler for new job
for job in cluster.squeue():
    print "Cancelling job {}".format(job["JOBID"])
    cluster.scancel(job["JOBID"])

print "Submitting job ..."
jobid = cluster.sbatch("/db/job.sh")
time.sleep(3) # enough time to update the jobid
print "jobid: {}".format(jobid)

rc, out = cluster.ldms_ls("-x sock -p 20001 -h agg-2 -l sampler-1/slurm_sampler")
_set = set(map(int, re.search(r'job_id\s+(\S+)', out).group(1).split(',')))
verify(2, jobid in _set, "job_id verified")

rc, out = cluster.ldms_ls("-x sock -p 20001 -h agg-2 -l sampler-2/slurm_sampler")
_set = set(map(int, re.search(r'job_id\s+(\S+)', out).group(1).split(',')))
verify(3, jobid in _set, "job_id verified")

test.finish()

# cluster.remove() # this destroys entire cluster
