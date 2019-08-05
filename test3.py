#!/usr/bin/env python

import os
import re
import pwd
import sys
import json
import time
import docker
import argparse
import TADA

from distutils.spawn import find_executable
from LDMS_Test import LDMSDCluster, LDMSDContainer

if __name__ != "__main__":
    raise RuntimeError("This should not be impoarted as a module.")

execfile(os.getenv("PYTHONSTARTUP", "/dev/null"))

def jprint(obj):
    """Pretty print JSON object"""
    print json.dumps(obj, indent=2)

def get_ovis_commit_id(prefix):
    """Get commit_id of the ovis installation"""
    try:
        path = "{}/bin/ldms-pedigree".format(prefix)
        f = open(path)
        for l in f.readlines():
            if l.startswith("echo commit-id: "):
                e, c, commit_id = l.split()
                return commit_id
    except:
        pass
    return "-"

#### default values #### ---------------------------------------------
sbin_ldmsd = find_executable("ldmsd")
if sbin_ldmsd:
    default_prefix, a, b = sbin_ldmsd.rsplit('/', 2)
else:
    default_prefix = "/opt/ovis"

#### argument parsing #### -------------------------------------------
ap = argparse.ArgumentParser(description =
                         "Run test scenario of 2 samplers -> agg-1 -> agg-2 " \
                         "with slurm job ID verification." )
ap.add_argument("--clustername", type = str,
                help = "The name of the cluster. The default is "
                "USER-test3-COMMIT_ID.")
ap.add_argument("--prefix", type = str,
                default = default_prefix,
                help = "The OVIS installation prefix.")
ap.add_argument("--src", type = str,
                help = "The path to OVIS source tree (for gdb). " \
                       "If not specified, src tree won't be mounted.")
ap.add_argument("--db", type = str,
                default = "{}/db".format(os.path.realpath(sys.path[0])),
                help = "The path to host db directory." )
ap.add_argument("--slurm-notifier", type = str,
                default = "__find_from_prefix__",
                help = "The path (in container) to slurm_notifier library." )
ap.add_argument("--tada-addr",
                help="The test automation server host and port as host:port.",
                default="localhost:9862")

args = ap.parse_args()

#### config variables #### ------------------------------
USER = pwd.getpwuid(os.geteuid())[0]
PREFIX = args.prefix
COMMIT_ID = get_ovis_commit_id(PREFIX)
SRC = args.src
CLUSTERNAME = args.clustername if args.clustername else \
              "{}-test3-{:.7}".format(USER, COMMIT_ID)
DB = args.db
SLURM_NOTIFIER = args.slurm_notifier
if SLURM_NOTIFIER == "__find_from_prefix__":
    paths = map(lambda x: "{}/{}/ovis-ldms/libslurm_notifier.so"\
                          .format(PREFIX, x),
                ["lib", "lib64"])
    for p in paths:
        if os.path.exists(p):
            SLURM_NOTIFIER = p.replace(PREFIX, '/opt/ovis', 1)
            break
    else:
        raise RuntimeError("libslurm_notifier.so not found")


#### spec #### -------------------------------------------------------
spec = {
    "name" : CLUSTERNAME,
    "description" : "{}'s test3 cluster".format(USER),
    "type" : "NA",
    "templates" : { # generic template can apply to any object by "!extends"
        "compute-node" : {
            "daemons" : [
                {
                    "name" : "munged",
                    "type" : "munged",
                },
                {
                    "name" : "sampler-daemon",
                    "requires" : [ "munged" ],
                    "!extends" : "ldmsd-sampler",
                },
                {
                    "name" : "slurmd",
                    "requires" : [ "munged" ],
                    "!extends" : "slurmd",
                },
            ],
        },
        "slurmd" : {
            "type" : "slurmd",
            "plugstack" : [
                {
                    "required" : True,
                    "path" : SLURM_NOTIFIER,
                    "args" : [
                        "auth=none",
                        "port=10000",
                    ],
                },
            ],
        },
        "sampler_plugin" : {
            "interval" : 1000000,
            "offset" : 0,
            "config" : [
                "component_id=%component_id%",
                "instance=%hostname%/%plugin%",
                "producer=%hostname%",
            ],
            "start" : True,
        },
        "ldmsd-base" : {
            "type" : "ldmsd",
            "listen_port" : 10000,
            "listen_xprt" : "sock",
            "listen_auth" : "none",
        },
        "ldmsd-sampler" : {
            "!extends" : "ldmsd-base",
            "samplers" : [
                {
                    "plugin" : "slurm_sampler",
                    "!extends" : "sampler_plugin",
                    "start" : False, # override
                },
                {
                    "plugin" : "meminfo",
                    "!extends" : "sampler_plugin",
                },
                {
                    "plugin" : "vmstat",
                    "!extends" : "sampler_plugin",
                },
                {
                    "plugin" : "procstat",
                    "!extends" : "sampler_plugin",
                }
            ],
        },
        "prdcr" : {
            "host" : "%name%",
            "port" : 10000,
            "xprt" : "sock",
            "type" : "active",
            "interval" : 1000000,
        },
    },
    "nodes" : [
        {
            "hostname" : "compute-1",
            "component_id" : 10001,
            "!extends" : "compute-node",
        },
        {
            "hostname" : "compute-2",
            "component_id" : 10002,
            "!extends" : "compute-node",
        },
        {
            "hostname" : "agg-1",
            "daemons" : [
                {
                    "name" : "aggregator",
                    "!extends" : "ldmsd-base",
                    "listen_port" : 20000, # override
                    "prdcrs" : [ # these producers will turn into `prdcr_add`
                        {
                            "name" : "compute-1",
                            "!extends" : "prdcr",
                        },
                        {
                            "name" : "compute-2",
                            "!extends" : "prdcr",
                        },
                    ],
                    "config" : [ # additional config applied after prdcrs
                        "prdcr_start_regex regex=.*",
                        "updtr_add name=all interval=1000000 offset=0",
                        "updtr_prdcr_add name=all regex=.*",
                        "updtr_start name=all"
                    ],
                },
            ]
        },
        {
            "hostname" : "agg-2",
            "daemons" : [
                {
                    "name" : "aggregator",
                    "!extends" : "ldmsd-base",
                    "listen_port" : 20001, # override
                    "config" : [
                        "prdcr_add name=agg-1 host=agg-1 port=20000 "\
                                  "xprt=sock type=active interval=20000000",
                        "prdcr_start_regex regex=.*",
                        "updtr_add name=all interval=1000000 offset=0",
                        "updtr_prdcr_add name=all regex=.*",
                        "updtr_start name=all"
                    ],
                },
            ],
        },
        {
            "hostname" : "headnode",
            "daemons" : [
                {
                    "name" : "slurmctld",
                    "type" : "slurmctld",
                },
            ]
        },
    ],

    #"image": "ovis-centos-build:slurm",
    "cap_add": [ "SYS_PTRACE" ],
    "image": "ovis-centos-build",
    "ovis_prefix": PREFIX,
    "env" : { "FOO": "BAR" },
    "mounts": [
        "{}:/db:rw".format(DB),
    ] +
    ( ["{0}:{0}:ro".format(SRC)] if SRC else [] ),
}

#### test definition ####

test = TADA.Test(test_suite = "LDMSD",
                 test_type = "FVT",
                 test_name = "agg-slurm",
                 test_desc = "LDMSD 2-level agg with slurm",
                 commit_id = COMMIT_ID,
                 tada_addr = args.tada_addr)
test.add_assertion(1, "ldms_ls agg-2")
test.add_assertion(2, "slurm job_id verification in compute-1/slurm_sampler")
test.add_assertion(3, "slurm job_id verification in compute-2/slurm_sampler")
test.add_assertion(4, "component_id verification in compute-1/slurm_sampler")
test.add_assertion(5, "component_id verification in compute-2/slurm_sampler")


#### Start! ####
test.start()

print "-- Get or create the cluster --"
cluster = LDMSDCluster.get(spec["name"], create = True, spec = spec)

print "-- Start daemons --"
cluster.start_daemons()

print "... wait a bit to make sure ldmsd's are up"
time.sleep(5)

cont = cluster.get_container("headnode")

print "-- ldms_ls to agg-2 --"
rc, out = cont.ldms_ls("-x sock -p 20001 -h agg-2")
print out

expect = set([ "{}/{}".format(p, s) for p in ["compute-1", "compute-2"] \
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

rc, out = cluster.ldms_ls("-x sock -p 20001 -h agg-2 -l compute-1/slurm_sampler")
_set = set(map(int, re.search(r'job_id\s+(\S+)', out).group(1).split(',')))
verify(2, jobid in _set, "job_id verified")

rc, out = cluster.ldms_ls("-x sock -p 20001 -h agg-2 -l compute-2/slurm_sampler")
_set = set(map(int, re.search(r'job_id\s+(\S+)', out).group(1).split(',')))
verify(3, jobid in _set, "job_id verified")

rc, out = cluster.ldms_ls("-x sock -p 20001 -h agg-2 -l compute-1/slurm_sampler")
_set = set(map(int, re.search(r'component_id\s+(\S+)', out).group(1).split(',')))
verify(4, _set == set([10001]), "component_id == 10001")

rc, out = cluster.ldms_ls("-x sock -p 20001 -h agg-2 -l compute-2/slurm_sampler")
_set = set(map(int, re.search(r'component_id\s+(\S+)', out).group(1).split(',')))
verify(5, _set == set([10002]), "component_id == 10001")

test.finish()

cluster.remove() # this destroys entire cluster
