#!/usr/bin/python

import os
import re
import pwd
import sys
import json
import time
import docker
import argparse
import TADA
import logging

from distutils.spawn import find_executable
from LDMS_Test import LDMSDCluster, LDMSDContainer, process_args, \
                      add_common_args, jprint, parse_ldms_ls

if __name__ != "__main__":
    raise RuntimeError("This should not be impoarted as a module.")

logging.basicConfig(format = "%(asctime)s %(name)s %(levelname)s %(message)s",
                    level = logging.INFO)

log = logging.getLogger(__name__)

execfile(os.getenv("PYTHONSTARTUP", "/dev/null"))

#### argument parsing #### -------------------------------------------
ap = argparse.ArgumentParser(description = "Spank DELME" )
add_common_args(ap)
ap.add_argument("--libdir",
                help="The directoryere the test target is installed.",
                default="__find_from_prefix__")
args = ap.parse_args()
process_args(args)
if args.libdir == "__find_from_prefix__":
    args.libdir = "/opt/ovis/lib64" if os.path.exists(args.prefix + "/lib64") \
                                    else "/opt/ovis/lib"

#### config variables #### ------------------------------
USER = args.user
PREFIX = args.prefix
COMMIT_ID = args.commit_id
SRC = args.src
CLUSTERNAME = args.clustername
DB = args.data_root
JOB_EXPIRY = 10

#### spec #### -------------------------------------------------------
common_plugin_config = [
        "component_id=%component_id%",
        "instance=%hostname%/%plugin%",
        "producer=%hostname%",
    ]
spec = {
    "name" : CLUSTERNAME,
    "description" : "{}'s spank coredump test".format(USER),
    "type" : "NA",
    "templates" : { # generic template can apply to any object by "!extends"
        "compute-node" : {
            "daemons" : [
                { "name" : "sshd", "type" : "sshd" },
                { "name" : "munged", "type" : "munged" },
                {
                    "name" : "slurmd",
                    "type" : "slurmd",
                    "plugstack" : [
                        {
                            "required" : True,
                            "path" : "%libdir%/ovis-ldms/libslurm_notifier.so",
                            "args" : [
                                "auth=none",
                                "port=10000",
                                "timeout=1",
                                "client=sock:localhost:10000:none",
                            ],
                        },
                    ],
                },
                {
                    "name" : "sampler-daemon",
                    "!extends" : "ldmsd-sampler",
                },
            ],
        },
        "ldmsd-sampler" : {
            "!extends" : "ldmsd-base",
            "samplers" : [
                {
                    "plugin" : "meminfo",
                    "!extends" : "sampler_plugin",
                },
            ],
        },
        "ldmsd-base" : {
            "type" : "ldmsd",
            "listen_port" : 10000,
            "listen_xprt" : "sock",
            "listen_auth" : "none",
        },
        "sampler_plugin" : {
            "interval" : 1000000,
            "offset" : 0,
            "config" : common_plugin_config,
            "start" : True,
        },
    },
    "nodes" : [
        {
            "hostname" : "node-1",
            "component_id" : 10001,
            "!extends" : "compute-node",
        },
        {
            "hostname" : "headnode",
            "component_id" : 20001,
            "daemons" : [
                { "name" : "sshd", "type" : "sshd" },
                { "name" : "munged", "type" : "munged" },
                { "name" : "slurmctld", "type" : "slurmctld" },
            ],
        },
    ],

    "libdir": args.libdir,
    "cpu_per_node" : 4,
    "oversubscribe" : "FORCE",
    "slurm_loglevel" : "debug2",
    "cap_add": [ "SYS_PTRACE", "SYS_ADMIN" ],
    "image": "ovis-centos-build",
    "ovis_prefix": PREFIX,
    "env" : { "FOO": "BAR" },
    "mounts": [
        "{}:/db:rw".format(DB),
    ] +
    ( ["{0}:{0}:ro".format(SRC)] if SRC else [] ),
}

#### Get or create the cluster ####
log.info("-- Get or create the cluster --")
cluster = LDMSDCluster.get(spec["name"], create = True, spec = spec)
log.info("-- Start daemons --")
cluster.make_known_hosts()
cluster.start_daemons()

def submit_job(name, num_tasks, duration = 10, subscriber_data = {}):
    global cluster
    cont = cluster.get_container("headnode")
    script = \
        "#!/bin/bash\n" \
        "#SBATCH -n {num_tasks}\n" \
        "#SBATCH -D /db\n" \
        "export SUBSCRIBER_DATA='{subscriber_data}'\n" \
        "srun bash -c 'for X in {{1..{duration}}}; do echo $SLURM_PROCID: $X; sleep 1; done'\n" \
        .format(
            num_tasks = num_tasks,
            duration = duration,
            subscriber_data = json.dumps(subscriber_data),
        )
    fname = "/db/{}.sh".format(name)
    cont.write_file(fname, script)
    jobid = cluster.sbatch(fname)
    return jobid

submit_job("job.sh", 4)
