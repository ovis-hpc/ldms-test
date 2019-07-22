#!/usr/bin/env python

import os
import re
import sys
import json
import time
import docker
import argparse
import TADA

from distutils.spawn import find_executable
from LDMS_Test import LDMSDCluster, LDMSDContainer

#if __name__ != "__main__":
#    raise RuntimeError("This should not be imported as a module.")

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
    return None

def update_expect_file(fname, data):
    s = json.dumps(data)
    f = open(fname, 'w')
    f.write(s)
    f.close()

#### default values #### ---------------------------------------------
sbin_ldmsd = find_executable("ldmsd")
if sbin_ldmsd:
    default_prefix, a, b = sbin_ldmsd.rsplit('/', 2)
else:
    default_prefix = "/opt/nick/ovis"

#### argument parsing #### -------------------------------------------
ap = argparse.ArgumentParser(description =
                         "Run test scenario of slurm stream using ldmsd_stream_publish " \
                         "with slurm data and job slot verification." )
ap.add_argument("--clustername", type = str,
                help = "The name of the cluster. The default is "
                "USER-slurm-test-COMMIT_ID.")
ap.add_argument("--prefix", type = str,
                default = default_prefix,
                help = "The OVIS installation prefix.")
ap.add_argument("--src", type = str,
                help = "The path to OVIS source tree (for gdb). " \
                       "If not specified, src tree won't be mounted.")
ap.add_argument("--db", type = str,
                default = "{}/db".format(os.path.realpath(sys.path[0])),
                help = "The path to host db directory, location of Slurm_Test-static.txt file." )
ap.add_argument("--slurm-notifier", type = str,
                default = "__find_from_prefix__",
                help = "The path (in container) to slurm_notifier library." )
ap.add_argument("--tada_addr", help="Test automation server host and port " \
		" as host:port")
args = ap.parse_args()

#### config variables #### ------------------------------
USER = os.getlogin()
PREFIX = args.prefix
COMMIT_ID = get_ovis_commit_id(PREFIX)
SRC = args.src
CLUSTERNAME = args.clustername if args.clustername else \
              "{}-slurm-stream-test-{:.7}".format(USER, COMMIT_ID)
DB = args.db
'''
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
'''

#### spec #### -------------------------------------------------------
spec = {
    "name" : CLUSTERNAME,
    "description" : "{}'s slurm stream test cluster".format(USER),
    "type" : "NA",
    "templates" : {
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
                }
            ],
        },
        "ldmsd-base" : {
            "type" : "ldmsd",
            "listen_port" : 10000,
            "listen_xprt" : "sock",
            "listen_auth" : "munge",
        },
        "ldmsd-sampler" : {
            "!extends" : "ldmsd-base",
            "samplers" : [
                {
                    "plugin" : "slurm_sampler",
                    "interval" : 1000000,
                    "offset" : 0,
                    "config" : [
                        "component_id=%component_id%",
                        "stream=test-slurm-stream",
                        "instance=%hostname%/%plugin%",
                        "producer=%hostname%"
                    ],
                    "start" : True,
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
                    "listen_port" : 20000,
                    "prdcrs" : [
                        {
                            "name" : "compute-1",
                            "!extends" : "prdcr",
                        },
                        {
                            "name" : "compute-2",
                            "!extends" : "prdcr",
                        },
                    ],
                    "config" : [
                        "prdcr_start_regex regex=.*",
                        "updtr_add name=all interval=1000000 offset=0",
                        "updtr_prdcr_add name=all regex=.*",
                        "updtr_start name=all"
                    ],
                },
            ]
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

    "cap_add": [ "SYS_PTRACE" ],
    "image": "ovis-centos-build",
    "ovis_prefix": PREFIX,
    "env" : { "FOO": "BAR" },
    "mounts": [
        "{}:/db:rw".format(DB),
    ] +
    ( ["{0}:{0}:ro".format(SRC)] if SRC else [] ),
}

### slurm-stream data ###
init_data = {
    "job_id" : 12345,
    "job_name" : "test-job",
    "subscriber_data" : "test",
    "uid" : 0,
    "gid" : 0,
    "nnodes" : 1,
    "nodeid" : 1,
    "local_tasks" : 8,
    "total_tasks" : 8
}
event_template = {
    "schema" : "meminfo",
    "timestamp" : 1561661493,
    "context" : "remote"
}
slurm_init = event_template.copy()
slurm_init['event'] = "init"
slurm_init['data'] = init_data

slurm_task_init = event_template.copy()
slurm_task_init['event'] = "task_init_priv"
slurm_task_init['data'] = {
    "job_id" : 12345,
    "task_id" : 0,
    "task_global_id" : 0,
    "task_pid" : 9000,
    "nodeid" : 1
}
slurm_task_exit = event_template.copy()
slurm_task_exit['event'] = "task_exit"
slurm_task_exit['data'] = {
    "job_id" : 12345,
    "task_id" : 0,
    "task_global_id" : 0,
    "task_pid" : 8000,
    "nodeid" : 1,
    "task_exit_status" : 0
}

slurm_exit = event_template.copy()
slurm_exit['event'] = "exit"
slurm_exit['data'] = {
    "job_id" : 12345,
    "nodeid" : 1
}

#### test definition ####

test = TADA.Test(test_suite = "LDMSD",
                 test_type = "LDMSD",
                 test_name = "slurm-stream-test",
                 tada_addr = args.tada_addr,
                 commit_id = COMMIT_ID)
test.add_assertion(1, "ldms_ls Jobs are assigned to next slot")
test.add_assertion(2, "Newest job replaces oldest job when slots full")
test.add_assertion(3, "Task process id data in each metric set is correct")
test.add_assertion(4, "Max number of jobs, with all jobs correctly represented")
test.add_assertion(5, "Job data in each metric set reflects matching text file")


#### Start! ####
test.start()

print "-- Get or create the cluster --"
cluster = LDMSDCluster.get(spec["name"], create = True, spec = spec)

print "-- Start daemons --"
cluster.start_daemons()

print "... wait a bit to make sure ldmsd's are up"
time.sleep(5)

cont = cluster.get_container('headnode')

def verify(num, cond, cond_str):
    a = test.assertions[num]
    print a["assert-desc"] + ": " + ("Passed" if cond else "Failed")
    test.assert_test(num, cond, cond_str)

json_path = args.db

# Add job events over stream
i = 0
job_count = 8
while i < job_count:
    slurm_init['timestamp'] += 1
    update_expect_file(json_path+"/Slurm_Test-data.json", slurm_init)
    data_file = "/db/Slurm_Test-data.json"

    for host in [ 'compute-1', 'compute-2' ]:
        cont = cluster.get_container(host)
        rc, out = cont.exec_run("ldmsd_stream_publish -h {host} -x sock -p 10000"
                                " -a munge -s test-slurm-stream -t json -f {fname}"
                                .format(fname=data_file,
                                        host=host))
    slurm_task_init['timestamp'] += 1
    update_expect_file(json_path+"/Slurm_Test-data.json", slurm_task_init)
    for host in [ 'compute-1', 'compute-2' ]:
        cont = cluster.get_container(host)
        rc, out = cont.exec_run("ldmsd_stream_publish -h {host} -x sock -p 10000"
                                " -a munge -s test-slurm-stream -t json -f {fname}"
                                .format(fname=data_file,
                                        host=host))
        if out:
           print(out)
    slurm_task_exit['timestamp'] += 1
    update_expect_file(json_path+"/Slurm_Test-data.json", slurm_task_exit)
    for host in [ 'compute-1', 'compute-2' ]:
        cont = cluster.get_container(host)
        rc, out = cont.exec_run("ldmsd_stream_publish -h {host} -x sock -p 10000"
                                " -a munge -s test-slurm-stream -t json -f {fname}"
                                .format(fname=data_file,
                                        host=host))
    slurm_exit['timestamp'] += 1
    update_expect_file(json_path+"/Slurm_Test-data.json", slurm_exit)
    for host in [ 'compute-1', 'compute-2' ]:
        cont = cluster.get_container(host)
        rc, out = cont.exec_run("ldmsd_stream_publish -h {host} -x sock -p 10000"
                                " -a munge -s test-slurm-stream -t json -f {fname}"
                                .format(fname=data_file,
                                        host=host))
        if out:
           print(out)
        rc, out = cont.ldms_ls("-h {host} -p 10000 -x sock -a munge -l".format(host=host))
        cnt = 0
        for line in (out.split('\n')):
            if cnt == 2:
                ids = line.split()[3].split(',')
                print(ids)
                jid = 12345
                verify(1, int(ids[i]) == jid+i, 'correct job_id fills next job_slot')
            elif cnt == 14+i:
                tids = line.split()[3].split(',')
                verify(3, int(tids[i]) == 9000+i, ' correct task process id in metric set')
            cnt += 1
    slurm_init['data']['job_id'] += 1
    slurm_init['data']['nodeid'] += 1
    slurm_task_init['data']['job_id'] += 1
    slurm_task_init['data']['task_pid'] += 1
    slurm_task_init['data']['task_id'] += 1
    slurm_task_init['data']['task_global_id'] += 1
    slurm_task_init['data']['nodeid'] += 1
    slurm_task_exit['data']['job_id'] += 1
    slurm_task_exit['data']['task_pid'] += 1
    slurm_task_exit['data']['task_id'] += 1
    slurm_task_exit['data']['task_global_id'] += 1
    slurm_task_exit['data']['nodeid'] += 1
    slurm_exit['data']['job_id'] += 1
    slurm_exit['data']['nodeid'] += 1
    i += 1

# Check to ensure jobs are correctly represented with max number of jobs running
for hosts in ['compute-1', 'compute-2']:
    cont = cluster.get_container(host)
    rc, out = cont.ldms_ls("-h {host} -p 10000 -x sock -a munge -l".format(host=host))
    cnt = 0
    k = 0
    for line in (out.split('\n')):
        if cnt == 2:
            i = 0
            ids = line.split()[3].split(',')
            jid = 12345
            while i < 8:
                verify(4, int(ids[i]) == jid+i, 'job_ids correctly represented with max jobs')
                i += 1
        elif cnt == 10:
            i = 0
            tstamp = line.split()[3].split(',')
            while i < 8:
                verify(4, int(tstamp[i]) == 1561661494+i, 'job_start correctly represented with max jobs')
                i += 1
        elif cnt == 11:
            i = 0
            tstamp = line.split()[3].split(',')
            while i < 8:
                verify(4, int(tstamp[i]) == 1561661494+i, 'job_end correctly represented with max jobs')
                i += 1
        if cnt == 14+k:
            if k == 8:
                break
            tids = line.split()[3].split(',')
            verify(4, int(tids[k]) == 9000+k, ' correct task process id in metric set with max jobs')
            k += 1
        cnt += 1

# Add extra job, and ensure it takes oldest job slot
slurm_init['data']['job_id'] = 12353
slurm_init['timestamp'] = 1561661512
slurm_init['data']['nodeid'] += 1

slurm_task_init['data']['job_id'] = 12353
slurm_task_init['timestamp'] = 1561661512
slurm_task_init['data']['task_pid'] += 1
slurm_task_init['data']['task_id'] += 1
slurm_task_init['data']['task_global_id'] += 1
slurm_task_init['data']['nodeid'] += 1

slurm_task_exit['data']['job_id'] = 12353
slurm_task_exit['timestamp'] = 1561661514
slurm_task_exit['data']['task_pid'] += 1
slurm_task_exit['data']['task_id'] += 1
slurm_task_exit['data']['task_global_id'] += 1
slurm_task_exit['data']['nodeid'] += 1

slurm_exit['data']['job_id'] = 12353
slurm_exit['timestamp'] = 1561661515
slurm_exit['data']['nodeid'] += 1

last_job = [ slurm_init, slurm_task_init, slurm_task_exit, slurm_exit ]

for event in last_job:
    update_expect_file(json_path+"/Slurm_Test-data.json", event)
    for host in [ 'compute-1', 'compute-2' ]:
        cont = cluster.get_container(host)
        rc, out = cont.exec_run("ldmsd_stream_publish -h {host} -x sock -p 10000"
                                " -a munge -s test-slurm-stream -t json -f {fname}"
                                .format(fname=data_file,
                                        host=host))
for host in [ 'compute-1', 'compute-2' ]:
    cont = cluster.get_container(host)
    rc, out = cont.ldms_ls("-h {host} -p 10000 -x sock -a munge -l".format(host=host))
    cnt = 0
    for line in (out.split('\n')):
        if cnt == 2:
            ids = line.split()[3].split(',')
            print(ids)
            verify(2, int(ids[0]) == 12353, 'slots full, oldest job slot selected')
            break
        cnt += 1

test_results = open(json_path+"/Slurm_Test-results.txt", 'w')
for host in [ 'compute-1', 'compute-2' ]:
    cont = cluster.get_container(host)
    rc, out = cont.ldms_ls("-h {host} -p 10000 -x sock -a munge -l".format(host=host))
    cnt = 0
    for line in (out.split('\n')):
        if cnt == 0:
            pass
        else:
            test_results.write(line+'\n')
        cnt += 1

test_results.close()

results = open(json_path+'/Slurm_Test-results.txt').read()
expected_data = open(json_path+'/Slurm_Test-static.txt').read()
verify(5, (results == expected_data), 'ldms_ls == text_file')
print('Test Finished')

test.finish()

cluster.remove()
