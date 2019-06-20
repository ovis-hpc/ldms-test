#!/usr/bin/env python
import os
import sys
import time
import socket
import docker

from StringIO import StringIO

from LDMS_Test import LDMSD, LDMSD_SVC, Network

_pystart = os.getenv('PYTHONSTARTUP')
if _pystart:
    execfile(_pystart)

DB=os.path.realpath("db")
if not os.path.exists(DB):
    os.makedirs(DB)

USER = os.getlogin()

NET = "{}_net".format(USER)

N_SAMP = 16
SAMPS = [ "{}_samp{:02d}".format(USER, i) for i in range(0, N_SAMP) ]
N_AGG1 = 2
AGG1S = [ "{}_agg1{}".format(USER, i) for i in range(0, N_AGG1) ]
N_AGG2 = 1
AGG2S = [ "{}_agg2{}".format(USER, i) for i in range(0, N_AGG2) ]


### Network Prep ###

net = Network(NET, driver='overlay', scope='swarm', attachable=True)

### Samp ###

samp_config = """
load name=meminfo
config name=meminfo producer=${HOSTNAME} instance=${HOSTNAME}/meminfo
start name=meminfo interval=2000000 offset=0
"""

samps = [ LDMSD_SVC(name, ovis_prefix="/home/narate/opt/ovis",
                    config=samp_config, networks = [ NET ]) \
               for name in SAMPS ]

for samp in samps:
    print "Starting {}".format(samp.name)
    samp.start_ldmsd()


### Lv1 Agg ###

prdcr_samp_conf = [
    """
    prdcr_add name={samp} host={samp} port={port} xprt={xprt} type=active \
              interval=2000000
    prdcr_start name={samp}
    """.format(
        samp = samp.name,
        port = samp.port,
        xprt = samp.xprt,
    ) for samp in samps
]

updtr_conf = """
updtr_add name=upd interval=2000000 offset=500000
updtr_prdcr_add name=upd regex=.*
updtr_start name=upd
"""

# split the load
agg1s = []
i = 0
for name in AGG1S:
    conf = "".join(prdcr_samp_conf[j*N_AGG1 + i] \
                    for j in range(0, N_SAMP//N_AGG1)) \
           + updtr_conf
    agg = LDMSD_SVC(name, ovis_prefix = "/home/narate/opt/ovis", config = conf,
                    networks = [ NET ])
    agg1s.append(agg)
    i += 1

for agg in agg1s:
    print "Starting {}".format(agg.name)
    agg.start_ldmsd()

### Lv2 Agg ###
prdcr_agg1_conf = [
    """
    prdcr_add name={agg} host={agg} port={port} xprt={xprt} type=active \
              interval=2000000
    prdcr_start name={agg}
    """.format(
        agg = agg.name,
        port = agg.port,
        xprt = agg.xprt,
    ) for agg in agg1s
]
agg2s = []
i = 0
for name in AGG2S:
    conf = "".join(prdcr_agg1_conf[j*N_AGG2 + i] \
                    for j in range(0, N_AGG1//N_AGG2)) \
           + updtr_conf
    agg = LDMSD_SVC(name, ovis_prefix = "/home/narate/opt/ovis", config = conf,
                    networks = [ NET ])
    agg2s.append(agg)
    i += 1

for agg in agg2s:
    print "Starting {}".format(agg.name)
    agg.start_ldmsd()

### svc for ldms_ls ###
svc = LDMSD_SVC("{}_svc".format(USER), ovis_prefix="/home/narate/opt/ovis",
                networks = [ NET ])

def svc_cmd_print(svc, cmd):
    rc, out = svc.exec_run(cmd)
    print "---- {} ----".format(cmd)
    print out
    print "------------"

svc_cmd_print(svc, "ldms_ls -x sock -p 10000 -h {}".format(AGG2S[0]))
svc_cmd_print(svc, "ldms_ls -x sock -p 10000 -h {}".format(AGG1S[0]))
svc_cmd_print(svc, "ldms_ls -x sock -p 10000 -h {}".format(AGG1S[1]))
svc_cmd_print(svc, "ldms_ls -x sock -p 10000 -h {}".format(SAMPS[3]))
svc_cmd_print(svc, "ldms_ls -x sock -p 10000 -h {} -l".format(SAMPS[3]))

# access ldmsd log content of samp00
s = samps[0].read_file(samps[0].log_file)
print s

def cleanup():
    for x in samps + agg1s + agg2s + [svc]:
        x.kill_svc()
    net.rm()

# call cleanup() to cleanup services and network
