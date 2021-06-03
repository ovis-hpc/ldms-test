#!/usr/bin/python3

import os
import shutil

from socket import gethostname
from os.path import dirname

# == For TADA recording == #
USER = os.environ.get("USER", os.geteuid())
CLUSTER_NAME = "cygnus"
TADA_ADDR = "localhost:9862"

# == Runtime configuration == #
SSH_PORT = 22 # SSH port to remotely execute commands on participating hosts
DEBUG = False # `True` to run in debug mode
WORK_DIR = os.path.dirname(os.path.abspath(__file__)) # default: the source dir
which_ldmsd = shutil.which("ldmsd")
default_prefix = "/opt/ovis" if not which_ldmsd else \
                 dirname(dirname(which_ldmsd))
OVIS_PREFIX = os.getenv("OVIS_PREFIX", default_prefix)

# MYHOST shall match hostname variants specified in SAMP_HOSTS, L1_HOSTS,
# L2_HOSTS, and L3_HOSTS lists. Otherwise, the child scripts executed on each
# host won't be able to identify its host.
#
# For example, some cluster may have hostname like "node-1" associated with
# Ethernet interface while "node-1-ib" associated with Infiniband interface. In
# the case that we're testing "rdma" transport that will use the Infiniband
# hostname variant (e.g. "node-1-ib"), `MYHOST` shall be:
#   MYHOST = gethostname().split('.', 1)[0] + "-ib"
MYHOST = gethostname().split('.', 1)[0] + "-iw"

# LDMS transport type
XPRT = os.getenv("XPRT", "rdma")

# Logging level for ldmsd.
LOG_LEVEL = os.getenv("LOG_LEVEL", "ERROR")

# List of hosts to run sampler daemons
SAMP_HOSTS = [ "cygnus-0{}-iw".format(i) for i in range(1, 5) ]
SAMP_PER_HOST = 64 # number of sampler daemons per host in SAMP_HOSTS
SETS_PER_SAMP = 4 # number of sets per sampler daemon

# List of hosts to run L1 aggregators
L1_HOSTS = [ "cygnus-05-iw", "cygnus-06-iw" ]
L1_PER_HOST = 8 # number of L1 aggregators per host in L1_HOSTS

# List of hosts to run L2 aggregators
L2_HOSTS = [ "cygnus-07-iw" ]
L2_PER_HOST = 4 # number of L2 aggregators per host in L2_HOSTS

# The host (not a list of hosts) that run L3 aggregator
L3_HOST = "cygnus-08-iw"

# The listening port of a daemon will be `base + daemon_index` and
# `HOST-PORT` becomes the name of the daemon. If a host run multiple levels of
# aggregators, be mindful to set their BASE_PORT so that their port ranges will
# not overlap.
SAMP_BASE_PORT = 20000
L1_BASE_PORT   = 21000
L2_BASE_PORT   = 22000
L3_BASE_PORT   = 23000

CONN_INTERVAL = 1000000 # prdcr connection interval (microseconds)
SAMP_INTERVAL = 1000000 # sampling/update interval (microseconds)
MON_INTERVAL  = 1000000 # stat monitoring interval (microseconds)

STEADY_WAIT = 10 # the waiting time in seconds to form a steady state after all
                 # daemons are up

# SOS on L3
os.environ["SOS_DEFAULT_BACKEND"] = "mmap"
L3_STORE_ROOT = "sos" # set to `None` to disable store_sos
