TOC
===

* [Overview](#Overview)
* [Docker Host Cluster Setup](#docker-host-cluster-setup)
* [Docker Setup](#docker-setup)
  * [dockerd network port](#dockerd-network-port)
  * [Setting Up Docker Swarm](#setting-up-docker-swarm)
  * [Preparing Docker Image](#preparing-docker-image)
  * [Distribute Docker Image](#distribute-docker-image)
* [Python Module Usage Guide](#python-module-usage-guide)
  * [Create (or Get) Virtual Cluster](#create-or-get-virtual-cluster)
  * [Start Services](#start-services)
  * [Cluster and Container Utilities](#cluster-and-container-utilities)
  * [LDMS Utilities](#ldms-utilities)
  * [Slurm Job Utilities](#slurm-job-utilities)
  * [TADA Utilities](#tada-utilities)
* [Debugging](#debugging)
* [Example Results](#example-results)

Overview
========

`LDMS_Test` is a Python module containing tools to help building a virtual
cluster for testing ldmsd. It relies on Docker Swarm (a bunch of dockerd running
in swarm mode) and creates a virtual cluster as Docker Service which has the
number of replicas equals to the number of nodes in the virtual cluster. The
Docker Service contains Tasks, the number of which equals to that of replicas,
and each Task has a Docker Container (on a swarm member dockerd) associated with
it. These Containers are the nodes in the virtual cluster. Please note that the
containers of a service speread out across dockerds in the swarm. To isolate
the virtual cluster network, the Docker Overlay Network is uniquely created for
the Docker Service when the `create()` routine is executed. Since the Docker
Service utilizes, but does not own, the Docker Network, we assign the same name
to both entities for obvious association. Note that `$ docker service remove
<SERVICE_NAME>` only removes the service, not the network it uses, but the
`remove()` routine in Python conveniently removes the network it uses.

`DockerClusterService` implements basic routines for handling the virtual
cluster (e.g. create, remove), and the `DockerClusterContainer` implements basic
routines for interacting with each of the containers (e.g. getting aliases of
the virtual node). The `LDMSDCluster` extends `DockerClusterService` and
`LDMSDContainer` extends `DockerClusterContainer` to provide ldmsd-specific
routines (e.g. start ldmsd, perform `ldms_ls`). Please see [Python Module Usage
Guide](#python-module-usage-guide) for a guideline on how to use the virtual
cluster in Python. For full details of the APIs, please see in-line Python
documentation in the module.

In this document, we will use our bare-metal cluster "cygnus" (hostnames:
cygnus-01..08) as an example on how to deploy and run the test infrastructure.
Cygnus cluster runs on CentOS 7.6.1810.


Docker Host Cluster Setup
=========================

Bare-metal cluster is not really needed to deploy the test infrastructure
described in this document. It can be deployed on just 2 virtual box machines
(or even a single virtual box machine!) hosting dockerds, but obviosly the
scalabilty will be limited.

The `/home/<user>` should be NFS-mounted so that users' files will be accessible
across nodes in the bare-metal cluster.


Docker Setup
============

This section will guide you through docker setup needed for the test
infrastructure. `cygnus-{01..08}` all have `docker-ce` version 18.09 installed.
`dockerd` system service is enabled so that `dockerd` service is started on
boot.

```bash
$ systemctl enable dockerd.service
```

The users that need to run the virtual cluster testing facility must also be a
member of `docker` user group, otherwise they cannot create/delete Docker
Network or Docker Service.


dockerd network port
--------------------

By default,
dockerd is configured to listen only on to the unix-domain socket with dockerd
CLI options in `/usr/lib/systemd/system/dockerd.service` file. We need to
configure dockerd to also listen on port 2375 (its default network port) because
a docker client (our Python test script) needs a connection to the dockerd to
manipulate containers running under it (containers in our virtual cluster).
Even though dockerd support `/etc/docker/daemon.json` dockerd configuration, the
CLI option takes precedence. As such, we have to override the `dockerd.service`
to get rid of the CLI option by creating an override file for dockerd.service as
follows:

```bash
# file: /etc/systemd/system/docker.service.d/override.conf
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd --containerd=/run/containerd/containerd.sock
# original:
# ExecStart=/usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock
```

The `ExecStart=` empty value setting is needed, otherwise systemd would just
appending the new value to the existing value. The following is the content of
`/etc/docker/daemon.json`:

```json
# file: /etc/docker/daemon.json
{
"hosts": [
        "unix:///var/run/docker.sock",
        "tcp://0.0.0.0:2375"
    ]
}
```

Now, stop the service, reload so that the override takes effect, and start the
dockerd (on all noes).

```bash
$ systemctl stop dockerd.service
$ systemctl daemon-reload
$ systemctl start dockerd.service
```

NOTE: CentOS has a nice package called `pssh` that helps executing the given
command on given hosts to help cluster admin, for example:

```bash
$ HOSTS=$(echo cygnus-{01..08})
$ pssh -H "$HOSTS" -i hostname
[1] 15:11:48 [SUCCESS] cygnus-03
cygnus-03.ogc.int
[2] 15:11:48 [SUCCESS] cygnus-02
cygnus-02.ogc.int
[3] 15:11:48 [SUCCESS] cygnus-01
cygnus-01.ogc.int
[4] 15:11:48 [SUCCESS] cygnus-04
cygnus-04.ogc.int
[5] 15:11:48 [SUCCESS] cygnus-08
cygnus-08.ogc.int
[6] 15:11:48 [SUCCESS] cygnus-05
cygnus-05.ogc.int
[7] 15:11:48 [SUCCESS] cygnus-07
cygnus-07.ogc.int
[8] 15:11:48 [SUCCESS] cygnus-06
cygnus-06.ogc.int

# Or, use alias for convenient
$ alias pssh.all="pssh -H \"$(echo cygnus-{01..08})\""
$ pssh.all -i hostname
[1] 15:11:48 [SUCCESS] cygnus-03
cygnus-03.ogc.int
[2] 15:11:48 [SUCCESS] cygnus-02
cygnus-02.ogc.int
[3] 15:11:48 [SUCCESS] cygnus-01
cygnus-01.ogc.int
[4] 15:11:48 [SUCCESS] cygnus-04
cygnus-04.ogc.int
[5] 15:11:48 [SUCCESS] cygnus-08
cygnus-08.ogc.int
[6] 15:11:48 [SUCCESS] cygnus-05
cygnus-05.ogc.int
[7] 15:11:48 [SUCCESS] cygnus-07
cygnus-07.ogc.int
[8] 15:11:48 [SUCCESS] cygnus-06
cygnus-06.ogc.int
```

Issuing `pssh.all -i systemctl stop dockerd.service` would stop dockerd on all
nodes in the cluster.


Setting Up Docker Swarm
-----------------------

We need docker daemons to run in swarm mode so that the tasks (containers) load
in the service can be spread throughout the bare-metal cluster. To setup a
swarm, first pick a node (cygnys-08 in this case), and issue `docker swarm
init`. This will initialize the dockerd in the node as swarm manager and
generate a token for other nodes to join the swarm.

```bash
# On cygnus-08
$ docker swarm init

$ pssh -H "$(echo cygnus-{01..07})" -i docker swarm join \
       --token <TOKEN_FROM_SWARM_INIT> cygnus-08:2377


# Just in case you swarm-init, but forgot to save the token,
# use the following comands to obtain it on cygnus-08.

# Obtain worker token
$ docker swarm join-token worker

# Or `manager` token to join the swarm as manager
$ docker swarm join-token manager

# We can also promote workers to managers as the following
$ docker node promote cygnus-01 cygnus-05

# Docker document mentioned that the number of managers should be an odd number
# and is less than 7. A manager by default is also a worker (i.e. it also run
# tasks for the service).
```


Preparing Docker Image
----------------------

Dockerfile is provided for building the image described in this section. The
following is the command to build the image using the provided Dockerfile.

```bash
$ docker build -t ovis-centos-build -f docker/Dockerfile
```

The docker image for running ldmsd virtual cluster is based on `centos:7`
official image. Additional pakcages are installed to make the image suitable for
running ldmsd, debugging ldmsd and being a virtual cluster. The following is a
list of extra packages being installed:

- `epel-release` for `munge` package
- `munge` needed by ldms munge auth
- `openssh-server` to conveniently SSH from one container to another
- `gdb` for debugging

The following list of installation will also make the image suitable for
building ldms:

- `"Development Tools"` package group
- `openssl-devel` needed by ovis auth build
- `pip`
- `Cython` via `pip`
- `numpy` via `pip`

SSH host keys are generated (`/etc/ssh/ssh_host_<TYPE>_key`). And user SSH key
is generated and add to `authorized_keys` to conveniently SSH among nodes
without password.


Distribute Docker Image
-----------------------

```bash
# On cygnus-08, save the image first
$ docker image save ovis-centos-build > img

# using pssh to conveniently load the image on other nodes
$ pssh -H "$(echo cygnus-{01..07})" -i -t 0 docker image load -i $PWD/img
```


Python Module Usage Guide
=========================

LDMSDCluster and Test are the classes used for creating virtual cluster and to
report the test results respectively.

The general workflow is 1) create (or get existing) virtual cluster, 2) start
required services, 3) perform test, 4) report test. For debugging, please see
Debugging section.


Create (or Get) Virtual Cluster
-------------------------------

The following snippet is an example to get the existing virtual cluster (having
the Python handle object to control it). The optional `create = True` argument
is to create the virtual cluster if it does not exist.

```python
cluster = LDMSDCluster.get(spec["name"], create = True, spec = spec)
```

Alternatively, create the virtual cluster with:

```python
cluster = LDMSDCluster.create(spec = spec)
```

The difference is that `create()` will fail if the virtual cluster existed while
`get()` won't.

Docker containers could not change their hostname after created witout the
"privilege" granted. As such, the hostnames of containers in the virtual cluster
are named as "node-{NUMBER}" where NUMBER ranges from 1 to N. The test
application can still define hostnames to the containers in the `spec`, and
those hostnames will appear as hostname aliases in the `/etc/hosts` file in the
containers.

In addition, for `LDMSDCluster`, the number of containers is the number of
daemons + 1. Each daemon defined in `spec` will run on its own container. The
extra container (last container, e.g. `node-5` for 4-daemon spec) is for
`slurmctld`, job submitting, and `ldms_ls`. One may think of it as a service
node (vs compute nodes) for a cluster.

The `spec` is a dictionary containing LDMSD Cluster Spec defined as follows:

```python
{
    "name" : "NAME_OF_THE_VIRTUAL_CLUSTER",
    "description" : "DESCRIPTION OF THE TEST",
    "type" : "TYPE OF THE TEST",
    "define" : [ # a list of daemon templates
        {
            "name" : "TEMPLATE_NAME",
            "type" : "TEMPLATE_TYPE", # only "sampler" for now
            "listen_port" : LDMSD_LISTENING_PORT, # int
            "listen_xprt" : "LDMS_TRANSPORT_TYPE", # sock, rdma, or ugni
            "listen_auth" : "LDMS_AUTH_TYPE", # none, ovis, or munge
            "env" : [ # a list of environment variables and their values
                "INTERVAL=1000000",
                "OFFSET=0",
            ],
            "samplers" : [ # a list of sampler plugins to load/config
                {
                    "plugin" : "PLUGIN_NAME",
                    "config" : [ # a list of plugin configuration parameters
                        # '%plugin%' is replaced with PLUGIN_NAME
                        "instance=${HOSTNAME}/%plugin%",
                        "producer=${HOSTNAME}",
                    ]
                },
            ]
        }
    ],
    "daemons" : [ # a list of LDMS Daemons (1 per container)
        {
            # defining daemon using a template, all attributes of the template
            # are applied to the daemon, except for `env` being a concatenation
            # of template["env"] + daemon["env"] (hence daemon env overrides
            # that of the template for the env variables appearing in both
            # places).
            "host" : "HOST_NAME",
            "asset" : "TEMPLATE_NAME", # referring to the template in `define`
            "env" : [ # additional env
                "COMPONENT_ID=10001",
                "HOSTNAME=%host%", # %host% is replaced with HOST_NAME value
                                   # from above.
            ]
        },
        # or
        {
            # defining a daemon plainly (not using template).
            "host" : "HOST_NAME",
            "listen_port" : LDMSD_LISTENING_PORT, # int
            "listen_xprt" : "LDMS_TRANSPORT_TYPE", # sock, rdma, or ugni
            "listen_auth" : "LDMS_AUTH_TYPE", # none, ovis, or munge
            "env" : [ # list of env
                "HOSTNAME=%host%"
            ],
            "config" : [ # list of ldmsd configuration commands
                "load name=meminfo",
                ...
            ]
        },
    ],
    "image": "DOCKER_IMAGE_NAME", # Optional image name to run each container.
                                  # default: "ovis-centos-build"
    "ovis_prefix": "PATH-TO-OVIS-IN-HOST-MACHINE", # path to OVIS in the host
                                                   # machine. This will be
                                                   # mounted to `/opt/ovis`
                                                   # in the container.
                                                   # default: "/opt/ovis"
    "env" : { "FOO": "BAR" }, # additional environment variables applied
                              # cluster-wide. The environment variables in
                              # this list (or dict) has the least precedence.
    "mounts": [ # additional mount points for each container, each entry is a
                # `str` and must have the following format
        "SRC_ON_HOST:DEST_IN_CONTAINER:MODE", # MODE can be `ro` or `rw`
        ...
    ]
}
```

All environment definitions (`spec["env"]`,`spec["define"][i]["env"]`, and
`spec["daemons"][j]["env"]`) have two form: list `["NAME=VALUE"]` or dictionary
`{"NAME": "VALUE"}`. When the same variable is defined in all three places,
`spec["env"]` (least precedence) < `spec["define"][i]["env"]` <
`spec["daemons"][j]["env"]` (most precedence).

The following is an example defining a virtual cluster of 4 nodes: 2 samplers
and 2 aggregators (2 levels of aggregation).

```python
{
    "name" : "ldms_slurm",
    "description" : "LDMSD 2 level aggregation test with slurm",
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
                "prdcr_add name=sampler-1 host=sampler-1 port=10000" \
                         " xprt=sock type=active interval=20000000",
                "prdcr_add name=sampler-2 host=sampler-2 port=10000" \
                         " xprt=sock type=active interval=20000000",
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
                "prdcr_add name=agg-1 host=agg-1 port=20000 xprt=sock" \
                         " type=active interval=20000000",
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
```


Start Services
--------------

The containers in the virtual cluster has only main process (`/bin/bash`)
running initially. We need to manually start the services. All `start_*()`
methods of supported services do nothing if the service has already started.
The following is the list of supported services and the corresponding
`LDMSDCluster` method to starts it:

- `cluster.start_sshd()` to start `sshd` in each container. This is convenient
  for debugging.
- `cluster.start_munged()` to start `munged` in each container. This is required
  by `munge` ldms authentication and `slurm`.
- `cluster.start_slurm()` to start `slurmd` in the sampler nodes and `slurmctld`
  in the service node (last container).
- `cluster.start_ldmsd()` to start `ldmsd` in each container, except for the
  service container (the last one).


Cluster and Container Utilities
-------------------------------

The following is the list of utilities to interact with the virtual cluster and
the containers inside it.

- Getting a container inside the cluster: `cont = cluster.get_container(NAME)`.
  The NAME can be container hostname (e.g. "node-1"), or its alias (e.g.
  "sampler-1").
- Getting a list of all containers: `conts = cluster.containers`.
- Execute a program in a container: `cont.exec_run(CMD)`, e.g.
  `rc, out = cont.exec_run("df -h")`.
- `cluster.exec_run(*args)` calls `cont.exec_run(*args)` where `cont` is the
  service node (last container).
- Write to a file in a continer:  `cont.write_file(PATH, CONTENT)`.
- Read content of a file in a container: `cont.read_file(PATH)`.
- Get a hostname of a container: `cont.hostname`.
- Get the first IP address of a container: `cont.ip_addr`
- Get network interfaces and their IP addresses of a container:
  `cont.interfaces`.
- `cont.pgrep(OPTIONS)` executes `pgrep` in the container, e.g.
  `rc, out = cont.pgrep("-c ldmsd")` (if `ldmsd` is running, `rc` is 0).


LDMS Utilities
--------------

The following is the list of utilities for executing LDMS-related programs:
- Check if ldmsd is running in a container: `True == cont.check_ldmsd()`.
- Start `ldmsd` in a container: `cont.start_ldmsd()`.
- Kill `ldmsd` in a container: `cont.kill_ldmsd()`. This is a short-hand for
  `ldmsd.exec_run("pkill ldmsd")`.
- Execute `ldms_ls` in a container: `cont.ldms_ls(OPTIONS)`, e.g.
  `cont.ldms_ls("-x sock -p 10000 -h sampler-1")`. This is a short-hand for
  `cont.exec_run("ldms_ls " + OPTIONS)`.
- `cluster.ldms_ls(*args)` calls `cont.ldms_ls(*args)` where `cont` is the
  service node (last container).


Slurm Job Utilities
-------------------

The following is the list of utilities for executing Slurm-related programs:
- Submitting a job: `cluster.sbatch(SCRIPT_PATH)`, where `SCRIPT_PATH` is the
  path to the script in the service node (the last container).
- Get status of all jobs: `cluster.squeue()`, or get status of a single job:
  `cluster.squeue(JOB_ID)`.
- Cancelling a job: `cluster.scancel(JOB_ID)`.


TADA Utilities
--------------

To report test results to TADA Daemon (`tadad`), first we need to create a
TADATest object and define assertions as follows:

```python
test = TADATest(test_suite = "LDMSD",
            test_type = "LDMSD",
            test_name = "agg + slurm_sampler + slurm_notifier",
            tada_host = "cygnus-08",
            tada_port = 9862)
test.add_assertion(1, "ldms_ls agg-2")
test.add_assertion(2, "slurm job_id verification on sampler-1")
test.add_assertion(3, "slurm job_id verification on sampler-2")
```

Then, notify the `tadad` that we are starting the test:
```python
test.start()
```

After that, for each assertion point, call `test.assert_test()` to report the
result of the assertion. The assertion that was not tested with `assert_test()`
will be reported as `SKIPPED`. For example:
```python
# result is `bool`
result = verify_ldms_ls()
test.assert_test(1, result, "ldms_ls results")
# deliberately skip 2
result = verify_jobid()
test.assert_test(3, result, "job_id results")
```


Debugging
=========

Test cases failed oftentimes while in development. This section provides you
some tips on how to debug with LDMSD Virtual Cluster.

Use `python -i` for interactive python session. Having access to python objects
representing the virtual cluster could help in debugging.


Cleanup
-------

Do proper cleanup (e.g. `cluster.remove()`), but you might want to hold it while
you're debugging. Sometimes, an Exception might prevent the code to properly
cleanup, e.g. docker network was created, but failed to cleanup after service
creation failure. The following is the docker commands to remove a service and a
network.

```bash
$ docker service rm mycluster
$ docker network rm mycluster # network is created using service name

# We might want to periodically check whether the network has already
# removed, since docker could take 5-10 seconds to remove it.
$ docker network ls
```


Container Interactive Shell
---------------------------

Sometimes, we want to have an interactive shell to the cluster to look around.

To do so, first we need to find out where the last container is:
```bash
$ docker service ps --no-trunc mycluster
ID                          NAME                IMAGE               NODE                DESIRED STATE       CURRENT STATE            ERROR               PORTS
tj8ao52d9vpm7of1s262dlb1m   mycluster.1         ovis-centos-build   cygnus-02.ogc.int   Running             Running 13 minutes ago
wns19z8w28icoyctgrzzl5507   mycluster.2         ovis-centos-build   cygnus-08.ogc.int   Running             Running 12 minutes ago
di5mr9m29mhkexwvk5h59zgm4   mycluster.3         ovis-centos-build   cygnus-01.ogc.int   Running             Running 12 minutes ago
rubpkah0ovi5qkz1voakgmqce   mycluster.4         ovis-centos-build   cygnus-05.ogc.int   Running             Running 9 minutes ago
e10nz8d82zzmoyek44becav91   mycluster.5         ovis-centos-build   cygnus-07.ogc.int   Running             Running 12 minutes ago
```

Note thet the `--no-trunc` option is to not truncate the ID in the output.
Now we see that the last task slot `mycluster.5` is running on `cygnus-07`,
we need to work with the dockerd on that host to get to the container. Try
listing all containers on `cygnus-07` by:

```bash
$ docker -H cygnus-07 ps -a
CONTAINER ID        IMAGE                      COMMAND             CREATED             STATUS              PORTS               NAMES
5bfd7c6781f6        ovis-centos-build:latest   "bash"              16 minutes ago      Up 16 minutes                           mycluster.5.e10nz8d82zzmoyek44becav91
```

Notice that the name of the container is the `TaskName.FullTaskID`. We can then
use the container name to execute an interactive bash session in it:

```
$ docker -H cygnus-07 exec -it mycluster.5.e10nz8d82zzmoyek44becav91 bash
[root@node-5 /]#
```

From here, we can do all sorts of things, e.g.

```
[root@node-5 /]# ssh node-1
[root@node-1 ~]# pgrep ldmsd
155
```

EXCEPT THAT YOU CANNOT GDB!!!! HOLY COW! In order to gdb, we need `SYS_PTRACE`
capability, which can be specified with `--cap-add SYS_PTRACE` to the `docker
run` command (or `docker create` command). Unfortunately, there is no way we can
do this using `docker service`. The `dockerd` just doesn't support adding
capabilities to containers managed under services (I verified with the docker
source code). According to docker issue thread
https://github.com/moby/moby/issues/25885#issuecomment-501017588 the feature
is to be released as part of 19.06 or 19.09.

I think for the short term, if we want to gdb inside docker, we should
orchestrate the containers ourselves. We still need Docker Swarm for the network
communication though.


Example Results
===============

```bash
$ python -i test3.py
-- Get or create the cluster --
-- start/check sshd --
-- start/check munged --
-- start/check ldmsd --
... wait a bit to make sure ldmsd's are up
-- ldms_ls to agg-2 --
sampler-2/vmstat
sampler-2/slurm_sampler
sampler-2/procstat
sampler-2/meminfo
sampler-1/vmstat
sampler-1/slurm_sampler
sampler-1/procstat
sampler-1/meminfo

ldms_ls agg-2: Passed
Cancelling job 3
Submitting job ...
jobid: 4
slurm job_id verification on sampler-1: Passed
slurm job_id verification on sampler-2: Passed
>>> cluster.remove()
>>>
```

`tadad` output:
```bash
$ ./tadad
2019-07-03 13:49:10,896 tada_server      INFO   Listening on udp 0.0.0.0:9862
LDMSD - [127.0.0.1:40369]
    2019-07-03 13:50:31 agg + slurm_sampler + slurm_notifier
        passed    1 ldms_ls agg-2, dir result verified
        passed    2 slurm job_id verification on sampler-1, job_id verified
        passed    3 slurm job_id verification on sampler-2, job_id verified
```
