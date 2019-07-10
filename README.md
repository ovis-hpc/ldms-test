TOC
===

* [Overview](#Overview)
* [SYNOPSIS](#synopsis)
* [Docker Host Cluster Setup](#docker-host-cluster-setup)
* [Docker Setup](#docker-setup)
  * [dockerd network port](#dockerd-network-port)
  * [Setting Up Docker Swarm](#setting-up-docker-swarm)
  * [Preparing Docker Image](#preparing-docker-image)
  * [Distribute Docker Image](#distribute-docker-image)
* [Python Module Usage Guide](#python-module-usage-guide)
  * [Create (or Get) Virtual Cluster](#create-or-get-virtual-cluster)
    * [LDMSDClusterSpec](#ldmsdclusterspec)
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
cluster for testing ldmsd. A virtual cluster is a set of docker containers on a
docker swarm overlay network (a virtual network allowing docker containers to
talk to each other via dockerd swarm members). A virtual cluster has exactly one
network. The hostname of each container is `"node-{i}"`, i = 1 .. N.

`DockerCluster` implements basic routines for handling the virtual cluster (e.g.
create, remove), and the `DockerClusterContainer` implements basic routines for
interacting with each of the containers (e.g. getting aliases of the virtual
node). The `LDMSDCluster` extends `DockerCluster` and `LDMSDContainer` extends
`DockerClusterContainer` to provide ldmsd-specific routines (e.g. start ldmsd,
perform `ldms_ls`). Please see [Python Module Usage
Guide](#python-module-usage-guide) for a guideline on how to use the virtual
cluster in Python. For full details of the APIs, please see in-line Python
documentation in the module.

In this document, we will use our bare-metal cluster "cygnus" (hostnames:
cygnus-01..08) as an example on how to deploy and run the test infrastructure.
Cygnus cluster runs on CentOS 7.6.1810.

SYNOPSIS
========

The following is a synopsis of how to use the utilities in the module.

```python
from LDMS_Test import LDMSDCluster

spec = { ... } # see LDMSDClusterSpec section

# Three choices to obtain a virtual cluster.
# 1) create it
cluster = LDMSDCluster.create(spec = spec) # fail if cluster existed
# 2) get the existing one
cluster = LDMSDCluster.get(name = 'mycluster')
# 3) get and create if not existed
cluster = LDMSDCluster.get(name = 'mycluster', create = True, spec = spec)


# start daemons you want to use on the cluster
#   if the daemon has already started, it does nothing

# SSH - good for debugging
cluster.start_sshd()

# munged - needed for munge authentication
cluster.start_munged()

# slurm services - slurmd starts on "sampler" nodes, and slurmctld on last node
cluster.start_slurm()

# start ldmsd
cluster.start_ldmsd()

# get all containers
conts = cluster.containers

# getting a container by name or alias
cont = cluster.get_container('node-1')

# write a file in a container
cont.write_file('/path/to/file', 'string content')

# read content of a file in a container
_str = cont.read_file('/path/to/file')

# execute a program in a container and return the results
rc, output = cont.exec_run("some_program some_param")
    # by default, `exec_run()` is blocking. Please consult
    # `help(docker.models.containers.Container.exec_run)` python doc
    # for advanced usage.

# destroy the cluster
cluster.remove()
```


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
infrastructure. `cygnus-{01..08}` all have `docker-ce` version 18.09 installed,
and `docker.service` system service is enabled so that `dockerd` is started on
boot.

```bash
$ systemctl enable docker.service
```

The users that need to run the virtual cluster testing facility must also be a
member of `docker` user group, otherwise they cannot create/delete Docker
Networks or Docker Containers.


dockerd network port
--------------------

By default,
dockerd is configured to listen only on to the unix-domain socket with dockerd
CLI options in `/usr/lib/systemd/system/docker.service` file. We need to
configure dockerd to also listen on port 2375 (its default network port) because
a docker client (our Python test script) needs a connection to the dockerd to
manipulate containers running under it (containers in our virtual cluster).
Even though dockerd support `/etc/docker/daemon.json` dockerd configuration, the
CLI option takes precedence. As such, we have to override the `docker.service`
to get rid of the CLI option by creating an override file for `docker.service`
as follows:

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
$ systemctl stop docker.service
$ systemctl daemon-reload
$ systemctl start docker.service
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

Issuing `pssh.all -i systemctl stop docker.service` would stop dockerd on all
nodes in the cluster.


Setting Up Docker Swarm
-----------------------

We need docker daemons to run in swarm mode so that the containers living in
different hosts (managed by different dockerds) can talk to each other over the
overlay network. To setup a swarm, first pick a node (cygnys-08 in this case),
and issue `docker swarm init`. This will initialize the dockerd in the node as
swarm manager and generate a token for other nodes to join the swarm.

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
# and is less than 7. A manager by default is also a worker.
```


Preparing Docker Image
----------------------

Dockerfile is provided for building the image described in this section. The
following is the command to build the image using the provided Dockerfile in
`docker/` directory.

```bash
$ cd docker/
$ docker build -t ovis-centos-build .
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
required daemons, 3) perform test, 4) report test. For debugging, please see
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


### LDMSDClusterSpec

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


Start Daemons
-------------

The containers in the virtual cluster has only main process (`/bin/bash`)
running initially. We need to manually start the daemons. All `start_*()`
methods of supported daemons do nothing if the daemon has already started.
The following is the list of supported daemons and the corresponding
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
creation failure.

To remove using python shell:
```bash
$ python
>>> from LDMS_Test import DockerCluster
>>> cluster = DockerCluster.get('mycluster')
>>> cluster.remove()
```

The following is the commands to remove containers and the network using CLI if
the Python doesn't work.

```bash
# find your cluster network first
$ docker network ls
NETWORK ID          NAME                     DRIVER              SCOPE
d6d69740ddf1        bridge                   bridge              local
b49037b44bdd        docker_gwbridge          bridge              local
9359ff933aec        host                     host                local
ihixyccuq8g4        ingress                  overlay             swarm
gc313pefx6g3        mycluster                overlay             swarm
216ddeb1cd8c        none                     null                local

# in this example, the network name is mycluster (it also is the
# cluster name)

# See containers of mycluster in all hosts
$ pssh -H "$(echo cygnus-{01..08})" -i "docker ps --filter=network=mycluster"
[1] 15:37:01 [SUCCESS] cygnus-01
CONTAINER ID        IMAGE               COMMAND             CREATED              STATUS              PORTS               NAMES
31fff3c5de9f        centos:7            "/bin/bash"         About a minute ago   Up About a minute                       mycluster-35
c2c1af104786        centos:7            "/bin/bash"         About a minute ago   Up About a minute                       mycluster-34
7a122d9574f3        centos:7            "/bin/bash"         About a minute ago   Up About a minute                       mycluster-33
df4b1fca3340        centos:7            "/bin/bash"         About a minute ago   Up About a minute                       mycluster-32

...

# Remove containers
$ pssh -H "$(echo cygnus-{01..08})" -i 'X=$(docker ps --filter=network=mycluster -q) ; docker rm -f $X'

# Now, remove the network
$ docker network rm mycluster
```


Container Interactive Shell
---------------------------

Sometimes, we want to have an interactive shell to the cluster to look around.

To do so, first we need to find out where our nodes are:
```bash
narate@cygnus-08 ~/test/ldms
$ pssh -H "$(echo cygnus-{01..08})" -i 'docker ps --filter=network=mycluster'
[1] 16:04:25 [SUCCESS] cygnus-01
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
7ef03cc2f29e        ovis-centos-build   "/bin/bash"         21 seconds ago      Up 18 seconds                           mycluster-1
[2] 16:04:25 [SUCCESS] cygnus-03
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
10ea81f6f877        ovis-centos-build   "/bin/bash"         16 seconds ago      Up 13 seconds                           mycluster-3
[3] 16:04:25 [SUCCESS] cygnus-04
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
453e837b5b52        ovis-centos-build   "/bin/bash"         14 seconds ago      Up 11 seconds                           mycluster-4
[4] 16:04:25 [SUCCESS] cygnus-02
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
bfee6eb67b75        ovis-centos-build   "/bin/bash"         19 seconds ago      Up 16 seconds                           mycluster-2
[5] 16:04:25 [SUCCESS] cygnus-08
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
[6] 16:04:25 [SUCCESS] cygnus-05
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
2ae4d0ed09c2        ovis-centos-build   "/bin/bash"         11 seconds ago      Up 7 seconds                            mycluster-5
[7] 16:04:25 [SUCCESS] cygnus-06
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
[8] 16:04:25 [SUCCESS] cygnus-07
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES

```

`mycluster-5` happened to be on `cygnus-05`. We have an interactive shell to
it by:

```
$ docker -H cygnus-05 exec -it mycluster-5 bash
[root@node-5 /]#
```

From here, we can do all sorts of things, e.g.

```
[root@node-5 /]# ssh node-1
[root@node-1 ~]# pgrep ldmsd
160
```

If the cluster is created with `cap_add = ['SYS_PTRACE']`, we can also attach a
gdb to it as follows:

```
[root@node-1 ~]# gdb -p 160
GNU gdb (GDB) Red Hat Enterprise Linux 7.6.1-114.el7
Copyright (C) 2013 Free Software Foundation, Inc.
License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.  Type "show copying"
and "show warranty" for details.
This GDB was configured as "x86_64-redhat-linux-gnu".
For bug reporting instructions, please see:
<http://www.gnu.org/software/gdb/bugs/>.
Attaching to process 160
Reading symbols from /opt/ovis/sbin/ldmsd...done.
Reading symbols from /home/narate/opt/ovis/lib/libovis_event.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libovis_event.so.0
Reading symbols from /lib64/libm.so.6...(no debugging symbols found)...done.
Loaded symbols for /lib64/libm.so.6
Reading symbols from /home/narate/opt/ovis/lib/libovis_ctrl.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libovis_ctrl.so.0
Reading symbols from /home/narate/opt/ovis/lib/libldms.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libldms.so.0
Reading symbols from /home/narate/opt/ovis/lib/librequest.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/librequest.so.0
Reading symbols from /home/narate/opt/ovis/lib/libldmsd_stream.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libldmsd_stream.so.0
Reading symbols from /home/narate/opt/ovis/lib/libjson_util.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libjson_util.so.0
Reading symbols from /lib64/libc.so.6...(no debugging symbols found)...done.
Loaded symbols for /lib64/libc.so.6
Reading symbols from /lib64/libcrypto.so.10...Reading symbols from
/lib64/libcrypto.so.10...(no debugging symbols found)...done.
(no debugging symbols found)...done.
Loaded symbols for /lib64/libcrypto.so.10
Reading symbols from /home/narate/opt/ovis/lib/libmmalloc.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libmmalloc.so.0
Reading symbols from /home/narate/opt/ovis/lib/libcoll.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libcoll.so.0
Reading symbols from /home/narate/opt/ovis/lib/libovis_third.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libovis_third.so.0
Reading symbols from /home/narate/opt/ovis/lib/libovis_util.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libovis_util.so.0
Reading symbols from /lib64/librt.so.1...(no debugging symbols found)...done.
Loaded symbols for /lib64/librt.so.1
Reading symbols from /home/narate/opt/ovis/lib/libzap.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libzap.so.0
Reading symbols from /lib64/libdl.so.2...(no debugging symbols found)...done.
Loaded symbols for /lib64/libdl.so.2
Reading symbols from /lib64/libpthread.so.0...(no debugging symbols
found)...done.
[New LWP 166]
[New LWP 165]
[New LWP 164]
[New LWP 163]
[New LWP 162]
[New LWP 161]
[Thread debugging using libthread_db enabled]
Using host libthread_db library "/lib64/libthread_db.so.1".
Loaded symbols for /lib64/libpthread.so.0
Reading symbols from /lib64/ld-linux-x86-64.so.2...(no debugging symbols
found)...done.
Loaded symbols for /lib64/ld-linux-x86-64.so.2
Reading symbols from /lib64/libz.so.1...Reading symbols from
/lib64/libz.so.1...(no debugging symbols found)...done.
(no debugging symbols found)...done.
Loaded symbols for /lib64/libz.so.1
Reading symbols from /opt/ovis/lib/ovis-lib/libzap_sock.so...done.
Loaded symbols for /opt/ovis/lib/ovis-lib/libzap_sock.so
Reading symbols from /home/narate/opt/ovis/lib/libldms_auth_none.so...done.
Loaded symbols for /home/narate/opt/ovis/lib/libldms_auth_none.so
Reading symbols from /opt/ovis/lib/ovis-ldms/libslurm_sampler.so...done.
Loaded symbols for /opt/ovis/lib/ovis-ldms/libslurm_sampler.so
Reading symbols from /opt/ovis/lib/ovis-ldms/libmeminfo.so...done.
Loaded symbols for /opt/ovis/lib/ovis-ldms/libmeminfo.so
Reading symbols from /home/narate/opt/ovis/lib/libsampler_base.so.0...done.
Loaded symbols for /home/narate/opt/ovis/lib/libsampler_base.so.0
Reading symbols from /opt/ovis/lib/ovis-ldms/libvmstat.so...done.
Loaded symbols for /opt/ovis/lib/ovis-ldms/libvmstat.so
Reading symbols from /opt/ovis/lib/ovis-ldms/libprocstat.so...done.
Loaded symbols for /opt/ovis/lib/ovis-ldms/libprocstat.so
0x00007ff494407fad in nanosleep () from /lib64/libc.so.6
Missing separate debuginfos, use: debuginfo-install
glibc-2.17-260.el7_6.6.x86_64 openssl-libs-1.0.2k-16.el7_6.1.x86_64
zlib-1.2.7-18.el7.x86_64
(gdb) b sample
Breakpoint 1 at 0x7ff48ee594fd: sample. (4 locations)
(gdb) c
Continuing.
[Switching to Thread 0x7ff490c7c700 (LWP 165)]

Breakpoint 1, sample (self=0x7ff48f25f0e0 <vmstat_plugin>) at
../../../../ldms/src/sampler/vmstat.c:205
205             if (!set) {
(gdb)

```

NOTE: The source code is displayed in the gdb session because the path to the
ovis development tree is mounted as the same path in the container. For example,
adding `"/home/bob/ovis:/home/bob/ovis:ro"` to `mounts` in the spec.


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
