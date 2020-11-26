Quick Links
===========

* [How to develop a test](#toc) (this document)
* [How to run test scripts](TEST.md)
* [tadad(1)](tadad.md)
* [tadaq(1)](tadaq.md)


TOC
===

* [Overview](#overview)
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
      * [Node Spec](#node-spec)
      * [Daemon Spec](#daemon-spec)
        * [slurmd spec](#slurmd-spec)
        * [ldmsd spec](#ldmsd-spec)
      * [Template extension and attribute substitution](#template-extension-and-attribute-substitution)
    * [Spec skeleton](#spec-skeleton)
  * [Start Daemons](#start-daemons)
  * [Cluster and Container Utilities](#cluster-and-container-utilities)
  * [LDMS Utilities](#ldms-utilities)
  * [Slurm Job Utilities](#slurm-job-utilities)
  * [TADA Utilities](#tada-utilities)
* [Debugging](#debugging)
  * [Cleanup](#cleanup)
  * [Container Interactive Shell](#container-interactive-shell)
* [Example Results](#example-results)

Overview
========

`LDMS_Test` is a Python module containing tools to help building a virtual
cluster for testing ldmsd. A virtual cluster is a set of docker containers on a
docker swarm overlay network (a virtual network allowing docker containers to
talk to each other via dockerd swarm members). A virtual cluster has exactly one
network. The hostname of each container must be defined in the `node`
specification and must be unique (within the virtual cluster).

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

# start all daemons according to spec. This ignores the daemons that are
# already running.
cluster.start_daemons()

# get all containers
conts = cluster.containers

# getting a container by hostname or alias
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
to all dockerds across nodes in the bare-metal cluster.


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

The difference is that `create()` will fail if the virtual cluster has already
existed while `get()` won't. Please also be mindful that the spec cannot be
changed after the cluster is created. If `get()` is called with `spec`, it will
be checked against the spec used at create (saved as a label of the docker
network) and throw `RuntimeError('spec mismatch')` if the given spec does not
match the one creating the cluster.

Docker containers could not change their hostname after created. However, node
aliases can be specified in `spec["node_aliases"]` as a dictionary mapping a
hostname to a list of aliases.


### LDMSDClusterSpec

The `spec` is a dictionary containing LDMSD Cluster Spec describing the docker
virtual cluster, nodes in the virtual cluster, and daemons running on them.  The
following is a list of attributes for Spec object:

- `name` is the name of the virtual cluster.
- `description` is a short description of the virtual cluster.
- `templates` is a dictionary of templates to apply with `!extends` special
  keyword attribute.
- `cap_add` is a list of capabilities to add to the containers.
- `cap_drop` is a list of capabilities to drop from the containers.
- `image` is the name of the docker image to use.
- `ovis_prefix` is the path to ovis installation in the host machine. If not
  specified, `/opt/ovis` in the container will not be mounted.
- `env` is a dictionary of cluster-wide environment variables.
- `mounts` is a list of mount points with `SRC:DST:MODE` format in which SRC
  being the source path in the HOST, DST being the destination path in the
  CONTAINER, and MODE being `rw` or `ro`.
- `node_aliases` is a dictionary mapping hostname to a list of hostname aliases.
  This attribute is optional.
- `nodes` is a list of nodes, each item of which describes a node in
   the cluster.

Besides these known attributes, the application can define any attribute to any
object (e.g. "tag": "something"). Even though ignored by the cluster processing
routine, these auxiliary attributes are useful for attribute substitution.
[Template extension and attribute substitution][1] discusses how templates and
attribute substitution work to reduce large text repetition in the spec. Please
see [Node Spec](#node-spec) for the specification of a node object, and [Daemon
Spec](#daemon-spec) for that of the daemon (in a node).


[1]: #template-extension-and-attribute-substitution


#### Node Spec
The node in `spec["nodes"]` is a dictionary describing a node in the cluster
containing the following attributes:
- `hostname` defines the hostname of the container, and is used to construct
  container name with `{clustername}-{hostname}` format.
- `env` is a dicionary of environment variables for the node which is merged
  with cluster-wide env (node-level precedes cluster-level).
- `daemons` is a list of objects describing daemons running on the node.


#### Daemon Spec

The daemon in `spec["nodes"][X]["daemons"]` is a dictionary describing supported
daemons with the following common attributes:
- `name` is the name of the daemon.
- `type` is the type of the supported daemons, which are `sshd`, `munged`,
  `slurmctld`, `slurmd`, and `ldmsd`.

`sshd`, `munged` and `slurmd` daemons do not have extra attributes other than
the common daemon attributes described above.


##### slurmd spec

In addition to `name` and `type` attributes, `slurmd` daemon has the following
extra attributes:
- `plugstack` is a list of dictionary describing Slurm plugin. Each entry in the
  `plugstack` list contains the following attributes:
  - `required` can be True or False describing whether the plugin is required or
    optional. slurmd won't start if the required plugin failed to load.
  - `path` is the path to the plugin.
  - `args` is a list of arguments (strings) to the plugin.


##### ldmsd spec

In addition to `name` and `type` attributes, `ldmsd` daemon contains the
following extra attributes:
- `listen_port` is an integer describing the daemon listening port.
- `listen_xprt` is the LDMS transport to use (`sock`, `ugni` or `rdma`).
- `listen_auth` is the LDMS authentication method to use.
- `samplers` (optional) is a list of sampler plugins (see below).
- `prdcrs` (optional) is a list of producers (see below).
- `config` (optional) is a list of strings for ldmsd configuration commands.

The `samplers` list is processed first (if specified), then `prdcrs` (if
specified), and `config` (if specified) is processed last.

The sampler object in ldmsd daemon `samplers` list is described as follows:
- `plugin` is the plugin name.
- `interval` is the sample interval (in micro seconds).
- `offset` is the sample offset (in micro seconds).
- `start` can be True or False -- marking whether the plugin needs a start
  command (some plugins update data by events and do not require start command).
- `config` is a list of strings `NAME=VALUE` for plugin configuration arguments.

The producer object in the ldmsd daemon `prdcrs` list is described as
follows:
- `host` is the hostname of the ldmsd to connect to.
- `port` is an integer describing the port of the target ldmsd.
- `xprt` is the transport type of the target ldmsd.
- `type` is currently be `active` only.
- `interval` is the connection retry interval (in micro-seconds).


#### Template extension and attribute substitution

Templates and `%ATTR%` substitution can be used to reduce repititive
descriptions in the spec. The `!extends` object attribute is reserved for
instructing the spec mechanism to apply the referred template (a member of
`templates` in the top-level spec). The attributes locally defined in the object
will override those from the template. An object may `!extends` only one
template. A template can also `!extends` another template (analogous to
subclassing). The templates are applied to objects in the spec before the
`%ATTR%` substitution is processed.

The `%ATTR%` appearing in the value string (e.g. `/%hostname%/%plugin%`) is
replaced by the value of the attribute ATTR of the nearest container object
(starts from self, container of self, container of container of self, ..).

Consider the following example:

```python
{
    "templates": {
        "node-temp": {
            "daemons": [
                { "name": "sshd", "type": "sshd" },
                { "name": "sampler", "!extends": "ldmsd-sampler" },
            ],
        },
        "ldmsd-base": {
            "type": "ldmsd",
            "listening_port": 10000,
        },
        "ldmsd-sampler": {
            "!extends": "ldmsd-base",
            "samplers": [
                {
                    "plugin": "meminfo",
                    "config": [
                        "instance=%hostname%/%plugin%",
                    ],
                },
            ],
        },
    },
    "nodes": [
        { "hostname": "node-1", "!extends": "node-temp" },
        { "hostname": "node-2", "!extends": "node-temp" },
        ...
    ],
}
```

The nodes extend `node-temp` template resulting in them having `daemons` defined
in the template (`sshd` and `sampler`). The `sampler` extends `ldmsd-sampler`,
which also extends `ldmsd-base`. As a result, the `sampler` daemon object get
extended to have `type` being `ldmsd`, `listening_port` being 10000, and a list
of `samplers`.

The `%hostname%` and `%plugin%` in `instance=%hostname%/%plugin%` is later
substituted with the nearest attributes by containment hierarchy. For `node-1`,
the string becomes `instance=node-1/meminfo` because the nearest `hostname` is
the hostname attribute defined by the node object, and the nearest `plugin`
attribute is the attribute defined in the sampler plugin object itself.


### Spec skeleton

The following is the skeleton of the spec:

```python
{
    "name" : "NAME_OF_THE_VIRTUAL_CLUSTER",
    "description" : "DESCRIPTION OF THE CLUSTER",
    "templates" : { # a list of templates
        "TEMPLATE_NAME": {
            "ATTR": VALUE,
            ...
        },
        ...
    },
    "cap_add": [ "DOCKER_CAPABILITIES", ... ],
    "cap_drop": [ "DOCKER_CAPABILITIES", ... ],
    "image": "DOCKER_IMAGE_NAME",
             # Optional image name to run each container.
             # default: "ovis-centos-build"
    "ovis_prefix": "PATH-TO-OVIS-IN-HOST-MACHINE",
                   # Path to OVIS in the host machine. This will be
                   # mounted to `/opt/ovis` in the container.
                   # default: "/opt/ovis"
    "env" : { "FOO": "BAR" }, # additional environment variables
                              # applied cluster-wide. The
                              # environment variables in this list
                              # (or dict) has the least precedence.
    "mounts": [ # additional mount points for each container, each
                # entry is a `str` and must have the following
                # format
        "SRC_ON_HOST:DEST_IN_CONTAINER:MODE",
                                     # MODE can be `ro` or `rw`
        ...
    ],
    "nodes" : [ # a list of node spec
        {
            "hostname": "NODE_HOSTNAME",
            "env": { ... }, # Environment for this node.
                            # This is merged into cluster-wide
                            # environment before applying to
                            # exec_run. The variables in the
                            # node overrides those in the
                            # cluser-wide.
            "daemons": [ # list of daemon spec
                # Currently, only the following daemon types
                # are supported: sshd, munged, slurmd, slurmctld,
                # and ldmsd. Currently, we do not support two
                # daemons of the same type. Each type has different
                # options described as follows.
                {
                    "name": "DAEMON_NAME0",
                    "type": "sshd",
                    # no extra options
                },
                {
                    "name": "DAEMON_NAME1",
                    "type": "munged",
                    # no extra options
                },
                {
                    "name": "DAEMON_NAME2",
                    "type": "slurmctld",
                    # no extra options
                },
                {
                    "name": "DAEMON_NAME3",
                    "type": "slurmd",
                    "plugstack" : [ # list of slurm plugins
                        {
                            "required" : True or False,
                            "path" : "PATH_TO_PLUGIN",
                            "args" : [
                                "ARGUMENTS",
                                ...
                            ],
                        },
                        ...
                    ],
                },
                {
                    "name": "DAEMON_NAME4",
                    "type": "ldmsd",
                    "listen_port" : PORT,
                    "listen_xprt" : "XPRT",
                    "listen_auth" : "AUTH",
                    "samplers": [
                        {
                            "plugin": "PLUGIN_NAME",
                            "interval": INTERVAL_USEC,
                            "offset": OFFSET,
                            "start": True or False,
                            "config": [ # list of "NAME=VALUE"
                                        # plugin configuration
                                "NAME=VALUE",
                                ...
                            ],
                        },
                        ...
                    ],
                    "prdcrs": [ # each prdcr turns into prdcr_add
                                # command.
                        {
                            "host" : "HOST_ADDRESS",
                            "port" : PORT,
                            "xprt" : "XPRT",
                            "type" : "active",
                            "interval" : INTERVAL_USEC,
                        },
                        ...
                    ],
                    "config": [
                        # additional config commands
                        "CONFIG_COMMANDS",
                        ...
                    ]
                },
                ...
            ],
        },
        ...
    ],
}
```



The following is an example defining a virtual cluster of 5 nodes: 2 compute
nodes with sampler daemon and slurmd, 2 aggregator nodes with ldmsd running in
aggregator mode, and 1 head node running slurmctld.

```python
{
    "name" : "mycluster",
    "description" : "My test cluster with ldmsd and slurm",
    "cap_add": [ "SYS_PTRACE" ], # for GDB in the containers
    "image": "ovis-centos-build",
    "ovis_prefix": "/home/bob/opt/ovis",
    "env" : { "FOO": "BAR" },
    "mounts": [
        "/home/bob/db:/db:rw", # for writing data out from container
        "/home/bob/projects/ovis:/home/bob/projects/ovis:ro", # for gdb
    ],
    "templates" : { # generic template can apply to any object by "!extends"
        "compute-node" : { # compute node template
            "daemons" : [  # running munged, slurmd, and ldmsd (sampler)
                {
                    "name" : "munged",
                    "type" : "munged",
                },
                {
                    "name" : "sampler-daemon",
                    "requires" : [ "munged" ],
                    "!extends" : "ldmsd-sampler", # see below
                },
                {
                    "name" : "slurmd",
                    "requires" : [ "munged" ],
                    "!extends" : "slurmd", # see below
                },
            ],
        },
        "slurmd" : { # template for slurmd
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
        "ldmsd-sampler" : { # template for ldmsd (sampler)
            "type" : "ldmsd",
            "listen_port" : 10000,
            "listen_xprt" : "sock",
            "listen_auth" : "none",
            "samplers" : [
                {
                    "plugin" : "slurm_sampler",
                    "!extends" : "sampler_plugin",
                    "start" : False, # override
                },
                {
                    "plugin" : "meminfo",
                    "!extends" : "sampler_plugin", # see below
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
        "sampler_plugin" : { # template for common sampler plugin config
            "interval" : 1000000,
            "offset" : 0,
            "config" : [
                "component_id=%component_id%",
                "instance=%hostname%/%plugin%",
                "producer=%hostname%",
            ],
            "start" : True,
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
                    "type" : "ldmsd",
                    "listen_port" : 20000,
                    "listen_xprt" : "sock",
                    "listen_auth" : "none",
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
                    "type" : "ldmsd",
                    "listen_port" : 20001,
                    "listen_xprt" : "sock",
                    "listen_auth" : "none",
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

}
```


Start Daemons
-------------
The containers in the virtual cluster has only main process (`/bin/bash`)
running initially. We need to manually start the daemons.

`cluster.start_daemons()` is a convenient method that subsequently calls
`container.start_daemons()` for each container in the cluster.
`container.start_daemons()` starts all daemon sequentially according to the
`spec["nodes"][X]["daemons"]` definition. For each daemon, if it has already
been started, the starting routine does nothing. The following is a list of
`start_*()` methods of supported daemons if one wishes to start each daemon
manually:

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

For the information on how to operate `tadad`, please see [tadad(1)](tadad.md).

To report test results to TADA Daemon (`tadad`), first we need to create a
TADATest object and define assertions as follows:

```python
test = TADATest(test_suite = "LDMSD",
            test_type = "LDMSD",
            test_name = "agg + slurm_sampler + slurm_notifier",
            test_user = "bob",
            commit-id = "abcdefg",
            tada_addr = "cygnus-08:9862")
test.add_assertion(1, "ldms_ls agg-2")
test.add_assertion(2, "slurm job_id verification on sampler-1")
test.add_assertion(3, "slurm job_id verification on sampler-2")
```

The `test_suite`, `test_type`, `test_name`, `test_user` and `commit_id`
combination identifies the test run. When the test rerun with the same
combination, the results in the database is over-written. The `commit_id` is
meant to be the commit ID from the target program being tested (e.g. ldmsd).

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

Finally, notify `tadad` that the test finishes by:
```python
test.finish()
```

The defined assertions that were not tested will be reported as `SKIPPED`.


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
