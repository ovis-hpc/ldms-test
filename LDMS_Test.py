import os
import re
import sys
import time
import json
import socket
import docker
import subprocess

import pdb

from functools import wraps
from StringIO import StringIO
from distutils.version import LooseVersion

use_docker_service = False
# NOTE 1: If `use_docker_service` is True, LDMSDCluster will be based on
# DockerClusterService class which use Docker Service to create the virtual
# cluster. Otherwise, LDMSDCluster is based on DockerCluster which creates the
# virtual cluster by iteratively create the Docker Network and Docker Containers
# by itself.
#
# NOTE pros/cons
# - DockerClusterService
#   - pros:
#     - Easy to cleanup on CLI, by `docker rm SERVICE_NAME`
#     - Easy to list running clusters, by `docker service ls`
#     - The Service logic in the swarm of dockerds manages the containers.
#       We just give the spec.
#   - cons:
#     - Since the service manages containers, we have little control over them.
#       For example, when one container dies, a new container will be created
#       to fill its spot.
#     - Cannot add or drop capabilities at create, resulting in not able to
#       ues gdb (it needs `SYS_PTRACE` capability).
#
# - DockerCluster
#   - pros:
#     - We create containers directly. Thus, we have control on their parameters
#       at create.
#     - We can use GDB!!!
#   - cons:
#     - `docker network ls` to list the cluster network. If the network existed,
#       the cluster is there (but could be bad).
#     - Quite cumbersome to cleanup in CLI. (but in python, you can still use
#       `cluster.remove()`).


# `D` Debug object to store values for debugging
class Debug(object): pass
D = Debug()

#############
#  Helpers  #
#############

class cached_property(object):
    """`@cached_property` decorator to make a cached property (getter-only)

    NOTE: The property's value is stored in `self.__cache__` dictionary.
    """
    def __init__(self, func):
        self.func = func
        self.name = func.func_name

    def __get__(self, obj, _type):
        cache = getattr(obj, "__cache__", dict())
        if not cache: # newly created cache
            obj.__cache__ = cache
        try:
            return cache[self.name]
        except KeyError:
            cache[self.name] = val = self.func(obj)
            return val

def env_dict(env):
    """Make env dict(NAME:VALUE) from list(NAME=VALUE) or dict(NAME:VALUE)

    Docker API understands two forms of `env` parameters: list(NAME=VALUE) and
    dict(NAME:VALUE). This function is to convert them into dict form to make us
    work a little easier.
    """
    if type(env) == dict:
        return dict(env) # return a copy
    if type(env) != list:
        raise TypeError("`env` is not a list nor a dict")
    return dict( e.split("=", 1) for e in env )

def get_docker_clients():
    """Get all docker clients to dockerds in the swarm"""
    dc = docker.from_env()
    nodes = dc.nodes.list()
    addrs = [ n.attrs["Description"]["Hostname"] for n in nodes ]
    addrs.sort()
    return [ docker.DockerClient(base_url = "tcp://{}:2375".format(a)) \
                    for a in addrs ]


#####################################################
#                                                   #
#   Convenient wrappers for docker.models classes   #
#                                                   #
#####################################################

class Container(object):
    """Docker Container Wrapper

    This class wraps docker.models.containers.Container, providing additional
    convenient methods (such as `read_file()` and `write_file()`) and properties
    (e.g. `ip_addr`). The wrapper only exposes the following APIs:
        - attrs : dict() of Container attributes,
        - name : the name of the container,
        - client : the docker client handle for manipulating the container,
        - exec_run() : execute a program inside the container,
        - remove() : remove the container.

    The following is the additional convenient properties and methods:
        - interfaces : a list of (network_name, IP_address) of the container,
        - ip_addr : the IP address of the first interface.
        - hostname : the hostname of the container,
        - env : the environment variables of the container (from config),
        - write_file() : a utility to write data to a file in the container,
        - read_file() : a utility to read a file in the container (return str).

    """
    def __init__(self, obj):
        if not isinstance(obj, docker.models.containers.Container):
            raise TypeError("obj is not a docker Container")
        self.obj = obj
        self.attrs = obj.attrs
        self.name = obj.name
        self.client = obj.client

    def is_running(self):
        """Check if the container is running"""
        try:
            return self.obj.attrs["State"]["Status"] == "running"
        except:
            return False

    def wait_running(self, timeout=10):
        """Wait until the container become "running" or timeout

        Returns
        -------
        True  if the container becomes running before timeout
        False otherwise
        """
        t0 = time.time()
        while not self.is_running():
            t1 = time.time()
            if t1-t0 > timeout:
                return False
            self.obj.update()
            time.sleep(1)
        return True

    def exec_run(self, *args, **kwargs):
        self.wait_running()
        return self.obj.exec_run(*args, **kwargs)

    def remove(self, **kwargs):
        self.obj.remove(**kwargs)

    @property
    def ip_addr(self):
        try:
            return self.interfaces[0][1] # address of the first network interface
        except:
            return None

    @property
    def interfaces(self):
        """Return a list() of (network_name, IP_address) of the container."""
        return [ (k, v['IPAddress']) for k, v in \
                 self.attrs['NetworkSettings']['Networks'].iteritems() ]

    @property
    def hostname(self):
        """Return hostname of the container"""
        return self.attrs["Config"]["Hostname"]

    @property
    def env(self):
        """Return environment from container configuration.

        Please note that the environment in each `exec_run` may differ.
        """
        return self.attrs["Config"]["Env"]

    def write_file(self, path, content):
        """Write `content` to `path` in the container"""
        cmd = "/bin/bash -c 'cat -  >{}'".format(path)
        erun = self.exec_run(cmd, stdin=True, socket=True)
        sock = erun.output
        sock.send(content)
        sock.close()

    def read_file(self, path):
        """Read file specified by `path` from the container"""
        cmd = "cat {}".format(path)
        erun = self.exec_run(cmd)
        return erun.output


class Service(object):
    """Docker Service Wrapper

    This class wraps docker.models.services.Service and provides additional
    convenient properties and methods. The wrapper only exposes a subset of
    docker Service APIs as follows:
        - attrs : the dictionary containing docker Service information,
        - name : the name of the docker Service,
        - client : the docker Client handle.
        - remove() : remove the service

    The following is the list of additional properties and methods provided by
    this class:
        - mounts : mount points (from config),
        - env : environment variables (from config),
        - net : docker Network associated with this service,
        - containers : a list of wrapped Container objects (cached property of
          get_containers()),
        - tasks_running() : check if all tasks (containers) are running,
        - wait_tasks_running() : block-waiting until all tasks become running
          or timeout,
        - get_containers() : return a list of wrapped Container objects in the
          service,
        - build_etc_hosts() : returns `str` build for `/etc/hosts` for the
          containers of this service (as a virtual cluster),
        - update_etc_hosts() : like build_etc_hosts(), but also overwrite
          `/etc/hosts` in all containers of this service.
    """
    def __init__(self, obj):
        if not isinstance(obj, docker.models.services.Service):
            raise TypeError("obj is not a docker Service")
        self.obj = obj
        self.attrs = obj.attrs # expose attrs
        self.name = obj.name
        self.client = obj.client

    def remove(self):
        self.obj.remove()

    def tasks_running(self):
        """Returns `True` if all tasks are in "running" state"""
        try:
            tasks = self.obj.tasks()
            nrep = self.obj.attrs["Spec"]["Mode"]["Replicated"]["Replicas"]
            if len(tasks) != nrep:
                return False
            for t in tasks:
                if t['Status']['State'] != 'running':
                    return False
        except:
            return False
        return True

    def wait_tasks_running(self, timeout = 10):
        """Return `True` if all tasks become "running" before timeout (sec)"""
        t0 = time.time()
        while not self.tasks_running():
            t1 = time.time()
            if t1-t0 > timeout:
                return False
            time.sleep(1)
        return True

    def get_containers(self, timeout = 10):
        """Return a list of docker Container within the service"""
        if not self.wait_tasks_running(timeout = timeout):
            raise RuntimeError("Some tasks (containers) are not running.")
        cont_list = list()
        d = docker.from_env()
        for t in self.obj.tasks():
            D.t = t
            nid = t['NodeID']
            node = d.nodes.get(nid)
            addr = node.attrs['Description']['Hostname'] + ":2375"
            # client to remote dockerd
            ctl = docker.from_env(environment={'DOCKER_HOST': addr})
            cont_id = t['Status']['ContainerStatus']['ContainerID']
            D.cont = cont = ctl.containers.get(cont_id)
            cont_list.append(Container(cont))
        cont_list.sort(lambda a,b: cmp(LooseVersion(a.name),LooseVersion(b.name)))
        return cont_list

    def build_etc_hosts(self, node_aliases = {}):
        """Returns the generated `/etc/hosts` content"""
        sio = StringIO()
        sio.write("127.0.0.1 localhost\n")
        for cont in self.containers:
            name = cont.attrs["Config"]["Hostname"]
            networks = cont.attrs["NetworkSettings"]["Networks"]
            for net_name, net in networks.iteritems():
                addr = net["IPAddress"]
                sio.write("{} {}".format(addr, name))
                alist = node_aliases.get(name, [])
                if type(alist) == str:
                    alist = [ alist ]
                for a in alist:
                    sio.write(" {}".format(a))
                sio.write("\n")
        return sio.getvalue()

    def update_etc_hosts(self, node_aliases = {}):
        """Update entries in /etc/hosts"""
        etc_hosts = self.build_etc_hosts(node_aliases = node_aliases)
        conts = self.get_containers()
        for cont in conts:
            cont.write_file("/etc/hosts", etc_hosts)

    @property
    def mounts(self):
        cont_spec = self.attrs['Spec']['TaskTemplate']['ContainerSpec']
        mounts = []
        for m in cont_spec.get('Mounts', []):
            mode = "ro" if "ReadOnly" in m else "rw"
            mounts.append("{}:{}:{}".format(m['Source'], m['Target'], mode))
        return mounts

    @property
    def env(self):
        """Cluster-wide environments"""
        cont_spec = self.attrs['Spec']['TaskTemplate']['ContainerSpec']
        return cont_spec.get('Env', [])

    @cached_property
    def containers(self):
        return self.get_containers(timeout = 60)

    @property
    def net(self):
        return [ self.client.networks.get(n["Target"]) \
                    for n in self.attrs["Spec"]["TaskTemplate"]["Networks"] ]

    @property
    def labels(self):
        return self.attrs["Spec"]["Labels"]


class Network(object):
    """Docker Network Wrapper"""

    def __init__(self, obj):
        if type(obj) != docker.models.networks.Network:
            raise TypeError("obj is not a docker Network object")
        self.obj = obj
        self.clients = get_docker_clients()

    @classmethod
    def create(cls, name, driver='overlay', scope='swarm', attachable=True,
                    labels = None):
        """A utility to create and wrap the docker network"""
        client = docker.from_env()
        try:
            obj = client.networks.create(name=name, driver=driver,
                                     scope=scope, attachable=attachable,
                                     labels = labels)
        except docker.errors.APIError, e:
            if e.status_code != 409: # other error, just raise it
                raise
            msg = e.explanation + ". This could be an artifact from previous " \
                  "run. To remove the network, all docker objects using net " \
                  "network must be remvoed first (e.g. service, container). " \
                  "Then, remove the network with `docker network rm {}`." \
                  .format(name)
            raise RuntimeError(msg)
        return Network(obj)

    @classmethod
    def get(cls, name, create = False, **kwargs):
        """Find (or optionally create) and wrap docker network"""
        client = docker.from_env()
        try:
            obj = client.networks.get(name)
        except docker.errors.NotFound:
            if not create:
                raise
            obj = Network.create(name, **kwargs)
        return Network(obj)

    @property
    def name(self):
        return self.obj.name

    @property
    def short_id(self):
        return self.obj.short_id

    def rm(self):
        self.obj.remove()

    def remove(self):
        self.obj.remove()

    @property
    def containers(self):
        """Containers in the network"""
        conts = []
        for c in self.clients:
            try:
                obj = c.networks.get(self.name)
            except docker.errors.NotFound:
                continue # skip clients not participating in our network
            D.obj = obj
            _conts = obj.attrs["Containers"]
            _conts = _conts if _conts else {}
            for cont_id in _conts:
                try:
                    cont = c.containers.get(cont_id)
                    conts.append(Container(cont))
                except docker.errors.NotFound:
                    continue # ignore the host-side endpoint appearing as
                             # container.
        return conts

    @property
    def labels(self):
        """Get labels"""
        return self.obj.attrs["Labels"]


################################################################################

class LDMSD(object):
    def __init__(self, hostname, network,
                 image="ovis-centos-build",
                 prefix='/opt/ovis', db_root='/DATA',
                 filer='10.10.0.16',
                 listen_port=10000,
                 listen_xprt='sock',
                 config_file=None,
                 log_file=None,
                 log_level='ERROR',
                 auth_name='munge',
                 sample_interval=1000000,
                 component_id=10000,
                 environment=None):
        self.client = docker.from_env()
        self.hostname = hostname
        self.image = image
        self.network_name = network.name
        self.prefix = prefix
        self.db_root = db_root
        if not config_file:
            self.config_file = '/opt/ovis/etc/ldmsd.conf'
        else:
            self.config_file = config_file
        self.listen_port = listen_port
        self.listen_xprt = listen_xprt
        self.cont_name=self.hostname + '-' + str(self.listen_port)
        if log_file:
            self.log_file = log_file
        else:
            self.log_file = '/var/log/' + self.cont_name + '.log'
        self.log_level = log_level
        self.auth_name = auth_name
        self.filer = filer
        self.container = None
        self.component_id = component_id
        self.sample_interval=sample_interval
        if environment:
            self.environment = environment
        else:
            env = []
            p = "LD_LIBRARY_PATH=/opt/ovis/lib64"
            e = os.getenv("LD_LIBRARY_PATH")
            if e:
                p += ':' + e
            env.append(p)

            env.append("PATH=/opt/ovis/bin:/opt/ovis/sbin:" + os.getenv("PATH"))
            env.append("LDMSD_PLUGIN_LIBPATH=/opt/ovis/lib64/ovis-ldms")
            env.append("ZAP_LIBPATH=/opt/ovis/lib64/ovis-lib")
            env.append("COMPONENT_ID={0}".format(self.component_id))
            env.append("LISTEN_PORT={0}".format(self.listen_port))
            env.append("SAMPLE_INTERVAL={0}".format(self.sample_interval))
            self.environment = env

        self.container = self.client.containers.run(
            self.image,
            detach=True,
            network=self.network_name,
            tty=True,
            stdin_open=True,
            user='root',
            name=self.cont_name,
            hostname=self.hostname,
            remove=True,
            volumes = {
                self.prefix : { 'bind' : '/opt/ovis', 'mode' : 'ro' },
                self.db_root : { 'bind' : '/db', 'mode' : 'rw' }
            },
            security_opt = [ 'seccomp=unconfined' ]
        )

    @property
    def name(self):
        return self.container.name

    @property
    def ip4_address(self):
        self.container.reload()
        return self.container.attrs['NetworkSettings']['Networks'][self.network_name]['IPAddress']

    def df(self):
        code, output = self.container.exec_run("/usr/bin/df")
        print output

    def ip_addr(self):
        code, output = self.container.exec_run("/usr/bin/ip addr")
        print output

    def exec_run(self, cmd):
        rc, output = self.container.exec_run(cmd)
        print output

    def kill(self):
        self.container.kill()
        self.container = None

    def test_running(self, cmd):
        rc, pid = self.container.exec_run('pgrep '+cmd)
        if rc:
            return False
        else:
            return True

    def ldms_ls(self, host='localhost'):
        cmd = 'ldms_ls -h {host} -x {xprt} -p {port} -a {auth}'\
        .format(
            xprt=self.listen_xprt,
            port=self.listen_port,
            config=self.config_file,
            host=host,
            log=self.log_file,
            log_level=self.log_level,
            auth=self.auth_name
        )
        rc, output = self.container.exec_run(cmd, environment=self.environment)
        if rc != 0:
            print("Error {0} running \n{1}".format(rc, cmd))
        print(output)

    def kill_ldmsd(self):
        rc, output = self.container.exec_run('pkill ldmsd')

    def start_ldmsd(self):
        if self.auth_name == 'munge':
            if not self.test_running('munged'):
                self.container.exec_run('/usr/sbin/munged')
            if not self.test_running('munged'):
                raise ValueError("Could not start munged but auth=munge")

        if not self.log_file:
            self.log_file = '/var/log/' + self.container.name + '.log'

        cmd = 'ldmsd -x {xprt}:{port} -H {host} -l {log} -v {log_level} -a {auth} -m 1m '
        if self.config_file:
            cmd += '-c {config}'

        cmd = cmd.format(
            xprt=self.listen_xprt,
            port=self.listen_port,
            config=self.config_file,
            host=self.hostname,
            log=self.log_file,
            log_level=self.log_level,
            auth=self.auth_name
        )

        rc, output = self.container.exec_run(cmd, environment=self.environment)
        if rc != 0:
            print("Error {0} running \n{1}".format(rc, cmd))
            print("Output:")
            print(output)
        else:
            if not self.test_running('ldmsd'):
                print("The ldmsd daemon failed to start. Check the log file {0}".\
                      format(self.log_file))
                rc, out = self.exec_run('cat ' + self.log_file)
                print(out)
            else:
                print("Daemon started")
        return rc

    def config_ldmsd(self):
        if not self.test_running('ldmsd'):
            print("There is no running ldmsd to configure")
            return

        cmd = 'echo status | ldmsd_controller --host {host}' \
              '--xprt {xprt} ' \
              '--port {port} ' \
              '--auth {auth} ' \
                  .format(
                      xprt=self.listen_xprt,
                      port=self.listen_port,
                      host=self.hostname)
        rc, output = self.container.exec_run(cmd, environment=self.environment)
        if rc != 0:
            print("Error {0} running \n{1}".format(rc, cmd))
            print("Output:")

        print(output)


class LDMSD_SVC(object):
    """Get or create service in docker swarm and prepare for `ldmsd`

    If the service already existed, all of the other object creation parameters
    are ignored and `LDMSD_SVC` will just wrap the existing service. Otherwise,
    they are used to create the docker service for `ldmsd`.

    Attributes
    ----------
    svc : docker.Service
        The corresponding docker service object.
    cont : docker.Container
        The corresponding docker container object.
    name : str
        The service name (from init()).
    xprt : str
        The transport type (from init).
    port : int
        The ldmsd port (from init).
    log_file : str
        The path to log file in the CONTAINER (from init).
    """
    def __init__(self, name, force_create=False, xprt="sock", port=10000,
                 config=None,
                 config_file = "/etc/ldmsd.conf",
                 auth = "none",
                 auth_opts = {},
                 log_level="INFO", log_file = "/var/log/ldmsd.log",
                 networks = [ "default_overlay" ],
                 ovis_prefix = "/opt/ovis",
                 extra_mounts = [],
                 extra_env = []):
        """
        Parameters
        ----------
        name : str
            The name of the service. This will also be used as a hostname
            for the docker container.
        force_create : bool
            If this flag is set to `True`, enforce the service creation. The
            initialization will fail if the service already existed. By default,
            this flag is `False`, i.e. `LDMSD_SVC` will wrap the existing
            service, or create a new service if it does not exist.
        xprt : str
            The LDMS transport type (default: "sock").
        port : int
            The port of the ldmsd (default: 10000).
        config : str
            The CONTENT of the configuration file. If not `None`, this will
            be written to a configuration file for the `ldmsd` to use. If the
            value is `None`, the config file is left untouched.
            (default: None).
        config_file : str
            The path (in CONTAINER) to config file. This could be useful if the
            config file has already been prepared, or testing out various
            configuration path. Please mind that if the `config` option is not
            `None`, the file specified by `config_file` will be overwritten.
            (default: "/etc/ldmsd.conf").
        auth : str
            The name of the authentication method (default: "none").
        auth_opts : dict( str:str )
            A dictionary of auth options, in which the key being authentication
            option name, and value being the corresponding value (e.g.
            `{ 'uid': '0' }` for `naive` authentication method).
            (default: None).
        log_level : str
            The `ldmsd` log level (default: "INFO").
        log_file : str
            The path in the CONTAINER to the `ldmsd` log file.
            (default: "/var/log/ldmsd.log")
        networks : list( str )
            A list of strings containing docker network names to attach this
            service to. The networks need to exist before creating LDMSD_SVC.
            (default: [ "default_overlay" ]).
        ovis_prefix : str
            The ovis installation prefix path in the HOST. This directory
            will be mounted as `/opt/ovis` in the container and proper
            environment variables will be setup.
        extra_mounts : list (str)
            A list of strings for extra mount points. The format of each entry
            is "SRC_PATH_IN_HOST:DEST_PATH_IN_CONTAINER:MODE", where "MODE" is
            either "ro" or "rw". An example of an entry is
            "/home/bob/data:/data:rw".
        extra_env : list (str)
            A list of strings for extra environment setup. The format of each
            entry is "NAME=VALUE".
        """
        self.client = docker.from_env()
        self.name = name
        self.xprt = xprt
        self.port = port
        self.config = config
        self.config_file = config_file
        self.auth = auth
        self.auth_opts = auth_opts.copy()
        self.log_level = log_level
        self.log_file = log_file
        self.networks = list(networks)
        self.ovis_prefix = ovis_prefix
        self.extra_mounts = list(extra_mounts)
        self.extra_env = list(extra_env)
        self.cont = None
        self.svc = None
        if not force_create:
            try:
                self.svc = self.client.services.get(name)
            except docker.errors.NotFound:
                pass
        if not self.svc:
            self._create_svc()

    def _create_svc(self):
        mounts = [ "{}:/opt/ovis:ro".format(self.ovis_prefix) ]
        mounts.extend(self.extra_mounts)
        env = [
            "PATH=/sbin:/bin:/usr/sbin:/usr/bin:/opt/ovis/bin:/opt/ovis/sbin",
            "LD_LIBRARY_PATH=/opt/ovis/lib:/opt/ovis/lib64",
            "ZAP_LIBPATH=/opt/ovis/lib/ovis-lib:/opt/ovis/lib64/ovis-lib",
            "LDMSD_PLUGIN_LIBPATH=/opt/ovis/lib/ovis-ldms:/opt/ovis/lib64/ovis-ldms",
        ]
        env.extend(self.extra_env)

        self.svc = self.client.services.create(
                image = "ovis-centos-build",
                command = "bash",
                open_stdin = True,
                env = env,
                hostname = self.name,
                name = self.name,
                # REMARK: hostname == (service)name for easy name resolve
                #         in docker overlay network
                networks = self.networks,
                user = "root",
                tty = True, # so that bash keep running ...
                mounts = mounts
            )
        return self.svc


    def is_svc_running(self):
        """Check if the service is running"""
        try:
            tasks = self.svc.tasks()
            return tasks[0]['Status']['State'] == 'running'
        except:
            return False

    def wait_svc_running(self, timeout=10):
        """Wait until the service is running

        Parameters
        ----------
        timeout : int
            The number of seconds for wait timeout.

        Returns
        -------
        True  : if the service is running before timeout.
        False : if the timeout occurred before the service became running.
        """
        t0 = time.time()
        while not self.is_svc_running():
            t1 = time.time()
            if t1-t0 > timeout:
                return False
            time.sleep(1)
        return True

    def kill_svc(self):
        """Kill the docker service (and container)"""
        self.svc.remove()

    def get_cont(self):
        if not self.cont:
            if not self.wait_svc_running():
                raise RuntimeError("Container main task not running")
            task = self.svc.tasks()[0]
            nid = task['NodeID']
            node = self.client.nodes.get(nid)
            addr = node.attrs['Description']['Hostname'] + ":2375"
            # client to remote dockerd
            ctl = docker.client.from_env(environment={'DOCKER_HOST': addr})
            task = self.svc.tasks()[0]
            cont_id = task['Status']['ContainerStatus']['ContainerID']
            self.cont = ctl.containers.get(cont_id)
        return self.cont

    def exec_run(self, cmd):
        """A wrapper of Container.exec_run()"""
        return self.get_cont().exec_run(cmd)

    def start_ldmsd(self):
        """A routine to start `ldmsd` in the container"""
        if self.check_ldmsd():
            return # already running
        if self.config:
            self.write_file(self.config_file, self.config)
        config = "" if not self.config else "-c {}".format(self.config_file)
        cmd = "ldmsd {config} -x {xprt}:{port}" \
               " -v {level} -l {log}".format(
                    config = config,
                    xprt = self.xprt,
                    port = self.port,
                    level = self.log_level,
                    log = self.log_file,
               )
        self.exec_run(cmd)

    def check_ldmsd(self):
        rc, out = self.exec_run("pgrep -c ldmsd")
        return rc == 0

    def kill_ldmsd(self):
        self.exec_run("pkill ldmsd")

    def write_file(self, path, content):
        """Write `content` to `path` in the container"""
        cmd = "/bin/bash -c 'cat -  >{}'".format(path)
        erun = self.get_cont().exec_run(cmd, stdin=True, socket=True)
        sock = erun.output
        sock.send(content)
        sock.close()

    def read_file(self, path):
        """Read file specified by `path` from the container"""
        cmd = "cat {}".format(path)
        erun = self.get_cont().exec_run(cmd)
        return erun.output

class DockerClusterContainer(Container):
    """A Container wrapper for containers in DockerCluster"""
    def __init__(self, obj, cluster):
        self.cluster = cluster
        super(DockerClusterContainer, self).__init__(obj)

    @property
    def aliases(self):
        """The list of aliases of the container hostname"""
        return self.cluster.node_aliases.get(self.hostname, [])

class DockerCluster(object):
    """Docker Cluster

    A utility to create a virtual cluster with Docker Swarm Network and Docker
    Containers. Instead of relying on Docker Service to orchestrate docker
    containers, which at the time this is written cannot add needed capabilities
    to the containers, this class manages the created containers itself.  The
    virtual cluster has exactly one overlay network. The hostnames of the nodes
    inside the virtual cluster is "node-{slot}", where the `{slot}` is the task
    slot number of the container.  `/etc/hosts` of each container is also
    modified so that programs inside each container can use the hostname to
    commnunicate to each other. Hostname aliases can also be set at `create()`.

    DockerCuster.create() creates the virtual cluster (as well as docker
    Service). DockerCluster.get() obtains the existing virtual cluster,
    and can optionally create a new virtual cluster if `create = True`. See
    `create()` and `get()` class methods for more details.
    """
    def __init__(self, obj):
        """Do not direcly call this, use .create() or .get() instead"""
        # obj must be a docker network with DockerCluster label
        if type(obj) != docker.models.networks.Network:
            raise TypeError("`obj` must be a docker Network")
        lbl = obj.attrs.get("Labels")
        if "DockerCluster" not in lbl:
            msg = "The network is not created by DockerCluster. " \
                  "Please remove or disconnect docker containers " \
                  "using the network first, then remove the network " \
                  "by `docker network rm {}`. " \
                  .format(obj.name)
            raise TypeError(msg)
        self.obj = obj
        self.net = Network(obj)
        self.cont_dict = None

    @classmethod
    def create(cls, name, image = "centos:7", nodes = 8,
                    mounts = [], env = [], labels = {},
                    node_aliases = {},
                    cap_add = [],
                    cap_drop = []):
        """Create virtual cluster with docker network and service

        If the docker network existed, this will failed. The hostname of each
        container in the virtual cluster is formatted as "node-{slot}", where
        '{slot}' is the docker task slot number for the container. Applications
        can set node aliases with `node_aliases` parameter.

        Example
        -------
        >>> cluster = DockerCluster.create(
                            name = "vc", nodes = 16,
                            mounts = [ "/home/bob/ovis:/opt/ovis:ro" ],
                            env = { "CLUSTERNAME" : "vc" },
                            node_aliases = { "node-1" : [ "head" ] },
                            cap_add = [ "SYS_PTRACE" ]
                        )

        Parameters
        ----------
        name : str
            The name of the cluster (also the name of the network).
        image : str
            The name of the image to use.
        nodes : int
            The number of nodes in the virtual cluster.
        mounts : list(str)
            A list of `str` of mount points with format "SRC:DEST:MODE",
            in which "SRC" being the source path (on the docker host),
            "DEST" being the mount destination path (in the container),
            and "MODE" being "ro" or "rw" (read-only or read-write).
        env : list(str) or dict(str:str)
            A list of "NAME=VALUE", or a dictionary of { NAME: VALUE } of
            environment variables.
        labels : dict(str:str)
            A dictionary of { LABEL : VALUE } for user-defined labels for the
            docker service.
        node_aliases : dict(str:list(str))
            A dictionary of { NAME : list( str ) } containing a list of aliases
            of the nodes.
        cap_add : list(str)
            A list of capabilities (e.g. 'SYS_PTRACE') to add to containers
            created by the virtual cluster.
        cap_drop : list(str)
            A list of capabilities to drop.

        Returns
        -------
        DockerCluster
            The virtual cluster handle.
        """
        lbl = dict(labels)
        cfg = dict(name = name,
                   image = image,
                   num_nodes = nodes,
                   env = env,
                   mounts = mounts,
                   cap_add = cap_add,
                   cap_drop = cap_drop,
               )
        lbl.update({
                "node_aliases": json.dumps(node_aliases),
                "DockerCluster" : json.dumps(cfg),
              })
        clients = get_docker_clients()

        # allocation table by client: [current_load, alloc, client]
        tbl = [ [ cl.info()["ContainersRunning"], 0, cl ] for cl in clients ]
        # and start calculating allocation for each client
        # sort clients by load
        tbl.sort( key = lambda x: x[0] )
        max_load = tbl[-1][0] # last entry
        _n = nodes # number of containers needed
        cn = len(tbl)
        # make the load equal by filling the diff from max_load first
        for ent in tbl:
            _a = max_load - ent[0]
            if not _a:
                break # reaching max_load, no need to continue
            _a = _a if _a < _n else _n
            ent[1] = _a
            _n -= _a
            if not _n:
                break # all containers allocated, no need to continue
        # evenly distribute remaining load after equalization
        _a = _n // cn
        for ent in tbl:
            ent[1] += _a
        # the remainders
        _n %= cn
        for i in range(0, _n):
            tbl[i][1] += 1

        # making parameters for containers
        _slot = 1
        cont_build = [] # store (client, cont_param)
        lbl_cont_build = [] # store (client_name, cont_param) for reference
        volumes = { src: {"bind": dst, "mode": mo } \
                    for src, dst, mo in map(lambda x:x.split(':'), mounts)
                }
        for load, n, cl in tbl:
            # allocate `n` container using `client`
            cl_info = cl.info()
            cl_name = cl_info["Name"]
            for i in range(0, n):
                hostname = "node-{}".format(_slot)
                cont_name = "{}-{}".format(name, _slot)
                cont_param = dict(
                        image = image,
                        name = cont_name,
                        command = "/bin/bash",
                        tty = True,
                        detach = True,
                        environment = env,
                        volumes = volumes,
                        cap_add = cap_add,
                        cap_drop = cap_drop,
                        network = name,
                        hostname = hostname,
                    )
                lbl_cont_build.append( (cl_name, cont_param) )
                cont_build.append( (cl, cont_param) )
                _slot += 1
        # memorize cont_build as a part of label
        dc = docker.from_env()
        lbl["cont_build"] = json.dumps(lbl_cont_build)
        net = Network.create(name = name, driver = "overlay",
                             attachable = True, scope = "swarm",
                             labels = lbl)
        # then, create the actual containers
        for cl, params in cont_build:
            cl.containers.run(**params)
        cluster = DockerCluster(net.obj)
        cluster.update_etc_hosts(node_aliases = node_aliases)
        return cluster

    @classmethod
    def get(cls, name, create = False, **kwargs):
        """Finds (or optionally creates) and returns the DockerCluster

        This function finds the DockerCluster by `name`. If the service
        is found, everything else is ignored. If the service not found and
        `create` is `True`, DockerCluster.create() is called with
        given `kwargs`. Otherwise, `docker.errors.NotFound` is raised.

        Parameters
        ----------
        name : str
            The name of the virtual cluster.
        create : bool
            If `True`, the function creates the service if it is not found.
            Otherwise, no new service is created (default: False).
        **kwargs
            Parameters for DockerCluster.create()
        """
        try:
            dc = docker.from_env()
            net = dc.networks.get(name)
            return cls(net)
        except docker.errors.NotFound:
            if not create:
                raise
            return cls.create(name = name, **kwargs)

    def is_running(self):
        """Check if the service (all ) is running"""
        for cont in self.containers:
            if not cont.is_running():
                return False
        return True

    def wait_running(self, timeout=10):
        """Wait for all containers to run"""
        t0 = time.time()
        while not self.is_running():
            t1 = time.time()
            if t1-t0 > timeout:
                return False
            time.sleep(1)
        return True

    @cached_property
    def containers(self):
        """A list of containers wrapped by DockerClusterContainer"""
        return self.get_containers()

    def get_containers(self, timeout = 10):
        """Return a list of docker Containers of the virtual cluster"""
        cont_list = [ DockerClusterContainer(c.obj, self) \
                            for c in self.net.containers ]
        cont_list.sort(key = lambda x: LooseVersion(x.name))
        return cont_list

    def get_container(self, name):
        """Get container by name"""
        if not self.cont_dict:
            cont_list = self.containers
            cont_dict = dict()
            for cont in cont_list:
                k = cont.attrs['Config']['Hostname']
                cont_dict[k] = cont
            for k, v in self.node_aliases.iteritems():
                cont = cont_dict[k]
                if type(v) == str:
                    v = [ v ]
                for n in v:
                    cont_dict[n] = cont
            self.cont_dict = cont_dict
        return self.cont_dict.get(name)

    @cached_property
    def node_aliases(self):
        """dict(hostname:list) - node aliases by hostname"""
        txt = self.net.obj.attrs["Labels"]["node_aliases"]
        return json.loads(txt)

    def remove(self):
        """Remove the docker service and its network"""
        for cont in self.containers:
            try:
                cont.remove(force = True)
            except:
                pass
        self.net.remove()

    @property
    def labels(self):
        """Labels"""
        return self.net.obj.attrs["Labels"]

    def build_etc_hosts(self, node_aliases = {}):
        """Returns the generated `/etc/hosts` content"""
        if not node_aliases:
            node_aliases = self.node_aliases
        sio = StringIO()
        sio.write("127.0.0.1 localhost\n")
        for cont in self.containers:
            name = cont.hostname
            ip_addr = cont.ip_addr
            sio.write("{0.ip_addr} {0.hostname}".format(cont))
            networks = cont.attrs["NetworkSettings"]["Networks"]
            alist = node_aliases.get(name, [])
            if type(alist) == str:
                alist = [ alist ]
            for a in alist:
                sio.write(" {}".format(a))
            sio.write("\n")
        return sio.getvalue()

    def update_etc_hosts(self, node_aliases = {}):
        """Update entries in /etc/hosts"""
        etc_hosts = self.build_etc_hosts(node_aliases = node_aliases)
        conts = self.get_containers()
        for cont in conts:
            cont.write_file("/etc/hosts", etc_hosts)


class DockerClusterService(Service):
    """Docker Cluster Service

    A utility to create a virtual cluster with (wrapped) Docker service. The
    virtual cluster has exactly one overlay network. The hostnames of the nodes
    inside the virtual cluster is "node-{slot}", where the `{slot}` is the task
    slot number of the container.  `/etc/hosts` of each container is also
    modified so that programs inside each container can use the hostname to
    commnunicate to each other. Hostname aliases can also be set at `create()`.

    DockerCusterService.create() creates the virtual cluster (as well as docker
    Service). DockerClusterService.get() obtains the existing virtual cluster,
    and can optionally create a new virtual cluster if `create = True`. See
    `create()` and `get()` class methods for more details.
    """
    def __init__(self, obj):
        """Do not direcly call this, use .create() or .get() instead"""
        super(DockerClusterService, self).__init__(obj)
        lbl = obj.attrs["Spec"]["Labels"]
        if "DockerClusterService" not in lbl:
            raise TypeError("Service {} is not created by " \
                            "DockerClusterService".format(obj.name))
        self.cont_dict = None

    @classmethod
    def create(cls, name, image = "centos:7", nodes = 8,
                    network = None, mounts = [], env = [], labels = {},
                    node_aliases = {},
                    cap_add = [],
                    cap_drop = []):
        """Create virtual cluster with docker network and service

        If the docker network or docker service existed, this will failed. The
        hostname of each container in the virtual cluster is formatted as
        "node-{slot}", where '{slot}' is the docker task slot number for the
        container. Applications can set node aliases with `node_aliases`
        parameter.

        Example
        -------
            cluster = DockerClusterService.create(
                            name = "vc", nodes = 16,
                            mounts = [ "/home/bob/ovis:/opt/ovis:ro" ],
                            env = { "CLUSTERNAME" : "vc" },
                            node_aliases = { "node-1" : [ "head" ] }
                        )

        Parameters
        ----------
        name : str
            The name of the cluster.
        image : str
            The name of the image to use.
        nodes : int
            The number of nodes in the virtual cluster.
        network : str
            The name of the network. By default, it is the same as `name`
            (cluster name).
        mounts : list(str)
            A list of `str` of mount points with format "SRC:DEST:MODE",
            in which "SRC" being the source path (on the docker host),
            "DEST" being the mount destination path (in the container),
            and "MODE" being "ro" or "rw" (read-only or read-write).
        env : list(str) or dict(str:str)
            A list of "NAME=VALUE", or a dictionary of { NAME: VALUE } of
            environment variables.
        labels : dict(str:str)
            A dictionary of { LABEL : VALUE } for user-defined labels for the
            docker service.
        node_aliases : dict(str:list(str))
            A dictionary of { NAME : list( str ) } containing a list of aliases
            of the nodes.
        cap_add : list(str)
            IGNORED - Slurm Service does not (yet) take cap_add
        cap_drop : list(str)
            IGNORED - Slurm Service does not (yet) take cap_drop

        Returns
        -------
        DockerClusterService
            The virtual cluster handle.
        """
        d = docker.from_env()
        if not network:
            network = name
        net = Network.create(name = network, driver = "overlay",
                                attachable = True, scope = "swarm")
        try:
            mode = docker.models.services.ServiceMode("replicated", nodes)
            lbl = {
                    "node_aliases": json.dumps(node_aliases),
                    "DockerClusterService" : "DockerClusterService",
                  }
            lbl.update(labels)
            svc = d.services.create(image, command = "bash", name = name,
                                    tty = True, networks = [ network ],
                                    mounts = mounts, env = env,
                                    labels = lbl,
                                    mode = mode,
                                    hostname = "node-{{.Task.Slot}}")
            svc = DockerClusterService(svc)
            svc.update_etc_hosts(node_aliases = node_aliases)
            return svc
        except:
            raise

    @classmethod
    def get(cls, name, create = False, **kwargs):
        """Finds (or optionally creates) and returns the DockerClusterService

        This function finds the DockerClusterService by `name`. If the service
        is found, everything else is ignored. If the service not found and
        `create` is `True`, DockerClusterService.create() is called with
        given `kwargs`. Otherwise, `docker.errors.NotFound` is raised.

        Parameters
        ----------
        name : str
            The name of the virtual cluster.
        create : bool
            If `True`, the function creates the service if it is not found.
            Otherwise, no new service is created (default: False).
        **kwargs
            Parameters for DockerClusterService.create()
        """
        d = docker.from_env()
        try:
            svc = d.services.get(name)
            return cls(svc)
        except docker.errors.NotFound:
            if not create:
                raise
            return cls.create(name = name, **kwargs)

    def is_svc_running(self):
        """Check if the service (all tasks) is running"""
        return self.svc.tasks_running()

    def wait_svc_running(self, timeout=10):
        """Wait for the service to run, or timeout"""
        return self.svc.wait_tasks_running(timeout = timeout)

    @cached_property
    def containers(self):
        """A list of containers wrapped by DockerClusterContainer"""
        conts = self.get_containers(timeout = 60)
        return [ DockerClusterContainer(c.obj, self) for c in conts ]

    def get_container(self, name):
        """Get container by name"""
        if not self.cont_dict:
            cont_list = self.containers
            cont_dict = dict()
            for cont in cont_list:
                k = cont.attrs['Config']['Hostname']
                cont_dict[k] = cont
            for k, v in self.node_aliases.iteritems():
                cont = cont_dict[k]
                if type(v) == str:
                    v = [ v ]
                for n in v:
                    cont_dict[n] = cont
            self.cont_dict = cont_dict
        return self.cont_dict.get(name)

    @cached_property
    def node_aliases(self):
        """dict(hostname:list) - node aliases by hostname"""
        txt = self.attrs["Spec"]["Labels"]["node_aliases"]
        return json.loads(txt)

    def remove(self):
        """Remove the docker service and its network"""
        nets = self.net
        self.obj.remove()
        for n in nets:
            n.remove()


### -- LDMSDCluster Spec Example -- ###
LDMSDCluster_spec_example = {
    "name" : "Dir_Info_Test",
    "description" : "Test the sampler-info provided in the directory data",
    "type" : "FVT",
    "define" : [
        {
            "name" : "sampler-daemon",
            "type" : "sampler",
            "listen_port" : 10000,
            "listen_xprt" : "sock",
            "listen_auth" : "munge",
            "env" : [
                "INTERVAL=1000000",
                "OFFSET=0"
            ],
            "samplers" : [
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
            "env" : [
                "COMPONENT_ID=10001",
                "HOSTNAME=%host%"
            ]
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
            "listen_auth" : "munge",
            "env" : [
                "HOSTNAME=%host%"
            ],
            "config" : [
                "prdcr_add name=sampler-1 host=sampler-1 port=10000 auth=munge interval=20000000",
                "prdcr_add name=sampler-2 host=sampler-1 port=10000 auth=munge interval=20000000",
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
            "listen_auth" : "munge",
            "env" : [
                "HOSTNAME=%host%"
            ],
            "config" : [
                "prdcr_add name=agg-1 host=agg-1 port=20000 auth=munge interval=20000000",
                "prdcr_start_regex regex=.*",
                "updtr_add name=all interval=1000000 offset=0",
                "updtr_prdcr_add name=all regex=.*",
                "updtr_start name=all"
            ]
        }
    ],

    # NOTE: The following is Narate's extension

    # (Optional) This is so that we can try on various OS.
    # Default: "ovis-centos-build"
    "image": "ovis-centos-build",

    # (Optional) ovis_prefix is used for mounting `/opt/ovis` and setup ovis ENV
    # Default: "/opt/ovis"
    "ovis_prefix": "/home/narate/opt/ovis",

    # (Optional) extra arbitrary mounts, e.g. store data
    # Default: []
    "mounts": [
        "/home/narate/store:/store:rw",
    ],

    # (Optional) environment applied cluster-wide
    # Default: []
    "env": [ ]
}

class LDMSDContainer(DockerClusterContainer):
    """Container wrapper for a container being a part of LDMSDCluster

    LDMSDContainer extends DockerClusterContainer -- adding ldmsd-specific
    routines and properties.

    Application does not normally direcly create LDMSDContainer. LDMSDContainer
    should be obtained by calling `get_container()` or accessing `containers` of
    LDMSDCluster.
    """
    def __init__(self, obj, svc):
        if not issubclass(type(svc), LDMSDCluster):
            raise TypeError("`svc` must be a subclass of LDMSDCluster")
        self.svc = svc
        super(LDMSDContainer, self).__init__(obj, svc)

    def pgrep(self, *args):
        """Return (rc, output) of `pgrep *args`"""
        return self.exec_run("pgrep " + " ".join(args))

    def check_ldmsd(self):
        """Check if ldmsd is running"""
        rc, out = self.exec_run("pgrep -c ldmsd")
        return rc == 0

    def start_ldmsd(self, spec_override = {}):
        """Start ldmsd in the container"""
        spec = self.ldmsd_spec.copy()
        spec.update(spec_override)
        cfg = self.get_ldmsd_config(spec)
        if self.check_ldmsd():
            raise RuntimeError("ldmsd is already running")
        self.write_file(spec["config_file"], cfg)
        cmd = self.get_ldmsd_cmd(spec)
        env = { k: re.sub("%(\w+)%", lambda m: spec[m.group(1)], v) \
                    for k, v in env_dict(spec["env"]).iteritems() }

        self.exec_run(cmd, environment = env)

    def kill_ldmsd(self):
        """Kill ldmsd in the container"""
        self.exec_run("pkill ldmsd")

    @cached_property
    def ldmsd_spec(self):
        """Get the spec for this ldmsd (from the associated service)"""
        svc_spec = self.svc.spec
        if not self.aliases:
            return dict() # return empty dict, if not having ldmsd role
        k = self.aliases[0] # alias is my role
        daemon = next( (d for d in svc_spec["daemons"] if d["host"] == k), None )
        if not daemon:
            return {} # empty dict
        dspec = daemon.copy()

        # apply cluster-level env
        w = env_dict(dspec.get("env", []))
        v = env_dict(svc_spec.get("env", []))
        v.update(w) # dspec precede svc_spec
        dspec["env"] = v

        k = dspec.pop("asset", None)
        if k:
            # apply asset
            asset = next( (a for a in svc_spec["define"] if a["name"] == k), {} )
            omitted = ["name"]
            for k,v in asset.iteritems():
                if k in omitted:
                    continue
                w = dspec.get(k)
                if k == "env":
                    u = env_dict(v)
                    u.update(w)
                    dspec[k] = u
                    continue
                if w == None:
                    dspec[k] = v
                elif type(w) == list:
                    dspec[k] = v + w
                elif type(w) == dict:
                    u = v.copy()
                    u.update(w) # attr in dspec precedes attr in asset
                    dspec[k] = u
                # else, do nothing, attr in dspec precedes attr in asset

        dspec.setdefault("log_file", "/var/log/ldmsd.log")
        dspec.setdefault("log_level", "INFO")
        dspec.setdefault("listen_auth", "none")
        dspec.setdefault("config_file", "/etc/ldmsd.conf")

        return dspec

    @cached_property
    def ldmsd_config(self):
        """Get ldmsd config `str` of this container"""
        return self.get_ldmsd_config(self.ldmsd_spec)

    def get_ldmsd_config(self, spec):
        """Generate ldmsd config `str` from given spec"""
        cfg = spec.get("config")
        if cfg:
            return '\n'.join(cfg)
        # samplers
        sio = StringIO()
        for samp in spec.get("samplers", []):
            plugin = samp["plugin"]
            samp_cfg = ("""
                load name={plugin}
                config name={plugin} component_id=${{COMPONENT_ID}} {config} \
            """ + ( "" if not samp.get("start") else \
            """
                start name={plugin} interval=${{INTERVAL}} offset=${{OFFSET}}
            """)
            ).format(plugin = plugin, config = " ".join(samp["config"]))

            # replaces all %VAR%
            samp_cfg = re.sub(r'%(\w+)%', lambda m: samp[m.group(1)], samp_cfg)
            sio.write(samp_cfg)
        return sio.getvalue()

    @cached_property
    def ldmsd_cmd(self):
        """Command to run ldmsd"""
        return self.get_ldmsd_cmd(self.ldmsd_spec)

    def get_ldmsd_cmd(self, spec):
        """Get ldmsd command line according to spec"""
        spec = dict(spec)
        cmd = "ldmsd -x {listen_xprt}:{listen_port} -a {listen_auth}" \
              "      -c {config_file} -l {log_file} -v {log_level}" \
              .format(**spec)
        return cmd

    def ldms_ls(self, *args):
        """Executes `ldms_ls` with *args, and returns (rc, output)"""
        cmd = "ldms_ls " + (" ".join(args))
        return self.exec_run(cmd)

    def start_munged(self):
        """Start Munge Daemon"""
        rc, out = self.pgrep("-c munged")
        if rc == 0: # already running
            return
        rc, out = self.exec_run("munged", user="munge")
        if rc:
            raise RuntimeError("munged failed to start, rc: {}, output: {}" \
                                                    .format(rc, out))

    def kill_munged(self):
        """Kill munged"""
        self.exec_run("pkill munged")

    def prep_slurm_conf(self):
        """Prepare slurm configurations"""
        self.write_file("/etc/slurm/cgroup.conf", "CgroupAutomount=yes")
        self.write_file("/etc/slurm/slurm.conf", self.svc.slurm_conf)

    def start_slurm(self):
        """Start slurmd in all sampler nodes, and slurmctld on svc node"""
        # determine our role
        prog = None
        if self.ldmsd_spec.get("type") == "sampler":
            # sampler node, run slurmd
            prog = "slurmd"
        if self.hostname == self.svc.containers[-1].hostname:
            # service node, run slurmctld
            prog = "slurmctld"
        if not prog: # no slurm role for this node, do nothing
            return
        rc, out = self.pgrep("-c " + prog)
        if rc == 0: # already running
            return
        self.prep_slurm_conf()
        rc, out = self.exec_run(prog)
        if rc:
            raise RuntimeError("{} failed, rc: {}, output: {}" \
                               .format(prog, rc, out))

    def kill_slurm(self):
        """Kill slurmd and slurmctld"""
        self.exec_run("pkill slurmd slurmctld")

    def start_sshd(self):
        """Start sshd"""
        rc, out = self.pgrep("-c -x sshd")
        if rc == 0: # already running
            return
        rc, out = self.exec_run("/usr/sbin/sshd")
        if rc:
            raise RuntimeError("sshd failed, rc: {}, output: {}" \
                               .format(rc, out))

BaseCluster = DockerCluster if not use_docker_service else DockerClusterService

class LDMSDCluster(BaseCluster):
    """LDMSD Cluster - a virtual cluster for LDMSD

    LDMSDCluster extends DockerCluster. Similarly to
    DockerCluster, `create()` class method creates the virtual cluster,
    and `get()` class method obtains the existing virtual cluster (and
    optionally creates it if `create=True` is given), but LDMSDCluster receives
    `spec` instead of long list of keyword arguments (see `get()` and
    `create()`). The containers in LDMSDCluster is wrapped by LDMSDContainer to
    provide ldmsd-specific utilities.
    """
    @classmethod
    def create(cls, spec):
        """Create a virtual cluster for ldmsd with given `spec`

        `spec` is a dictionary conforming to the following key definitions:
            {
                "name" : "NAME_OF_THE_VIRTUAL_CLUSTER",
                "description" : "DESCRIPTION OF THE TEST",
                "type" : "TYPE OF THE TEST",
                "define" : [ # a list of daemon templates
                    {
                        "name" : "TEMPLATE_NAME",
                        "type" : "TEMPLATE_TYPE", # only "sampler" for now
                        "listen_port" : LDMSD_LISTENING_PORT, # int
                        "listen_xprt" : "TRANSPORT_TYPE", # sock, rdma, or ugni
                        "listen_auth" : "AUTH_TYPE", # none, ovis, or munge
                        "env" : [ # a list of environment variables
                            "INTERVAL=1000000",
                            "OFFSET=0",
                        ],
                        "samplers" : [ # list of sampler plugins to load/config
                            {
                                "plugin" : "PLUGIN_NAME",
                                "config" : [ # a list of plugin config args
                                    "instance=${HOSTNAME}/%plugin%",
                                                       # '%plugin%' is replaced
                                                       # with PLUGIN_NAME
                                    "producer=${HOSTNAME}",
                                ]
                            },
                        ]
                    }
                ],
                "daemons" : [ # a list of LDMS Daemons (1 per container)
                    {
                        # defining daemon using a template, all attributes of
                        # the template are applied to the daemon, except for
                        # `env` being a concatenation of template["env"] +
                        # daemon["env"] (hence daemon env overrides that of the
                        # template for the env variables appearing in both
                        # places).
                        "host" : "HOST_NAME",
                        "asset" : "TEMPLATE_NAME",
                                  # referring to the template in `define`
                        "env" : [ # additional env
                            "COMPONENT_ID=10001",
                            "HOSTNAME=%host%", # %host% is replaced with
                                               # HOST_NAME value from above.
                        ]
                    },
                    # or
                    {
                        # defining a daemon plainly (not using template).
                        "host" : "HOST_NAME",
                        "listen_port" : LDMSD_LISTENING_PORT, # int
                        "listen_xprt" : "TRANSPORT_TYPE", # sock, rdma, or ugni
                        "listen_auth" : "AUTH_TYPE", # none, ovis, or munge
                        "env" : [ # list of env
                            "HOSTNAME=%host%"
                        ],
                        "config" : [ # list of ldmsd configuration commands
                            "load name=meminfo",
                            ...
                        ]
                    },
                ],
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
                ]
            }
        """
        kwargs = cls.spec_to_kwargs(spec)
        wrap = super(LDMSDCluster, cls).create(**kwargs)
        lc = LDMSDCluster(wrap.obj)
        lc.make_ovis_env()
        return lc

    @classmethod
    def get(cls, name, create = False, **kwargs):
        """Obtain an existing ldmsd virtual cluster (or create if `create=True`)"""
        d = docker.from_env()
        try:
            wrap = super(LDMSDCluster, cls).get(name)
            return LDMSDCluster(wrap.obj)
        except docker.errors.NotFound:
            if not create:
                raise
            return LDMSDCluster.create(**kwargs)

    @classmethod
    def spec_to_kwargs(cls, spec):
        """Convert `spec` to kwargs for DockerCluster.create()"""
        name = spec["name"]
        daemons = spec["daemons"]
        prefix = spec.get("ovis_prefix", "/opt/ovis")
        mounts = ["{}:/opt/ovis:ro".format(prefix)] + spec.get("mounts", [])
        cap_add = spec.get("cap_add", [])
        cap_drop = spec.get("cap_drop", [])
        # assign daemons to containers using node_aliases
        node_aliases = {}
        idx = 1
        for daemon in daemons:
            c_host = "node-{}".format(idx)
            node_aliases[c_host] = [ daemon["host"] ]
            idx += 1
        # starts with OVIS env
        env = {
                "PATH" : ":".join([
                        "/opt/ovis/bin",
                        "/opt/ovis/sbin",
                        "/usr/local/bin",
                        "/usr/local/sbin",
                        "/usr/bin",
                        "/usr/sbin",
                        "/bin",
                        "/sbin",
                    ]),
                "LD_LIBRARY_PATH" : "/opt/ovis/lib:/opt/ovis/lib64",
                "ZAP_LIBPATH" : "/opt/ovis/lib/ovis-lib:/opt/ovis/lib64/ovis-lib",
                "LDMSD_PLUGIN_LIBPATH" : "/opt/ovis/lib/ovis-ldms:/opt/ovis/lib64/ovis-ldms",
            }
        env.update(env_dict(spec.get("env", {})))
        kwargs = dict(
                    name = name,
                    image = spec.get("image", "ovis-centos-build"),
                    mounts = mounts,
                    nodes = len(daemons) + 1,
                    env = env,
                    labels = { "LDMSDCluster.spec": json.dumps(spec) },
                    node_aliases = node_aliases,
                    cap_add = cap_add,
                    cap_drop = cap_drop,
                 )
        return kwargs

    @cached_property
    def spec(self):
        return json.loads(self.labels["LDMSDCluster.spec"])

    @property
    def containers(self):
        s = super(LDMSDCluster, self)
        return [ LDMSDContainer(c.obj, self) for c in s.containers ]

    def get_container(self, name):
        cont = super(LDMSDCluster, self).get_container(name)
        if cont:
            cont = LDMSDContainer(cont.obj, self)
        return cont

    def start_ldmsd(self):
        """Start ldmsd in each node in the cluster"""
        for d in self.spec["daemons"]:
            cont = self.get_container(d["host"])
            if not cont.check_ldmsd():
                cont.start_ldmsd()

    def check_ldmsd(self):
        """Returns a dict(hostname:bool) indicating if each ldmsd is running"""
        return [ self.get_container(d["host"]).check_ldmsd() \
                                            for d in self.spec["daemons"] ]

    @property
    def slurm_conf(self):
        """Content for `/etc/slurm/slurm.conf`"""
        daemons = self.spec["daemons"]
        svc_cont = self.containers[-1]
        svc_node = svc_cont.hostname
        samplers = ",".join( cont.hostname \
                                for cont in self.containers \
                                if cont.ldmsd_spec.get("type") == "sampler" )
        slurmconf = """
            SlurmctldHost={svc_node}
            MpiDefault=none
            ProctrackType=proctrack/linuxproc
            ReturnToService=1
            SlurmctldPidFile=/var/run/slurmctld.pid
            SlurmctldPort=6817
            SlurmdPidFile=/var/run/slurmd.pid
            SlurmdPort=6818
            SlurmdSpoolDir=/var/spool/slurmd
            SlurmUser=root
            StateSaveLocation=/var/spool
            SwitchType=switch/none
            TaskPlugin=task/affinity
            TaskPluginParam=Sched
            InactiveLimit=0
            KillWait=30
            MinJobAge=300
            SlurmctldTimeout=120
            SlurmdTimeout=300
            Waittime=0
            FastSchedule=1
            SchedulerType=sched/builtin
            SelectType=select/cons_res
            SelectTypeParameters=CR_Core
            AccountingStorageType=accounting_storage/none
            AccountingStoreJobComment=YES
            ClusterName=cluster
            JobCompType=jobcomp/none
            JobAcctGatherFrequency=30
            JobAcctGatherType=jobacct_gather/none
            SlurmctldDebug=info
            SlurmctldLogFile=/var/log/slurmctld.log
            SlurmdDebug=info
            SlurmdLogFile=/var/log/slurmd.log
            NodeName={samplers} CPUs=1 State=UNKNOWN
            PartitionName=debug Nodes={samplers} Default=YES MaxTime=INFINITE State=UP
        """.format( svc_node = svc_node, samplers = samplers )
        return slurmconf

    def start_munged(self):
        """Start Munge Daemon"""
        for cont in self.containers:
            cont.start_munged()

    def start_slurm(self):
        """Start slurmd in all sampler nodes, and slurmctld on svc node"""
        self.start_munged()
        for cont in self.containers:
            cont.start_slurm()

    def start_sshd(self):
        """Start sshd in all containers"""
        for cont in self.containers:
            cont.start_sshd()

    def ssh_keyscan(self):
        """Report ssh-keyscan result of all nodes in the cluster"""
        hosts = reduce(lambda x,y: x+y,
                         ([c.hostname, c.ip_addr] + c.aliases \
                                             for c in self.containers) )
        cmd = "ssh-keyscan " + " ".join(hosts)
        cont = self.containers[-1]
        rc, out = cont.exec_run(cmd, stderr=False)
        return out

    def make_known_hosts(self):
        """Make `/root/.ssh/known_hosts` in all nodes"""
        ks = self.ssh_keyscan()
        for cont in self.containers:
            cont.exec_run("mkdir -p /root/.ssh")
            cont.write_file("/root/.ssh/known_hosts", ks)

    def make_ssh_id(self):
        """Make `/root/.ssh/id_rsa` and authorized_keys"""
        cont = self.containers[-1]
        cont.exec_run("mkdir -p /root/.ssh/")
        cont.exec_run("rm -f id_rsa id_rsa.pub", workdir="/root/.ssh/")
        cont.exec_run("ssh-keygen -q -N '' -f /root/.ssh/id_rsa")
        D.id_rsa = id_rsa = cont.read_file("/root/.ssh/id_rsa")
        D.id_rsa_pub = id_rsa_pub = cont.read_file("/root/.ssh/id_rsa.pub")
        for cont in self.containers:
            cont.exec_run("mkdir -p /root/.ssh/")
            cont.write_file("/root/.ssh/id_rsa", id_rsa)
            cont.exec_run("chmod 600 /root/.ssh/id_rsa")
            cont.write_file("/root/.ssh/id_rsa.pub", id_rsa_pub)
            cont.write_file("/root/.ssh/authorized_keys", id_rsa_pub)

    def exec_run(self, *args, **kwargs):
        """A pass-through to last_cont.exec_run()

        The `last_cont` is the last container in the virtual cluster, which does
        NOT have any `ldmsd` role (i.e. no `ldmsd` running on it). If
        `start_slurm()` was called, the `last_cont` is also the slurm head node
        where slurmctld is running.
        """
        cont = self.containers[-1]
        return cont.exec_run(*args, **kwargs)

    def sbatch(self, script_path):
        """Submits slurm batch job, and returns job_id on success

        Parameters
        ----------
        script_path : str
            The path to the script file in the service node (the last
            container).

        Returns
        -------
        jobid : int
            The JOB ID if the submission is a success.

        Raises
        ------
        RuntimeError
            If the submission failed.
        """
        _base = os.path.basename(script_path)
        _dir = os.path.dirname(script_path)
        rc, out = self.exec_run("sbatch " + _base, workdir = _dir)
        if rc:
            raise RuntimeError("sbatch error, rc: {}, output: {}"\
                                    .format(rc, out))
        m = re.search(r"Submitted batch job (\d+)", out)
        job_id = int(m.group(1))
        return job_id

    def squeue(self, jobid = None):
        """Execute `squeue` and parse the results

        Parameters
        ----------
        jobid : int
            The optional jobid. If not specified, the status of all jobs are
            queried.

        Returns
        -------
        [ { "KEY" : "VALUE" }, ... ]
            A list of dictionaries describing job statuses. Examples of keys are
            "JOBID", "UID", "START_TIME", "NODELIST", "WORKDIR", and "STATE".
            Please consult 'squeue(1)' manpage for more information.
        """
        cmd = "squeue -o %all"
        if jobid:
            cmd += " -j {}".format(jobid)
        rc, out = self.exec_run(cmd)
        if rc:
            raise RuntimeError("squeue error, rc: {}, output: {}" \
                                                            .format(rc, out))
        lines = out.splitlines()
        hdr = lines.pop(0)
        keys = hdr.split('|')
        return [ dict(zip(keys, l.split('|'))) for l in lines ]

    def scancel(self, jobid):
        """Cancel job"""
        cmd = "scancel {}".format(jobid)
        self.exec_run(cmd)

    def ldms_ls(self, *args):
        """Executes `ldms_ls` with *args, and returns (rc, output)"""
        cmd = "ldms_ls " + (" ".join(args))
        return self.exec_run(cmd)

    def make_ovis_env(self):
        """Make ovis environment (ld, profile)

        NOTE: We need the ld.so.conf.d/ovis.conf because the process
        initializing slurm job forked from slurmd does not inherit
        LD_LIBRARY_PATH. The /etc/profile.d/ovis.sh is for ssh session's
        convenience (making ldms binaries available for SSH session).
        """
        for cont in self.containers:
            cont.write_file("/etc/ld.so.conf.d/ovis.conf",
                            "/opt/ovis/lib\n"
                            "/opt/ovis/lib64\n")
            cont.exec_run("ldconfig")
            profile = """
                function _add() {
                    # adding VALUE into variable NAME
                    local NAME=$1
                    local VALUE=$2
                    [[ *:${!NAME}:* = *:${VALUE}:* ]] ||
                        eval export ${NAME}=${!NAME}:${VALUE}
                }

                PREFIX=/opt/ovis
                _add PATH $PREFIX/bin
                _add PATH $PREFIX/sbin
                _add LD_LIBRARY_PATH $PREFIX/lib
                _add LD_LIBRARY_PATH $PREFIX/lib64
                _add MANPATH $PREFIX/share/man
                _add PYTHONPATH $PREFIX/lib/python2.7/site-packages

                export ZAP_LIBPATH=$PREFIX/lib/ovis-lib
                _add ZAP_LIBPATH $PREFIX/lib64/ovis-lib
                export LDMSD_PLUGIN_LIBPATH=$PREFIX/lib/ovis-ldms
                _add LDMSD_PLUGIN_LIBPATH $PREFIX/lib64/ovis-ldms
            """
            cont.write_file("/etc/profile.d/ovis.sh", profile)


class TADATest(object):
    """TADA Test Utility

    Example:
        test = TADATest(test_suite = "mysuite", test_type = "type",
                        test_name = "simple_test",
                        test_host = "localhost",
                        tada_port = 9862)
        test.start()
        test.add_assertion(1, "Test One")
        test.add_assertion(2, "Test Two")
        test.add_assertion(3, "Test Three")
        ... # do some work
        test.assert_test(1, rc == 0, "verifying rc == 0")
        ... # do some other work
        test.assert_test(3, num == 5, "verifying num == 5")
        # Test Two is skipped, and will be reported in tadad as "skipped"
        test.finish()
    """

    SKIPPED = "skipped"
    PASSED = "passed"
    FAILED = "failed"

    def __init__(self, test_suite, test_type, test_name,
                       tada_host = "localhost", tada_port = 9862):
        self.test_suite = test_suite
        self.test_type = test_type
        self.test_name = test_name
        self.tada_host = tada_host
        self.tada_port = tada_port
        self.assertions = dict()
        self.sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _send(self, msg):
        if type(msg) != str:
            msg = json.dumps(msg)
        self.sock_fd.sendto(msg.encode('utf-8'),
                            (self.tada_host, self.tada_port))

    def start(self):
        """Notify `tadad` that the test has started"""
        msg = {
                "msg-type": "test-start",
                "test-suite": self.test_suite,
                "test-type": self.test_type,
                "test-name": self.test_name,
                "timestamp": time.time(),
              }
        self._send(msg)

    def add_assertion(self, number, desc):
        """Add test assertion point"""
        self.assertions[number] = {
                        "msg-type": "assert-status",
                        "test-suite": self.test_suite,
                        "test-type": self.test_type,
                        "test-name": self.test_name,
                        "assert-no": number,
                        "assert-desc": desc,
                        "assert-cond": "none",
                        "test-status": TADATest.SKIPPED,
                    }

    def _send_assert(self, assert_no):
        self._send(self.assertions[assert_no])

    def assert_test(self, assert_no, cond, cond_str):
        """Evaluate the assert condition and notify `tadad`"""
        msg = self.assertions[assert_no]
        msg["assert-cond"] = cond_str
        msg["test-status"] = TADATest.PASSED if cond else TADATest.FAILED
        self._send(msg)

    def finish(self):
        """Notify the `tadad` that the test is finished"""
        for num, msg in self.assertions.iteritems():
            if msg["test-status"] == TADATest.SKIPPED:
                self._send(msg)
        msg = {
                "msg-type": "test-finish",
                "test-suite": self.test_suite,
                "test-type": self.test_type,
                "test-name": self.test_name,
                "timestamp": time.time(),
              }
        self._send(msg)

if __name__ == "__main__":
    execfile(os.getenv('PYTHONSTARTUP', '/dev/null'))
