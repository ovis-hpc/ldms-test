#!/usr/bin/python3

import os
import re
import pdb
import sys
import time
import glob
import json
import errno
import socket
import ipaddress as ip

import docker

from abc import abstractmethod
from io import StringIO, BytesIO
from distutils.version import LooseVersion

from LDMS_Test import cached_property, LDMSDContainerTTY, LDMSDContainer, \
                      LDMSDCluster, Spec, env_dict, cs_rm, G

# `D` Debug object to store values for debugging
class Debug(object): pass
D = Debug()

def get_cluster_class():
    return DockerCluster

def process_config(conf):
    pass # do nothing

def get_docker_clients():
    """Get all docker clients to dockerds in the swarm"""
    dc = docker.from_env()
    nodes = dc.nodes.list()
    addrs = [ n.attrs["Description"]["Hostname"] for n in nodes ]
    addrs.sort()
    if len(addrs) == 1:
            return [ dc ]
    return [ docker.DockerClient(base_url = "tcp://{}:2375".format(a)) \
                    for a in addrs ]


class ContainerTTY(LDMSDContainerTTY):
    """A utility to communicate with a process inside a container"""
    EOT = b'\x04' # end of transmission (ctrl-d)

    def __init__(self, sockio):
        self.sockio = sockio
        self.sock = sockio._sock
        self.sock.setblocking(False) # use non-blocking io

    def read(self, idle_timeout = 1):
        bio = BytesIO()
        active = 1
        while True:
            try:
                buff = self.sock.recv(1024)
                if not buff:
                    break
                bio.write(buff)
                active = 1 # stay active if read succeed
            except BlockingIOError as e:
                if e.errno != errno.EAGAIN:
                    raise
                if not active: # sock stays inactive > idle_timeout
                    break
                active = 0
                time.sleep(idle_timeout)
        val = bio.getvalue()
        return cs_rm(val.decode()) if val is not None else None

    def write(self, data):
        if type(data) == str:
            data = data.encode()
        self.sock.send(data)

    def term(self):
        if self.sock:
            self.sock.send(self.EOT)
            self.sock = None
            self.sockio.close()


#####################################################
#                                                   #
#   Convenient wrappers for docker.models classes   #
#                                                   #
#####################################################

class Container(LDMSDContainer):
    """Docker Container Wrapper and an implementation of LDMSDContainer

    This class wraps docker.models.containers.Container, providing additional
    convenient methods (such as `read_file()` and `write_file()`) and properties
    (e.g. `ip_addr`). The wrapper only exposes the following APIs:
        - attrs : dict() of Container attributes,
        - name : the name of the container,
        - client : the docker client handle for manipulating the container,
        - exec_run() : execute a program inside the container,
        - remove() : remove the container.

    It also implements LDMSDContainer interfaces.

    """
    def __init__(self, obj, cluster=None):
        if not isinstance(obj, docker.models.containers.Container):
            raise TypeError("obj is not a docker Container")
        LDMSDContainer.__init__(self, obj, cluster)
        self.cluster = cluster
        self.obj = obj
        self.attrs = obj.attrs
        self._name = obj.name
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
            self.obj.reload()
            time.sleep(1)
        return True

    def exec_run(self, cmd, env=None, user=None):
        if type(cmd) != list:
            cmd = [ '/bin/bash', '-c', cmd ]
        return self._exec_run(cmd, environment=env, user=user)

    def _exec_run(self, *args, **kwargs):
        self.wait_running()
        (rc, out) = self.obj.exec_run(*args, **kwargs)
        if type(out) == bytes:
            return (rc, out.decode())
        return (rc, out)

    def exec_interact(self, cmd):
        """Execute `cmd` in the container with an interactive TTY

        Returns a ContainerTTY for communicating to the process spawned from
        `cmd` inside the container.
        """
        (rc, out) = self._exec_run(cmd, stdout=True, stderr=True, stdin=True,
                                  tty=True, socket=True)
        return ContainerTTY(out)

    def remove(self, **kwargs):
        self.obj.remove(**kwargs)

    def get_aliases(self):
        """The list of aliases of the container hostname"""
        return self.cluster.node_aliases.get(self.hostname, [])

    def get_ip_addr(self):
        try:
            for name, addr, addr6 in self.interfaces:
                if name == self.cluster.net.name:
                    return addr
            return None
        except:
            return None

    def get_ipv6_addr(self):
        try:
            for name, addr, addr6 in self.interfaces:
                if name == self.cluster.net.name:
                    return addr6
            return None
        except:
            return None

    def get_interfaces(self):
        """Return a list() of (network_name, ipv4_addr, ipv6_addr) of the container."""
        return [ (k, v['IPAddress'], v.get('GlobalIPv6Address')) for k, v in \
                 self.attrs['NetworkSettings']['Networks'].items() ]

    def get_name(self):
        return self.obj.name

    def get_hostname(self):
        """Return hostname of the container"""
        return self.attrs["Config"]["Hostname"]

    def get_host(self):
        return self.client.info()["Swarm"]["NodeAddr"]

    def get_env(self):
        """Return environment from container configuration.

        Please note that the environment in each `exec_run` may differ.
        """
        return self.attrs["Config"]["Env"]

    def write_file(self, path, content, user = None):
        """Write `content` to `path` in the container"""
        cmd = "/bin/bash -c 'cat - >{} && echo -n true'".format(path)
        rc, sock = self._exec_run(cmd, stdin=True, socket=True, user = user)
        sock = sock._sock # get the raw socket
        sock.setblocking(True)
        if type(content) == str:
            content = content.encode()
        sock.send(content)
        sock.shutdown(socket.SHUT_WR)
        D.ret = ret = sock.recv(8192)
        # skip 8-byte header
        ret = ret[8:].decode()
        sock.close()
        if ret != "true":
            raise RuntimeError(ret)

    def read_file(self, path):
        """Read file specified by `path` from the container"""
        cmd = "cat {}".format(path)
        rc, output = self._exec_run(cmd)
        if rc:
            raise RuntimeError("Error {} {}".format(rc, output))
        return output

    def pipe(self, cmd, content):
        """Pipe `content` to `cmd` executed in the container"""
        rc, sock = self._exec_run(cmd, stdin=True, socket=True)
        sock = sock._sock
        sock.setblocking(True)
        if type(content) == str:
            content = content.encode()
        sock.send(content)
        sock.shutdown(socket.SHUT_WR)
        D.ret = ret = sock.recv(8192)
        sock.close()
        if len(ret) == 0:
            rc = 0
            output = ''
        else:
            # skip 8-byte header
            output = ret[8:].decode()
            rc = ret[0]
            if rc == 1: # OK
                rc = 0
        return rc, output

    def start(self):
        return self.obj.start()

    def stop(self):
        return self.obj.stop()


def attr_grep(d, attr, lst = list()):
    if type(d) is list:
        for o in d:
            if type(o) is dict:
                attr_grep(o, attr, lst)
        return lst
    for k, v in d.items():
        if k.lower().find(attr.lower()) > -1:
            lst.append(v)
            continue
        if type(v) in [ dict, list ]:
            attr_grep(v, attr, lst)
            continue
    return lst

def next_subnet(net):
    addr = int(net.network_address)
    hostmask = int(net.hostmask)
    next_addr = addr + hostmask + 1
    if type(net) == ip.IPv4Network:
        net_addr = ip.IPv4Address(next_addr)
    elif type(net) == ip.IPv6Network:
        net_addr = ip.IPv6Address(next_addr)
    else:
        raise RuntimeError(f"Unknown network type: {type(net)}")
    net_str = f"{net_addr}/{net.prefixlen}"
    return ip.ip_network(net_str)

def next_ipv6_subnet():
    client = docker.from_env()
    nets = client.networks.list()
    nets_attrs = [ n.attrs for n in nets ]

    str_subnets = attr_grep(nets_attrs, 'subnet')
    subnets = [ ip.ip_network(s) for s in str_subnets ]
    ipv6_subnets = [ s for s in subnets if type(s) == ip.IPv6Network ]
    if not ipv6_subnets: # empty
        return ip.ip_network("fd00:0:0:1::/64")
    ipv6_subnets.sort()
    s = ipv6_subnets[-1]
    return next_subnet(s)

def next_ipv4_subnet():
    client = docker.from_env()
    nets = client.networks.list()
    nets_attrs = [ n.attrs for n in nets ]

    str_subnets = attr_grep(nets_attrs, "subnet")
    subnets = [ ip.ip_network(s) for s in str_subnets ]
    ipv4_subnets = [ s for s in subnets if type(s) == ip.IPv4Network and
                                           str(s).startswith("172.") ]
    if not ipv4_subnets: # empty
        return ip.ip_network("172.172.0.0/24")
    ipv4_subnets.sort()
    s = ipv4_subnets[-1]
    return next_subnet(s)

class Network(object):
    """Docker Network Wrapper"""

    def __init__(self, obj):
        if type(obj) != docker.models.networks.Network:
            raise TypeError("obj is not a docker Network object")
        self.obj = obj
        self.clients = get_docker_clients()
        subnets = attr_grep(obj.attrs, "subnet")
        self.net6 = None
        self.net4 = None
        for net_str in subnets:
            net = ip.ip_network(net_str)
            if type(net) == ip.IPv4Network:
                self.net4 = ip.ip_network(f"{net.network_address}/24")
            elif type(net) == ip.IPv6Network:
                self.net6 = ip.ip_network(f"{net.network_address}/120")

    @classmethod
    def create(cls, name, driver='overlay', scope='swarm', attachable=True,
                    labels = None, subnet = None, ipv6 = False):
        """A utility to create and wrap the docker network"""
        client = docker.from_env()
        try:
            if subnet:
                # NOTE: `iprange` is for docker automatic IP assignment.
                #       Since LMDS_Test manually assign IP addresses, we will
                #       limit docker iprange to be a small number.
                #       `gateway` is the max host IP address.
                ip_net = ip.ip_network(subnet)
                bc = int(ip_net.broadcast_address)
                gateway = str(ip.ip_address(bc - 1))
                iprange = str(ip.ip_address(bc & ~3)) + "/30"
                ipam_pool = docker.types.IPAMPool(subnet=subnet,
                                        iprange=iprange, gateway=gateway)
                ipam_config = docker.types.IPAMConfig(pool_configs=[ipam_pool])
                obj = client.networks.create(name=name, driver=driver,
                                     ipam=ipam_config,
                                     scope=scope, attachable=attachable,
                                     labels = labels,
                                     enable_ipv6 = ipv6)
            else:
                params = dict(name=name, driver=driver, scope=scope,
                              attachable=attachable, labels = labels,
                              enable_ipv6 = ipv6)
                obj = client.networks.create(**params)
        except docker.errors.APIError as e:
            if e.status_code != 409: # other error, just raise it
                raise
            msg = e.explanation + ". This could be an artifact from previous " \
                  "run. To remove the network, all docker objects using net " \
                  "network must be remvoed first (e.g. service, container). " \
                  "Then, remove the network with `docker network rm {}`." \
                  .format(name)
            raise RuntimeError(msg)
        time.sleep(5)
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

    def connect(self, container, *args, **kwargs):
        obj = self.obj.connect(container, *args, **kwargs)
        time.sleep(1)
        return obj

def get_host_tz():
    # try TZ env first
    try:
        tz = os.getenv('TZ')
        if tz is not None:
            return tz
    except:
        pass
    # try /etc/timezone
    try:
        tz = open('/etc/timezone').read().strip()
        if tz:
            return tz
    except:
        pass
    # then try /etc/localtime link
    try:
        tz = '/'.join(os.readlink('/etc/localtime').split('/')[-2:])
        return tz
    except:
        pass
    # otherwise, UTC
    return 'Etc/UTC'

class DockerCluster(LDMSDCluster):
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
    def _create(cls, spec):
        sp = Spec(spec)
        kwargs = cls.spec_to_kwargs(sp)
        return cls.__create(spec=sp, **kwargs)

    @classmethod
    def __create(cls, name, image = "centos:7", nodes = 8,
                    mounts = [], env = [], labels = {},
                    node_aliases = {},
                    cap_add = [],
                    cap_drop = [],
                    subnet = None,
                    host_binds = {},
                    spec = None):
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
        if type(nodes) == int:
            nodes = [ "node-{}".format(i) for i in range(0, nodes) ]
        lbl = dict(labels)
        _env = env
        env = dict(_env) # shallow copy env
        host_tz = get_host_tz()
        env.setdefault('TZ', host_tz)
        cfg = dict(name = name,
                   image = image,
                   env = env,
                   nodes = nodes,
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
        _n = len(nodes) # number of containers needed
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
        idx = 0
        for load, n, cl in tbl:
            # allocate `n` container using `client`
            cl_info = cl.info()
            cl_name = cl_info["Name"]
            for i in range(0, n):
                node = nodes[idx]
                idx += 1
                hostname = node
                cont_image = image
                cont_volumes = volumes
                cont_env = dict(env) # shallow copy env
                cont_name = "{}-{}".format(name, node)
                tmpfs = None
                atstart = G.conf["ldms-test"]["atstart"]
                try:
                    spec_nodes = spec["nodes"]
                    for _n in spec_nodes:
                        if _n["hostname"] == hostname:
                            tmpfs = _n.get("tmpfs")
                            cont_image = _n.get("image", image)
                            cont_volumes = _n.get("mounts", volumes)
                            _cont_env = _n.get("env", env)
                            cont_env.update(_cont_env)
                            atstart = _n.get("atstart", atstart)
                            break
                except:
                    pass
                cont_param = dict(
                        image = cont_image,
                        name = cont_name,
                        # command = "/bin/bash",
                        entrypoint = [ "/bin/bash" ],
                        tty = True,
                        detach = True,
                        environment = cont_env,
                        volumes = cont_volumes,
                        cap_add = cap_add,
                        cap_drop = cap_drop,
                        #network = name,
                        hostname = hostname,
                        ulimits = [
                            docker.types.Ulimit(
                                name = "nofile",
                                soft = 1000000,
                                hard = 1000000,
                            )
                        ],
                        tmpfs = tmpfs,
                        atstart = atstart,
                    )
                #if not subnet:
                #    cont_param["network"] = name
                binds = host_binds.get(hostname)
                if binds:
                    cont_param["ports"] = binds
                lbl_cont_build.append( (cl_name, cont_param) )
                cont_build.append( (cl, cont_param) )
        # memorize cont_build as a part of label
        dc = docker.from_env()
        lbl["cont_build"] = json.dumps(lbl_cont_build)
        ipv6 = spec.get("ipv6", False) if spec else False
        net = Network.create(name = name, driver = "overlay",
                             attachable = True, scope = "swarm",
                             labels = lbl, subnet = subnet, ipv6 = ipv6)
        if subnet:
            ip_net = ip.ip_network(subnet)
            ip_itr = ip_net.hosts()
        # then, create the actual containers
        first = True
        for cl, params in cont_build:
            atstart = params.pop('atstart')
            _retry = 3
            while _retry:
                cont = cl.containers.create(**params)
                if subnet:
                    ip_addr = next(ip_itr)
                    net.connect(cont, ipv4_address=str(ip_addr))
                else:
                    net.connect(cont)
                if atstart:
                    import tarfile
                    from io import BytesIO
                    b = BytesIO()
                    t = tarfile.open(mode="w", fileobj=b)
                    def tfltr(ti: tarfile.TarInfo):
                        ti.uid = 0
                        ti.gid = 0
                        ti.uname = "root"
                        ti.gname = "root"
                        return ti
                    t.add(atstart, "atstart.sh",filter=tfltr)
                    t.close()
                    cont.put_archive("/", b.getvalue())
                try:
                    cont.start()
                    if atstart:
                        cont.exec_run("/bin/bash /atstart.sh")
                    break # succeeded, no more retry
                except:
                    raise
                    _retry -= 1
                    if not _retry:
                        raise
                    cont.remove() # failed :(  .. remove and retry
                    time.sleep(1)
            if first:
                # must wait for the special swarm container to be up
                _retry = 10
                while _retry:
                    time.sleep(1)
                    net.obj.reload()
                    if len(net.obj.attrs["Containers"]) > 1:
                        break
                    _retry -= 1
            first = False
        cluster = DockerCluster(net.obj)
        cluster.update_etc_hosts(node_aliases = node_aliases)
        return cluster

    @classmethod
    def spec_to_kwargs(cls, spec):
        """Convert `spec` to kwargs for DockerCluster.create()"""
        name = spec["name"]
        nodes = spec["nodes"]
        mounts = []
        prefix = spec.get("ovis_prefix")
        _PYTHONPATH = None
        if prefix:
            mounts += ["{}:/opt/ovis:ro".format(prefix)]
            # handling python path
            pp = glob.glob(prefix+'/lib*/python*/*-packages')
            pp = [ p.replace(prefix, '/opt/ovis', 1) for p in pp ]
            _PYTHONPATH = ':'.join(pp)
        if not _PYTHONPATH:
            _PYTHONPATH = "/opt/ovis/lib/python3.6/site-packages:" \
                          "/opt/ovis/lib64/python3.6/site-packages"
        mounts += spec.get("mounts", [])
        cap_add = spec.get("cap_add", [])
        cap_drop = spec.get("cap_drop", [])
        # assign daemons to containers using node_aliases
        node_aliases = {}
        hostnames = [ node["hostname"] for node in nodes ]
        host_binds = { node["hostname"]: node["binds"] \
                    for node in nodes if node.get("binds") }
        for node in nodes:
            a = node.get("aliases")
            if a:
                node_aliases[node["hostname"]] = a
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
                "ZAP_LIBPATH" : "/opt/ovis/lib/ovis-ldms:/opt/ovis/lib64/ovis-ldms:/opt/ovis/lib/ovis-lib:/opt/ovis/lib64/ovis-lib",
                "LDMSD_PLUGIN_LIBPATH" : "/opt/ovis/lib/ovis-ldms:/opt/ovis/lib64/ovis-ldms",
                "PYTHONPATH" : _PYTHONPATH
            }
        env.update(env_dict(spec.get("env", {})))
        kwargs = dict(
                    name = name,
                    image = spec.get("image", "ovis-centos-build"),
                    mounts = mounts,
                    nodes = hostnames,
                    env = env,
                    labels = { "LDMSDCluster.spec": json.dumps(spec) },
                    node_aliases = node_aliases,
                    cap_add = cap_add,
                    cap_drop = cap_drop,
                    subnet = spec.get("subnet"),
                    host_binds = host_binds,
                 )
        return kwargs

    @classmethod
    def _get(cls, name):
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
            raise LookupError()

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

    def get_containers(self, timeout = 10):
        """Return a list of docker Containers of the virtual cluster"""
        our_conts = []
        clients = get_docker_clients()
        for cl in clients:
            conts = cl.containers.list(all=True)
            for cont in conts:
                if cont.attrs['NetworkSettings']['Networks'].get(self.net.name):
                    our_conts.append(cont)

        cont_list = [ Container(c, self) for c in our_conts ]
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
            for k, v in self.node_aliases.items():
                cont = cont_dict[k]
                if type(v) == str:
                    v = [ v ]
                for n in v:
                    cont_dict[n] = cont
            self.cont_dict = cont_dict
        return self.cont_dict.get(name)

    def get_node_aliases(self):
        """dict(hostname:list) - node aliases by hostname"""
        txt = self.net.obj.attrs["Labels"]["node_aliases"]
        return json.loads(txt)

    def remove(self):
        """Remove the cluster"""
        for cont in self.containers:
            try:
                self.net.obj.disconnect(cont.obj)
            except:
                raise
        self.net.remove()
        for cont in self.containers:
            try:
                cont.remove(force = True)
            except:
                raise

    @property
    def labels(self):
        """Labels"""
        return self.net.obj.attrs["Labels"]

    def get_spec(self):
        return json.loads(self.labels["LDMSDCluster.spec"])

    def get_name(self):
        return self.net.name

    @classmethod
    def _list(cls):
        dc = docker.client.from_env()
        nets = [ Network(n) for n in dc.networks.list() ]
        clusters = [ cls(n.obj) for n in nets if n.labels and n.labels.get('DockerCluster') ]
        return clusters
# ----------------------------------------------------------- DockerCluster -- #
