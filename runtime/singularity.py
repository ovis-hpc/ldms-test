#!/usr/bin/python3

import os
import io
import re
import pty
import sys
import pdb
import time
import json
import shlex
import heapq
import errno
import socket
import logging
import threading

from bisect import bisect, insort

from ipaddress import ip_network, ip_address

from subprocess import Popen, PIPE, STDOUT, DEVNULL, run

from LDMS_Test import G, cached_property, LDMSDContainerTTY, LDMSDContainer, \
                      LDMSDCluster, Spec, env_dict, bash_items, deep_copy

# REMARK: G.conf["singularity"] contains singularity configurations

logger = logging.getLogger(__name__)

class Debug(object): pass
D = Debug()

class Obj(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

def get_cluster_class():
    if hasattr(G, "conf"):
        process_config(G.conf)
    return SCluster

def process_config(conf):
    if not hasattr(G, "sing_hosts"):
        G.sing_hosts = bash_items(G.conf["singularity"]["hosts"])
        p = G.conf["singularity"]["ip_addr_db"]
        G.ip_addr_db = IPAddrDb(p)

def get_instance_name(node_spec, cluster_spec):
    """Returns container instance name"""
    cluster_name = cluster_spec["name"]
    node_name = node_spec["hostname"]
    return "{}-{}".format(cluster_name, node_name)

def BYTES(s):
    if type(s) == str:
        return s.encode()
    if type(s) == bytes:
        return s
    raise TypeError("Unsupported type: {}".format(type(s)))


def STR(s):
    if type(s) == bytes:
        return s.decode()
    if type(s) == str:
        return s
    raise TypeError("Unsupported type: {}".format(type(s)))


def ssh(host, cmd='', input=None, interactive=False, wait=True, port=22, **kwargs):
    """a utility that mimics Popen() and run() but under `ssh` command

    Examples
    --------
    # interactive
    >>> p = ssh('node1', interactive=True)
    >>> p.stdin.write(b'hostname\n')
    9
    >>> p.stdout.read()
    b'node1\n'
    >>> p.stdin.write(b'pwd\n')
    4
    >>> p.stdout.read()
    b'/root\n'
    >>>

    # one shot
    >>> p = ssh('node1', cmd='hostname')
    >>> p.wait()
    0
    >>> p.stdout.read()
    b'node1\n'

    # one shot with input
    >>> p = ssh('node1', cmd='bash', input='echo $HOSTNAME')
    >>> p.wait()
    0
    >>> p.stdout.read()
    b'node1\n'

    Returns
    -------
    obj(Popen): a Popen object for the ssh process
    """
    _kwargs = dict(shell = True, stdout = PIPE, stdin = PIPE, stderr = STDOUT,
                   executable = "/bin/bash")
    _kwargs.update(kwargs)
    p = Popen("ssh -T {} -p {} {}".format(host, port, cmd), **_kwargs)
    p.stdin = p.stdin.detach() # don't use buffer
    p.stdout = p.stdout.detach()
    os.set_blocking(p.stdout.fileno(), False)
    if p.stderr:
        p.stderr = p.stderr.detach()
        os.set_blocking(p.stderr.fileno(), False)
    if input:
        p.stdin.write(BYTES(input))
    if not interactive:
        p.stdin.close()
        if wait:
            p.wait()
    return p


class HexRanges(object):

    RE = re.compile(r'([0-9A-F]+)(?:-([0-9A-F]+))?(?:,|$)', flags=re.IGNORECASE)

    def __init__(self, _str=None):
        self._entries = []
        if _str:
            for a, b in self.RE.findall(_str):
                a = int(a, base=16)
                b = int(b, base=16) if b else None
                ent = [a, b] if b is not None else [a, a]
                insort(self._entries, ent)
            self.prune()

    @classmethod
    def union(cls, *args ):
        tmp = HexRanges()
        for r in args:
            tmp._entries.extend(r._entries)
        tmp.prune()
        return tmp

    def prune(self):
        self._entries.sort()
        tmp = []
        for ent in self._entries:
            if not tmp or tmp[-1][1] < ent[0]:
                tmp.append(ent)
                continue
            tmp[-1][1] = ent[1]
        self._entries = tmp

    def __str__(self):
        sio = io.StringIO()
        first = True
        for ent in self._entries:
            if not first:
                sio.write(",")
            if ent[0] == ent[1]:
                sio.write("{:X}".format(ent[0]))
            else:
                sio.write("{:X}-{:X}".format(*ent))
            first = False
        return sio.getvalue()

    def ins(self, ent):
        if not self._entries:
            self._entries.append(ent)
            return
        idx = bisect(self._entries, ent)
        l = r = None
        if idx > 0:
            l = self._entries[idx-1]
        if idx < len(self._entries):
            r = self._entries[idx]
        if l and l[1] >= ent[0]:
            if r and r[0] <= ent[1]: # ent joins l and r
                l[1] = r[1]
                self._entries.pop(idx)
            else: # ent goes into l
                l[1] = ent[1]
            return # done
        if r and r[0] <= ent[1]:
            # ent goes into r
            r[0] = ent[0]
            return # done
        # otherwise, ent is a new entry
        self._entries.insert(idx, ent)

    def inv(self, _min=-float('inf'), _max=float('inf')):
        """Returns an inverse ranges of self ( [-Inf, Inf] \ self )"""
        _new = HexRanges()
        l = _min
        for ent in self._entries:
            r = ent[0] - 1
            if l <= r:
                _new.ins([l, r])
            l = ent[1] + 1 # for the next entry
        r = _max
        if l <= r:
            _new.ins([l, r])
        return _new

    @property
    def first(self):
        return self._entries[0] if self._entries else None

    def pop(self):
        self._entries.pop(0)

    def __iter__(self):
        for ent in self._entries:
            for i in range(ent[0], ent[1]+1):
                yield i
# --------------------------------------------------------------- HexRanges -- #


class IPAddrDb(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self._f = open(db_path, "a+b", buffering=0)
        network_str = G.conf['singularity']['ip_addr_space']
        self.net = ip_network(network_str)
        self.init_db()

    def lock(self):
        """Taking an exclusive lock on the DB file

        This blocks until the other process releases the lock.
        """
        self._f.seek(0)
        os.lockf(self._f.fileno(), os.F_LOCK, 0)

    def trylock(self):
        """Try acquiring the DB file lock without blocking

        This raises BlockingIOError(11) if the other process has taken the lock.
        """
        self._f.seek(0)
        os.lockf(self._f.fileno(), os.F_TLOCK, 0)

    def unlock(self):
        """Release the file lock taken in IPAddrDb.lock()"""
        self._f.seek(0)
        os.lockf(self._f.fileno(), os.F_ULOCK, 0)

    def _read(self):
        # NOTE: Caller must hold flock
        sio = io.StringIO()
        self._f.seek(0)
        b = self._f.read()
        while len(b):
            sio.write(b.decode())
            b = self._f.read()
        try:
            db = json.loads(sio.getvalue())
            alloc = db["alloc_tbl"]
            for k,v in alloc.items():
                alloc[k] = HexRanges(v)
            return db
        except Exception as e:
            return None

    def _write(self, db):
        # NOTE: Caller must hold flock
        self._f.truncate(0)
        self._f.seek(0)
        alloc = db["alloc_tbl"]
        for k,v in alloc.items():
                alloc[k] = str(v)
        self._f.write( json.dumps(db, indent=2).encode() )

    def init_db(self):
        self.lock()
        db = self._read()
        if db is None:
            self._write( {"alloc_tbl": {}} )
        self.unlock()

    def alloc_addr(self, cluster_name, n):
        """Request `n` IP addresses for `cluster_name`"""
        self.lock()
        db = self._read()
        alloc_tbl = db["alloc_tbl"]
        addrs = alloc_tbl.get(cluster_name)
        if addrs:
            self.unlock()
            return addrs
        u = HexRanges.union(*alloc_tbl.values())
        net_addr = int(self.net.network_address)
        host_mask = int(self.net.hostmask)
        addr_min = net_addr + 1
        addr_max = (net_addr | host_mask) - 1
        avail = u.inv(_min=addr_min, _max=addr_max)
        addrs = HexRanges()
        _n = n
        ent = avail.first
        while _n and ent:
            if _n < ent[1] - ent[0] + 1:
                # no need to remove ent
                a = [ent[0], ent[0] + _n - 1]
                addrs.ins(a)
                ent[0] = ent[0] + _n
                _n = 0
            else:
                # take entire entry
                _n -= ent[1] - ent[0] + 1
                addrs.ins(ent)
                avail.pop()
            ent = avail.first
        if _n:
            addrs = None
        else:
            # update allocation
            alloc_tbl[cluster_name] = addrs
            self._write(db)
        self.unlock()
        return addrs

    def free_addr(self, cluster_name):
        self.lock()
        db = self._read()
        alloc_tbl = db["alloc_tbl"]
        alloc_tbl.pop(cluster_name, None)
        self._write(db)
        self.unlock()
# ---------------------------------------------------------------- IPAddrDb -- #


class PtyPopen(object):
    """Like subprocess.Popen(), but with PTY for I/O

    Example
    -------
    >>> p = PtyPopen("/bin/bash")
    >>> p.write("whoami\n")
    >>> out = p.read()
    >>> print(out)
    $ whoami
    root
    $
    """
    def __init__(self, args, **kwargs):
        """Initialization

        All arguments are passed to `Popen(args, **kwargs)`. `stdin`, `stdout`
        and `stderr` of the Popen will be set to the PTY. Hence, `stdin`,
        `stdout`, and `stderr` parameters must not be specified, or
        `AttributeError` will be raised.

        `start_new_session` parameter must also NOT be specified. It will be set
        to `True` so that the PtyPopen process is the leader of the new session
        (to gain control over jobs that might be spawned from it).

        """
        self._pfd, cfd = pty.openpty()
        os.set_blocking(self._pfd, False)
        s = set(["stdin", "stdout", "stderr", "start_new_session"]).intersection(kwargs)
        if s:
            raise AttributeError("Parameters `{}` must not be specified".format(list(s)))
        self._proc = Popen(args, stdin=cfd, stdout=cfd, stderr=STDOUT,
                start_new_session=True, **kwargs)
        os.close(cfd)
        D._proc = self._proc

    def __del__(self):
        if self._pfd is not None:
            os.close(self._pfd)
            self._pfd = None

    def read(self, idle_timeout = 0.1):
        """Keep reading output until it became idled"""
        if self._pfd is None:
            raise RuntimeError("pty terminated")
        sio = io.StringIO()
        active = 2
        while active:
            try:
                b = os.read(self._pfd, 4096)
            except OSError as e:
                D.ex = e
                if e.errno != errno.EAGAIN:
                    raise
            else:
                D.ex = None
                sio.write(STR(b))
                active = 2
                continue
            active -= 1
            if active:
                time.sleep(idle_timeout)
        return sio.getvalue()

    def write(self, data):
        if self._pfd is None:
            raise RuntimeError("pty terminated")
        return os.write(self._pfd, BYTES(data))

    def term(self):
        if self._pfd is not None:
            try:
                self.write('\x04') # Ctrl-D/EOT (end of transmission)
                self.read()
            except:
                pass
            os.close(self._pfd)
            self._pfd = None
            self._proc.wait()
# END class PtyPopen -----------------------------------------------------------

class PtySSH(object):
    """SSH wrapper with PTY

    Setup `ssh -t {HOST} /usr/bin/bash -i` with `\x02$ ` prompt.
    """

    PROMPT = '\x02$ '

    def __init__(self, host, port=22):
        cmd = ["/usr/bin/ssh", "-t", "-p", str(port), host, "/usr/bin/bash", "-i"]
        self._pty = PtyPopen(cmd)
        self._pty.write("unset PROMPT_COMMAND && export PS1='\x02$ '\n")
        D.buff = self.readp()

    def __del__(self):
        self._pty.term()

    def set_echo(self, val):
        """Set TTY echo to `True` (on) or `False` (off)"""
        cmd = "stty {}echo\n".format("-" if not val else "")
        self.write(cmd)
        self.readp()

    def write(self, data):
        return self._pty.write(data)

    def read(self, idle_timeout = 0.1):
        return self._pty.read(idle_timeout)

    def readp(self):
        """Blocking read until seeing the PROMPT `\x02$ `"""
        sio = io.StringIO()
        while True:
            buff = self._pty.read()
            sio.write(buff)
            if sio.getvalue().endswith(self.PROMPT):
                break
        return sio.getvalue()
# ------------------------------------------------------------------ PtySSH -- #


def _inst_list():
    inst_list = list()
    items = list()
    logger.debug("(_inst_list) start")
    _local = socket.gethostname()
    for host in G.sing_hosts:
        host, port = (host.split(':') + [22])[:2]
        _ssh = "" if host == _local or host == "localhost" else \
               "ssh -T {} -p {} ".format(host, port)
        cmd = _ssh + "singularity instance list --json"
        # cmd = "ssh -T {} -p {} singularity instance list --json".format(host, port)
        logger.debug("(_inst_list) ssh to {}".format(host))
        p = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True,
                  executable="/bin/bash")
        logger.debug("(_inst_list) -- background")
        items.append( (host, port, p) )
    for host, port, p in items:
        logger.debug("(_inst_list) waiting {}".format(host))
        p.wait()
        logger.debug("(_inst_list) -- done".format(host))
        out = p.stdout.read().decode()
        objs = json.loads(out)
        insts = objs["instances"]
        for inst in insts:
            inst["ssh_host"] = host
            inst["ssh_port"] = port
        inst_list.extend(insts)
    logger.debug("(_inst_list) end")
    return inst_list


class SContainer(LDMSDContainer):
    """Logical object representing a Singularity Container Instance

    SContainer.list() -> a list of `SContainer` of the running singularity instances
    SContainer("NAME") -> a handle of NAME instance on localhost
    SContainer("NAME", ssh_host="node55") -> a handle of NAME instance on node55

    """

    JSON_KEYS = [ "instance", "img", "ip", "pid" ]

    CLUSTER_SPEC_PATH = "/.ldms-test/cluster_spec.json"
    NODE_SPEC_PATH = "/.ldms-test/node_spec.json"
    IP_ADDR_PATH = "/.ldms-test/ip_addr"

    def __init__(self, instance=None, sing_json=None,
                       ssh_host="localhost", ssh_port=22,
                       local_dir=None, hostname=None, img=None,
                       cluster = None, ip_addr = None, network=None,
                       cluster_spec={}, node_spec={}):
        """
        Parameters
        ----------
        instance(str): the instance name
        sing_json(dict): JSON object from singularity CLI output
        ssh_host(str): the name of the host hosting the container that we need
                       to communicate over SSH
        ssh_port(int): the port of the SSH
        hostname(str): the hostname to assign to the container (in `.start()`)
        img(str): the path to the container image. This overrides
                  cluster_spec["image"].
        clsuter_spec(dict): the spec of the CLUSTER
        node_spec(dict): the spec of the node
        """
        logger.debug("(SContainer.__init__) BEGIN")
        self._shell = dict()
        if cluster:
            LDMSDContainer.__init__(self, None, cluster)
        self._ip_addr = ip_addr
        self._net = network
        if sing_json:
            for k in self.JSON_KEYS:
                setattr(self, k, sing_json[k])
            if instance and instance != sing_json["instance"]:
                raise RuntimeError("`instance` not matching `sing_json['instance']`")
        else:
            if not instance:
                raise ValueError("Missing `instance` parameter")
            self.instance = instance
            self.img = img if img \
                           else cluster_spec.get("image",
                                   G.conf["ldms-test"].get("image"))
            if not self.img:
                raise ValueError("`img` parameter, `cluster_spec['image']`, and `image` configuration are not specified")
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        _local = socket.gethostname()
        if ssh_host == _local or ssh_host == "localhost":
            self._ssh = ""
            self._ssh_i = "" # interactive
        else:
            self._ssh = "ssh -T {} -p {}".format(ssh_host, ssh_port)
            self._ssh_i = "ssh -t {} -p {}".format(ssh_host, ssh_port)
        if not local_dir:
            local_dir = G.conf["singularity"].get("local_dir", "/tmp")
            local_dir = "{}/{}".format(local_dir, self.instance)
        self.local_dir = local_dir
        self._hostname = hostname if hostname else \
                         node_spec.get("hostname", self.instance)
        self._cluster_spec = cluster_spec
        self._node_spec = node_spec
        if self.is_running:
            logging.debug("(SContainer __init__) checking SContainer")
            self.check_SContainer()
            logging.debug("(SContainer __init__) loading specs ...")
            self._cluster_spec = self.json_load(self.CLUSTER_SPEC_PATH)
            self._node_spec = self.json_load(self.NODE_SPEC_PATH)
            logging.debug("(SContainer __init__) ... done")
        logging.debug("(SContainer __init__) END")

    def check_SContainer(self):
        """Check whether or not it is started by SContainer"""
        if self.is_running:
            rc, out = self.exec_run("python3 -m json.tool {}".format(self.CLUSTER_SPEC_PATH))
            if rc:
                raise TypeError("Not a SContainer")
            rc, out = self.exec_run("python3 -m json.tool {}".format(self.NODE_SPEC_PATH))
            if rc:
                raise TypeError("Not a SContainer")

    def __repr__(self):
        cls = type(self)
        return "<{}.{} '{}'>".format(cls.__module__, cls.__name__, self.instance)

    def json_load(self, path_in_container):
        txt = self.read_file(path_in_container)
        return json.loads(STR(txt))

    @classmethod
    def list(cls):
        """Return a list of running singularity instances"""
        instances = _inst_list()
        lst = list()
        for inst in instances:
            try:
                obj = cls(sing_json = inst, ssh_host = inst["ssh_host"],
                          ssh_port = inst["ssh_port"])
            except TypeError:
                pass
            else:
                lst.append(obj)
        return lst

    @classmethod
    def find(cls, name):
        for inst in _inst_list():
            if inst["instance"] == name:
                return cls(sing_json = inst, ssh_host = inst["ssh_host"],
                                             ssh_port = inst["ssh_port"])
        return None

    def as_dict(self):
        return { k: getattr(self, k) for k in self.JSON_KEYS }

    def as_json(self):
        return json.dumps(self.as_dict())

    IP_RE = re.compile(r'\s*\|-- ([0-9.]+)')
    def get_ip_addr(self):
        """Retreive the first non loopback IP address from /proc/PID/net/fib_trie"""
        addrs = set()
        cmd = self._ssh + " cat /proc/{}/net/fib_trie".format(self.pid)
        p = run(cmd, shell=True, stdout=PIPE, stderr=STDOUT,
                executable="/bin/bash")
        if p.returncode:
            raise RuntimeError(p.stdout)
        for l in STR(p.stdout).splitlines():
            m = self.IP_RE.match(l)
            if not m:
                continue
            addr = m.group(1)
            if addr.endswith('.0') or addr.startswith('127.') or addr.endswith('.255'):
                continue
            return addr

    def set_ip_addr(self, ip_addr):
        cmd = "set -e; ip addr flush dev eth0; ip addr add {} dev eth0;".format(ip_addr)
        rc, out = self.pipe("bash", content=cmd)
        if rc:
            raise RuntimeError(out)

    def _prep_scmd(self, cmd, env=None, user=None, ssh=""):
        if type(cmd) is list:
            cmd = " ".join( shlex.quote(c) for c in cmd )
        if user is not None:
            cmd = "runuser {} -s /bin/bash -c {}".format(user, shlex.quote(cmd))
        envs = ""
        _env = dict(self._cluster_spec.get("env", {}))
        if env:
            _env.update(env)
        def __append(env, var, ents):
            env[var] = env.get(var, "") + ":" + (":".join(ents))
        __append(_env, "PATH", ["/opt/ovis/bin", "/opt/ovis/sbin",
                                "/usr/local/bin", "/usr/local/sbin",
                                "/usr/bin", "/usr/sbin", "/bin", "/sbin"])
        __append(_env, "LD_LIBRARY_PATH", ["/opt/ovis/lib", "/opt/ovis/lib64"])
        __append(_env, "ZAP_LIBPATH", ["/opt/ovis/lib/ovis-ldms",
                                       "/opt/ovis/lib64/ovis-ldms",
                                       "/opt/ovis/lib/ovis-lib",
                                       "/opt/ovis/lib64/ovis-lib"])
        __append(_env, "LDMSD_PLUGIN_LIBPATH", ["/opt/ovis/lib/ovis-ldms",
                                                "/opt/ovis/lib64/ovis-ldms"])
        __append(_env, "PYTHONPATH", ["/opt/ovis/lib/python3.6/site-packages",
                                      "/opt/ovis/lib64/python3.6/site-packages"])
        _env.setdefault("HOSTNAME", self._node_spec.get("hostname", ""))
        _ld_library_path = _env.get("LD_LIBRARY_PATH", "")
        for k,v in _env.items():
            envs += " " + shlex.quote("--env={}={}".format(k, v))
        scmd = "singularity exec {} instance://{} {}".format(envs, self.instance, cmd)
        if ssh:
            scmd = ssh + " " + shlex.quote(scmd)
        return scmd

    @property
    def shell(self):
        thr_id = threading.get_ident()
        p = self._shell.get(thr_id)
        if p:
            return p
        scmd = self._prep_scmd("bash -l", ssh=self._ssh)
        p = Popen(scmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT,
                  preexec_fn = os.setpgrp, executable="/bin/bash")
        p.stdin = p.stdin.detach() # don't use buffer
        p.stdout = p.stdout.detach()
        os.set_blocking(p.stdout.fileno(), False)
        if p.stderr:
            p.stderr = p.stderr.detach()
            os.set_blocking(p.stderr.fileno(), False)
        p.stdin.write(b'echo \xFF\n')
        while True:
            b = p.stdout.read()
            if b == b'\xFF\n':
                break
            if p.poll() is not None:
                return None
        self._shell[thr_id] = p
        p.read = p.stdout.read
        def p_write(p, data):
            off = 0
            while off < len(data):
                wlen = p.stdin.write(data[off:])
                off += wlen
        p.write = p_write.__get__(p, p.__class__)
        return p

    def _exec_run(self, cmd, env=None, in_data=None, timeout=None, user=None):
        if not self.shell:
            raise RuntimeError("cannot obtain container shell")
        if in_data:
            self.shell.write(b'{ cat <<\xFFEOF\n')
            self.shell.write(BYTES(in_data).replace(b'$', b'\\$'))
            self.shell.write(b'\n\xFFEOF\n')
            self.shell.write(b'} | sed -z \'s/\\n$//\' | ') # eliminate the '\n'
                                                            # we inserted right
                                                            # before '\xFFEOF'
        if env:
            for k, v in env.items():
                self.shell.write(BYTES(k))
                self.shell.write(b'=')
                self.shell.write(BYTES(shlex.quote(v)))
                self.shell.write(b' ')
        if user is not None:
            cmd = "runuser {} -s /bin/bash -c {}".format(user, shlex.quote(cmd))
        self.shell.write(BYTES(cmd))
        self.shell.write(b' 2>&1 ; echo "\xFFRC$?\xFFEND"\n')
        bio = io.BytesIO()
        t0 = time.time()
        while not bio.getvalue().endswith(b'\xFFEND\n'):
            b = self.shell.read()
            if b is not None:
                bio.write(b)
            if timeout:
                dt = time.time() - t0
                if dt >= timeout:
                    return (errno.ETIMEDOUT, bio.getvalue())
        b = bio.getvalue()
        idx = b.rindex(b"\xFFRC")
        rc = int(b[idx+3:].split(b'\xFF')[0])
        return (rc, b[:idx].decode())

    def _exec_run_old(self, cmd, env=None, in_data=None, timeout=None, user=None):
        """Execute `cmd` as `user` (with `env`) in the instance and returns"""
        logger.debug("(SContainer._exec_run) BEGIN")
        scmd = self._prep_scmd(cmd, env, user, self._ssh)
        p = Popen(scmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT,
                  executable="/bin/bash")
        if in_data is not None:
            p.stdin.write(BYTES(in_data))
        p.stdin.close()
        out = p.stdout.read()
        p.wait(timeout = timeout)
        rc = p.returncode
        logger.debug("(SContainer._exec_run) END")
        return (rc, out.decode())

    def exec_run(self, cmd, env=None, user=None):
        return self._exec_run(cmd, env=env, user=user)

    def pipe(self, cmd, content):
        return self._exec_run(cmd, in_data=content)

    def exec_interact(self, cmd):
        """Execute `cmd` interactively with PTY"""
        scmd = self._prep_scmd(cmd, ssh=self._ssh_i)
        return PtyPopen(scmd, shell=True)

    def get_env(self):
        rc, out = self.exec_run("env -0")
        if rc:
            raise RuntimeError(out)
        return dict( s.split('=', 1) for s in out.split('\x00') if s )

    def get_name(self):
        return self.instance

    def get_hostname(self):
        rc, out = self.exec_run("hostname -s")
        return STR(out).strip()

    def get_host(self):
        return self.ssh_host

    def get_aliases(self):
        return self._node_spec.get("aliases", [])

    def get_interfaces(self):
        # Our singularity container always have 1 interface: eth0
        ip_addrs = self.ip_addr
        ip_addr = ip_addrs[0] if ip_addrs else None
        return [ ( "eth0", ip_addr ) ]

    def read_file(self, path):
        rc, out = self.exec_run("cat {}".format(path))
        if rc:
            raise RuntimeError(out)
        return out

    def write_file(self, path, content, user = None):
        cmd = "bash -c 'cat > {}'".format(path)
        rc, out = self._exec_run(cmd, in_data=content, user=user)
        if rc:
            G.write_file_err = (rc, out, cmd, content)

    def start(self):
        logger.debug("starting container: {}".format(self.instance))
        if not os.path.exists(self.img):
            raise RuntimeError("Image '{}' not found".format(self.img))
        if self._ip_addr and self._net:
            ip_addr = "{}/{}".format(self._ip_addr, self._net.prefixlen)
        else:
            ip_addr = "NONE"
        binds = []
        ovis_prefix = self._cluster_spec.get("ovis_prefix")
        if ovis_prefix:
            binds.append("{}:/opt/ovis".format(ovis_prefix))
        binds.extend(self._cluster_spec.get("mounts", []))

        # prep bind paths in the image
        for s in binds:
            s = s.split(':')
            d = self.img + s[1]
            os.makedirs(d, mode=0o755, exist_ok=True)
        cmds = """
            set -e
            umask 0077
            cd {cwd}
            N=$( singularity instance list {self.instance} | wc -l )
            (( N == 1 )) || {{
                echo "{self.instance} is running"
                false
            }}
            singularity instance start -wnfC --hostname {self._hostname} \
                    -B "{binds}" {self.img} {self.instance} \
                    {cluster_spec} {node_spec} {ip_addr}
        """.format(self=self, binds=",".join(binds),
                cwd=os.getcwd(),
                cluster_spec=shlex.quote(json.dumps(self._cluster_spec)),
                node_spec=shlex.quote(json.dumps(self._node_spec)),
                ip_addr=shlex.quote(ip_addr)) \
           .encode()
        sh = self._ssh if self._ssh else "/bin/bash"
        p = run(sh, shell=True, stdout=PIPE, stderr=STDOUT, input=cmds,
                executable="/bin/bash")
        if p.returncode:
            raise RuntimeError(p.stdout)
        p = run(sh, shell=True, stdout=PIPE, stderr=STDOUT, executable="/bin/bash",
                input=BYTES("singularity instance list --json " + self.instance))
        objs = json.loads(STR(p.stdout))
        sing_json = objs["instances"][0]
        for k in self.JSON_KEYS:
            setattr(self, k, sing_json[k])
#        self.set_ip_addr(ip_addr)
        logger.debug("container '{}' started".format(self.instance))

    def stop(self):
        cmd = self._ssh + " singularity instance stop {}".format(self.instance)
        p = run(cmd, shell=True, stdout=PIPE, stderr=STDOUT,
                executable="/bin/bash")
        if p.returncode:
            raise RuntimeError(p.stdout)

    @property
    def is_running(self):
        return self.shell is not None
        #rc, out = self.exec_run("true")
        #return rc == 0

    @cached_property
    def cluster_name(self):
        if self.cluster_spec:
            return self.cluster_spec.get('name')
        return None

    @cached_property
    def cluster_spec(self):
        return self.get_cluster_spec()

    def get_cluster_spec(self):
        j = self.read_file(self.CLUSTER_SPEC_PATH)
        if j:
            return Spec(json.loads(j))
        return None
# -------------------------------------------------------------- SContainer -- #


class SCluster(LDMSDCluster):
    """Virtual cluster using Singularity

    The application (test scripts) should not create SCluster object directly.
    Instead, it should use the following class methods to get the handle object:

    - SCluster.get("CLUSTER_NAME") -> find and return the handle
    - SCluster.create(spec) -> create a new virtual according to `spec`
    - SCluster.get("CLUSTER_NAME", create=True, spec=spec) -> find and return
            the handle; if not found, create a new virtual cluster according to
            `spec`
    """
    def __init__(self, cluster_name):
        self._name = cluster_name
        self._containers = list()
        self._cont_idx = dict()
        self._spec = dict()

    @classmethod
    def _create(cls, spec):
        logger.debug("creating cluster: {}".format(spec["name"]))
        cluster = SCluster(spec["name"])
        cluster._spec = Spec(spec)
        logger.debug("(cluster create) polling existing containers")
        conts = SContainer.list()
        class HeapEntry(Obj):
            def __lt__(self, other):
                return len(self.conts) < len(other.conts)
        # existing containers on each host
        heap = { h.split(':')[0]: HeapEntry(host=h, conts=[]) for h in G.sing_hosts }
        for cont in conts:
            heap[cont.ssh_host].conts.append(cont)
        heap = list(heap.values())
        heapq.heapify(heap)
        # host-nodes assignment, fill the least-busy first
        logger.debug("(cluster create) assigning hosts to containers")
        nodes = deep_copy(spec["nodes"])
        for node in nodes:
            ent = heap[0]
            ent.conts.append(node)
            host, port = (ent.host.split(':', 1) + [ "22" ])[:2]
            node["sing_host"] = host
            node["sing_port"] = port
            # update heap
            heapq.heapreplace(heap, ent)
        # obtain IP address
        logger.debug("(cluster create) allocating IP addresses")
        addrs = G.ip_addr_db.alloc_addr(cluster.name, len(nodes))
        # start the virtual nodes
        logger.debug("(cluster create) starting containers")
        for node, addr in zip(nodes, addrs):
            inst_name = get_instance_name(node, spec)
            cont = SContainer(instance=inst_name,
                              ssh_host=node["sing_host"],
                              ssh_port=node["sing_port"],
                              node_spec=node,
                              ip_addr = ip_address(addr),
                              network = G.ip_addr_db.net,
                              cluster_spec=spec,
                              cluster=cluster)
            cont.start()
            cluster._cont_idx[node["hostname"]] = cont
            cluster._containers.append(cont)
        cluster._containers.sort(key = lambda c: c.name)
        cluster.update_etc_hosts(node_aliases=cluster.node_aliases)
        cluster.make_ssh_id()
        logger.debug("(cluster create) DONE.")
        return cluster

    @classmethod
    def _get(cls, name):
        logger.debug("(cluster get) Getting cluster {}".format(name))
        cluster = SCluster(name)
        logger.debug("(cluster get) listing all containers")
        instances = _inst_list()
        for inst in instances:
            if not inst["instance"].startswith(name + "-"):
                continue
            cont = SContainer(sing_json=inst, ssh_host=inst["ssh_host"],
                       ssh_port=inst["ssh_port"], cluster=cluster)
            cluster._containers.append(cont)
            cluster._cont_idx[cont.hostname] = cont
        cluster._containers.sort(key = lambda c: c.name)
        logger.debug("(cluster get) done listing")
        if not cluster._containers:
            logger.debug("(cluster get) cluster not found")
            raise LookupError("`{}` cluster not found".format(name))
        logger.debug("(cluster get) DONE")
        return cluster

    def remove(self):
        for cont in self.containers:
            cont.stop()
        G.ip_addr_db.free_addr(self.name)

    def get_name(self):
        return self._name

    def get_containers(self, timeout = 10):
        return self._containers

    def get_container(self, name):
        return self._cont_idx.get(name)

    def get_spec(self):
        if self._spec:
            return self._spec
        if not self.containers:
            raise RuntimeError("No containers")
        return self.containers[0].cluster_spec

    def get_node_aliases(self):
        return { n["hostname"]: n.get("aliases", []) \
                        for n in self.spec.get("nodes", []) }

    @classmethod
    def _list(cls):
        conts = SContainer.list()
        clusters = dict()
        for cont in conts:
            cluster_name = cont.cluster_name
            cluster = clusters.setdefault(cluster_name, SCluster(cluster_name))
            cluster._cont_idx[cont.hostname] = cont
            cluster._containers.append(cont)
        clusters = list(clusters.values())
        for cluster in clusters:
            cluster._containers.sort(key = lambda c: c.name)
        return clusters
# ---------------------------------------------------------------- SCluster -- #
