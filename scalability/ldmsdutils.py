#!/usr/bin/python3

import os
import io
import sys
import json
import time
import shutil
import signal
import logging
import subprocess as sp
from collections import namedtuple
from ovis_ldms import ldms
from config import *

from ldmsd.ldmsd_request import LDMSD_Request, LDMSD_Req_Attr

logger = logging.getLogger(__name__)

# --- Derived variables ------------------------------------------------------ #
SRC_DIR = os.path.dirname(os.path.abspath(__file__))

SAMPLERS = [ "{}-{}".format(h, SAMP_BASE_PORT + i) \
                                for h in SAMP_HOSTS \
                                for i in range(SAMP_PER_HOST) ]
N_SAMP = len(SAMPLERS)
SAMPLERS_IDX = { k: i for (k, i) in zip(SAMPLERS, range(N_SAMP)) }

L1_AGGS = [ "{}-{}".format(h, L1_BASE_PORT + i) \
                                for h in L1_HOSTS \
                                for i in range(L1_PER_HOST) ]
N_L1 = len(L1_AGGS)
L1_IDX = { k: i for (k, i) in zip(L1_AGGS, range(N_L1)) }

L2_AGGS = [ "{}-{}".format(h, L2_BASE_PORT + i) \
                                for h in L2_HOSTS \
                                for i in range(L2_PER_HOST) ]
N_L2 = len(L2_AGGS)
L2_IDX = { k: i for (k, i) in zip(L2_AGGS, range(N_L2)) }

L3_AGG = "{}-{}".format(L3_HOST, L3_BASE_PORT)

# Number of daemons by level of aggregation
NDL = [ N_SAMP, N_L1, N_L2, 1 ]

# Number of daemons per host by level of aggregation
NHL = [ SAMP_PER_HOST, L1_PER_HOST, L2_PER_HOST, 1 ]

# List of daemon names by level of aggregation
DL = [ SAMPLERS, L1_AGGS, L2_AGGS, [L3_AGG] ]

# List of hosts by level of aggregation
HL = [ SAMP_HOSTS, L1_HOSTS, L2_HOSTS, [ L3_HOST ] ]

# Base port by level of aggregation
BPL = [ SAMP_BASE_PORT, L1_BASE_PORT, L2_BASE_PORT, L3_BASE_PORT ]


# --- Utility functions ------------------------------------------------------ #

def logging_config():
    logging.basicConfig(
            format="%(asctime)s.%(msecs)d %(threadName)s %(name)s %(levelname)s %(message)s",
            datefmt="%F-%T",
            level = logging.INFO)

def cond_wait(cond_fn, timeout=0, interval=1, cond_name=None):
    """Periodically check `cond_fn()` until it is `True` or timeout"""
    t0 = time.time()
    if not cond_name:
        cond_name = cond_fn.__name__
    while not cond_fn():
        if timeout:
            t1 = time.time()
            if t1 - t0 >= timeout:
                raise TimeoutError("{} timed out".format(cond_name))
        time.sleep(interval)

def ldmsd_env():
    _env = dict(os.environ)
    _env["PATH"] = "{0}/bin:{0}/sbin:{1}".format(OVIS_PREFIX, _env["PATH"])
    _env["PYTHONPATH"] = "{}/lib/python3.6/site-packages:{}"\
                         .format(OVIS_PREFIX, _env["PYTHONPATH"])
    _env["LDMS_AUTH_FILE"] = OVIS_PREFIX + "/etc/ldms/ldmsauth.conf"
    _env["LDMSD_PLUGIN_LIBPATH"] = OVIS_PREFIX + "/lib/ovis-ldms"
    _env["ZAP_LIBPATH"] = OVIS_PREFIX + "/lib/ovis-ldms"
    return _env

def dir_init():
    _dirs = [ "{}/{}".format(_d, MYHOST) for _d in ["conf", "log", "pid", "mon"] ]
    for _d in _dirs:
        os.makedirs(_d, exist_ok=True)

def dir_cleanup():
    _dirs = [ "{}/{}".format(_d, MYHOST) for _d in ["conf", "log", "pid", "mon"] ]
    for _d in _dirs:
        shutil.rmtree(_d, ignore_errors=True)

def sampler_idx(name):
    return SAMPLERS_IDX[name]

def l1_idx(name):
    return L1_IDX[name]

def l2_idx(name):
    return L2_IDX[name]

FULL_STAT_METRICS = [
    "pid", "comm", "state", "ppid", "pgrp", "session", "tty_nr", "tpgid",
    "flags", "minflt", "cminflt", "majflt", "cmajflt", "utime", "stime",
    "cutime", "cstime", "priority", "nice", "num_threads", "itrealvalue",
    "starttime", "vsize", "rss", "rsslim", "startcode", "endcode", "startstack",
    "kstkesp", "kstkeip", "signal", "blocked", "sigignore", "sigcatch", "wchan",
    "nswap", "cnswap", "exit_signal", "processor", "rt_priority", "policy",
    "delayacct_blkio_ticks", "guest_time", "cguest_time", "start_data",
    "end_data", "start_brk", "arg_start", "arg_end", "env_start", "env_end",
    "exit_code",
]
FULL_STAT_METRICS_LEN = len(FULL_STAT_METRICS)
FULL_STAT_METRICS_TYPE = [int] * FULL_STAT_METRICS_LEN
FULL_STAT_METRICS_TYPE[1] = str # comm
FULL_STAT_METRICS_TYPE[2] = str # state
FULL_STAT_METRICS_IDX = { k: i for k, i in zip(FULL_STAT_METRICS, range(FULL_STAT_METRICS_LEN)) }

ProcStatBase = namedtuple("ProcStat", FULL_STAT_METRICS)

class ProcStat(ProcStatBase):
    def __str__(self):
        return " ".join( x for x in self )

# subset of FULL_STAT_METRICS that we are interested in
STAT_METRICS = [
    "state", "minflt", "cminflt", "majflt", "cmajflt", "utime", "stime",
    "cutime", "cstime", "num_threads", "starttime", "vsize", "rss", "rsslim",
    "processor", "rt_priority", "policy", "delayacct_blkio_ticks", "guest_time",
    "cguest_time", "exit_code",
]
STAT_METRICS_IDX = [ FULL_STAT_METRICS_IDX[k] for k in STAT_METRICS ]
MonStatBase = namedtuple("MonStat", [ "ts", "name", "fd" ] + STAT_METRICS)

class MonStat(MonStatBase):
    @classmethod
    def from_LDMSD(cls, ldmsd):
        _ts = time.time()
        _fd = ldmsd.fd_num()
        _stat = ldmsd.stat()
        _args = [ _ts, ldmsd.name, _fd ] + \
                [ FULL_STAT_METRICS_TYPE[i](_stat[i]) \
                                    for i in STAT_METRICS_IDX ]
        return MonStat(*_args)

    @classmethod
    def from_str(cls, _str):
        _args = _str.split(" ")
        _args = [ float(_args[0]), _args[1], int(_args[2]) ] + [
                    FULL_STAT_METRICS_TYPE[i](x) for x, i in \
                            zip(_args[3:], STAT_METRICS_IDX)
                ]
        return MonStat(*_args)

    def __str__(self):
        return " ".join(str(x) for x in self)


class Proc(object):

    def __init__(self, pid_file, host):
        self.pid_file = pid_file
        self._pid = None
        self.host = host

    def _host_check(self):
        if MYHOST != self.host:
            raise RuntimeError("Host mismatch. The process shall only be "
                    "controlled (start/stop/cleanup) or monitored from the "
                    "assigned host.")

    def comm_validate(self, comm):
        raise NotImplementedError()

    def cmdline_validate(self, cmdline):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def stop(self):
        self._host_check()
        _pid = self.getpid()
        if not _pid:
            logger.info("{} not running".format(self.name))
            return # already running
        os.kill(_pid, signal.SIGTERM)

    def cleanup(self):
        raise NotImplementedError()

    def fd_num(self):
        """Get the number of file descriptors"""
        self._host_check()
        if not self.pid:
            return 0
        try:
            return len(os.listdir("/proc/{}/fd".format(self.pid)))
        except:
            self._pid = None
            return 0

    def stat(self):
        """Get a sample of /proc/PID/stat"""
        self._host_check()
        try:
            txt = open("/proc/{}/stat".format(self.pid)).read().strip()
        except:
            self._pid = None
            return [0] * FULL_STAT_METRICS_LEN
        return ProcStat(*txt.split(' '))

    @property
    def pid(self):
        if not self._pid:
            self._pid = self.getpid()
        return self._pid

    def getpid(self):
        self._host_check()
        _pid = None
        if not os.path.exists(self.pid_file):
            return None
        try:
            f = open(self.pid_file)
            _pid = int(f.read())
            f.close()
            _comm = open("/proc/{}/comm".format(_pid)).read().strip()
            _cmdline = open("/proc/{}/cmdline".format(_pid)).read().strip()
            if not self.comm_validate(_comm) or \
                    not self.cmdline_validate(_cmdline):
                logger.warn("stale pidfile: {}".format(self.pid_file))
                return None
        except:
            return None
        return _pid


class LDMSDMon(Proc):
    """LDMSD monitoring daemon handler"""
    def __init__(self):
        _pid_file = "mon/{0}/{0}.pid".format(MYHOST)
        super().__init__(_pid_file, MYHOST)
        self.name = "ldmsd_mon"
        self.mon_file = "mon/{0}/{0}.mon".format(MYHOST)
        self.interval = MON_INTERVAL / 1e6

    def comm_validate(self, comm):
        return comm in [ "ldmsd_mon.py", "python3" ]

    def cmdline_validate(self, cmdline):
        return cmdline.find("ldmsd_mon.py") >= 0

    def start(self):
        self._host_check()
        _pid = self.getpid()
        if _pid:
            logger.info("already running (pid {})".format(_pid))
            return
        _pid = os.fork()
        if not _pid:
            os.setsid()
            sys.stdin.close()
            sys.stdout.close()
            sys.stderr.close()
            os.close(0)
            os.close(1)
            os.close(2)
            self.routine()
        else:
            logger.info("mon pid: {}".format(_pid))
            f = open(self.pid_file, "w")
            print(_pid, file=f)
            f.close()

    def routine(self):
        """This runs forever"""
        self._host_check()
        self.out = open(self.mon_file, "a")
        I = self.interval
        daemons = get_daemons()
        while True:
            for l in daemons:
                m = MonStat.from_LDMSD(l)
                print(str(m), file=self.out)
            self.out.flush()
            t0 = time.time()
            t1 = (t0//I*I) + I
            time.sleep(t1 - t0)

    def cleanup(self):
        self._host_check()
        _pid = self.getpid()
        if _pid:
            raise RuntimeError(
                    "Cannot cleanup, ldmsd_mon is still running (pid: {})" \
                               .format(_pid))
        for f in [ self.pid_file, self.mon_file ]:
            try:
                os.unlink(f)
            except:
                pass


class LDMSD(Proc):
    """LDMS Daemon Handler

    NOTE: Assuming the working directory is WORK_DIR from config.py
    """
    def __init__(self, name):
        self.name = name
        self.host, self.port = name.rsplit("-", 1)
        _pid_file = "pid/{}/{}.pid".format(self.host, self.name)
        super().__init__(_pid_file, self.host)
        self.conf_file = "conf/{}/{}.conf".format(self.host, self.name)
        self.log_file = "log/{}/{}.log".format(self.host, self.name)
        self.log_level = LOG_LEVEL
        self.xprt = XPRT
        self._prdcr = None
        self._expected_dir = None
        self._prdcr_dir = None
        self._conn = None # the LDMS connection

    def getConfig(self):
        """Subclass shall override this"""
        raise NotImplementedError()

    def comm_validate(self, comm):
        return comm == "ldmsd"

    def cmdline_validate(self, cmdline):
        return cmdline.find(self.name) >= 0

    def writeConfig(self):
        """Write configuration to the configuration file"""
        conf_txt = self.getConfig()
        f = open(self.conf_file, "w")
        f.write(conf_txt)
        f.close()

    def getCmdline(self, gdb=False):
        _edir = self.getExpectedDir()
        self.mem_opt = " -m {} ".format(MEM_PER_SET*len(_edir))
        cmd = "{gdb} " \
              "ldmsd {fg} -c {conf_file} -r {pid_file} -t -l {log_file} " \
              "-v {log_level} {mem_opt}" \
              .format(gdb = "gdb --args" if gdb else "",
                      fg = "-F" if gdb else "",
                      **vars(self))
        return cmd

    def start(self):
        self._host_check()
        _pid = self.getpid()
        if _pid:
            logger.info("{} already running (pid {})".format(self.name, _pid))
            return # already running
        self.writeConfig()
        cmd = self.getCmdline()
        sp.run(cmd, shell=True, executable="/bin/bash") # ldmsd will daemonize

    def cleanup(self):
        self._host_check()
        _pid = self.getpid()
        if _pid:
            raise RuntimeError("Cannot cleanup, {} is running (pid: {})"\
                               .format(self.name, _pid))
        for f in [ self.conf_file, self.pid_file, self.log_file ]:
            try:
                os.unlink(f)
            except:
                pass
        if self.host == L3_HOST and int(self.port) == int(L3_BASE_PORT):
            # This is L3, also remove sos
            if L3_STORE_ROOT:
                shutil.rmtree(L3_STORE_ROOT + "/test", ignore_errors = True)

    def getPrdcrDir(self, prdcr_cls=None):
        """Expected dir results from the producers"""
        if not self._prdcr_dir:
            _ret = list()
            for prdcr in self.getPrdcr():
                l = prdcr_cls(prdcr)
                _ret.extend(l.getExpectedDir())
            self._prdcr_dir = _ret
        return self._prdcr_dir

    def getExpectedDir(self):
        """Returns expected dir result of the daemon"""
        raise NotImplementedError()

    def getMissingDir(self, _dir=None):
        """Return expected dirs that is not in `_dir`.

        Perform self.dir() if _dir is None
        """
        _ldir = _dir if _dir else self.dir()
        _ldir = set( d.name for d in _ldir )
        _edir = set(self.getExpectedDir())
        _missing = _edir - _ldir
        return _missing

    def getPrdcr(self):
        """Returns a list of producers this daemon connects to"""
        if not self.agg_level:
            return []
        if self._prdcr:
            return self._prdcr
        N_DIST = NDL[self.agg_level-1] // NDL[self.agg_level]
        _names = DL[self.agg_level-1]
        _off = self.idx * N_DIST
        self._prdcr = [_names[_off + i] for i in range(N_DIST)]
        return self._prdcr

    @classmethod
    def allDaemons(cls):
        return [ cls("{}-{}".format(host, BPL[cls.agg_level] + i)) \
                                      for host in HL[cls.agg_level] \
                                      for i in range(NHL[cls.agg_level]) ]

    def connect(self):
        if self._conn:
            self._conn.close()
        self._conn = ldms.Xprt(self.xprt)
        self._conn.connect(host=self.host, port=self.port)

    def disconnect(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def getMaxRecvLen(self):
        # to make it work with LDMSD_Request
        return self._conn.msg_max

    def send_command(self, cmd):
        # to make it work with LDMSD_Request
        rc = self._conn.send(cmd)
        if rc != None:
            raise RuntimeError("Failed to send the command. %s" % os.strerror(rc))

    def receive_response(self, recv_len = None):
        # to make it work with LDMSD_Request
        return self._conn.recv()

    def req(self, cmd_line):
        """Form LDMSD_Request according to `cmd_line` and send.

        Returns the response received from the daemon.
        """
        if not self._conn:
            raise RuntimeError("Error: no LDMS connection")
        verb, args = ( cmd_line.split(" ", 1) + [None] )[:2]
        attr_list = []
        if args:
            attr_s = []
            attr_str_list = args.split()
            for attr_str in attr_str_list:
                name = None
                value = None
                [name, value] = (attr_str.split("=", 1) + [None])[:2]
                if (verb == "config" and name != "name") or (verb == "env"):
                    attr_s.append(attr_str)
                elif (verb == "auth_add" and name not in ["name", "plugin"]):
                    attr_s.append(attr_str)
                else:
                    try:
                        attr = LDMSD_Req_Attr(value = value, attr_name = name)
                    except KeyError:
                        attr_s.append(attr_str)
                    except Exception:
                        raise
                    else:
                        attr_list.append(attr)
            if len(attr_s) > 0:
                attr_str = " ".join(attr_s)
                attr = LDMSD_Req_Attr(value = attr_str, attr_id = LDMSD_Req_Attr.STRING)
                attr_list.append(attr)
        request = LDMSD_Request(command = verb, attrs = attr_list)
        request.send(self)
        resp = request.receive(self)
        return resp

    def prdcr_status(self):
        resp = self.req("prdcr_status")
        prdcrs = json.loads(resp['msg'])
        for p in prdcrs:
            sets = p['sets']
            prdcr_set_states = ['START', 'LOOKUP', 'READY', 'UPDATING', 'DELETED']
            counts = { k: 0 for k in prdcr_set_states }
            for s in sets:
                counts[s['state']] += 1
            p['set_state_summary'] = counts
        return prdcrs

    def prdcr_set_status(self):
        resp = self.req("prdcr_set_status")
        sets = json.loads(resp['msg'])
        prdcr_set_states = ['START', 'LOOKUP', 'READY', 'UPDATING', 'DELETED']
        counts = { k: 0 for k in prdcr_set_states }
        for s in sets:
            counts[s['state']] += 1
        return { 'sets': sets, 'set_state_summary': counts }

    def dir(self):
        self.t0 = time.time()
        if not self._conn:
            self.connect()
        self._dirs = self._conn.dir()
        self.t1 = time.time()
        return self._dirs

    def lookup(self, names=None):
        self.t0 = time.time()
        if not self._conn:
            self.connect()
        if type(names) == list:
            _list = names
        elif type(names) == str:
            _list = [names]
        elif names is None and self._dirs:
            _list = [ d.name for d in self._dirs ]
        else:
            raise ValueError("`names` is required")
        self._sets = [self._conn.lookup(s) for s in _list]
        self.t1 = time.time()
        return self._sets

    def update(self, sets=None):
        self.t0 = time.time()
        _sets = sets
        if not _sets:
            _sets = self._sets
        for s in _sets:
            s.update()
        self.t1 = time.time()

    def min_ts(self):
        t = self._sets[0].transaction_timestamp
        _min = t['sec'] + t['usec']*1e-6
        for s in self._sets:
            t = s.transaction_timestamp
            t = t['sec'] + t['usec']*1e-6
            if t < _min:
                _min = t
        return _min

    def max_ts(self):
        t = self._sets[0].transaction_timestamp
        _max = t['sec'] + t['usec']*1e-6
        for s in self._sets:
            t = s.transaction_timestamp
            t = t['sec'] + t['usec']*1e-6
            if t > _max:
                _max = t
        return _max

    def wait_set_removed(self, rm_sets=None, timeout=0, interval=10):
        logger.info("{}: waiting for set removal".format(self.name))
        if rm_sets:
            s = set(rm_sets)
            cond = lambda: not set(d.name for d in self.dir()).intersection(s)
        else:
            cond = lambda: len(self.dir()) < len(self.getExpectedDir())
        cond_wait(cond, timeout = timeout, interval = interval,
                  cond_name = "{} (some) sets removed".format(self.name))

    def wait_set_restored(self, timeout=0, interval=10):
        logger.info("{}: waiting for set restoration".format(self.name))
        cond_wait(lambda: len(self.dir()) == len(self.getExpectedDir()),
                  timeout=timeout,
                  interval=interval,
                  cond_name = "{} sets restored".format(self.name))


LISTEN_CFG_TMP = """\
listen host={host} port={port} xprt={xprt}
"""

SAMPLER_CFG_TMP = LISTEN_CFG_TMP + """
load name=test_sampler
config name=test_sampler action=add_schema schema=test num_metrics=2 type=U64
""" + \
"".join("""\
config name=test_sampler action=add_set instance={{name}}/set_{} schema=test component_id={{id}}
""".format(i) for i in range(0, SETS_PER_SAMP)) \
+ \
"""
start name=test_sampler interval=1000000 offset=0
"""

class LDMSDSampler(LDMSD):
    agg_level = 0

    def __init__(self, name):
        super().__init__(name)
        self.idx = SAMPLERS_IDX[name]
        self.id = self.idx + 1 # comp_id is idx + 1

    def getConfig(self):
        return SAMPLER_CFG_TMP.format(**vars(self))

    def getExpectedDir(self):
        """Returns expected dir result of the daemon"""
        if not self._expected_dir:
            self._expected_dir = [ "{}/set_{}".format(self.name, i) for i in range(SETS_PER_SAMP) ]
        return self._expected_dir


PRDCR_ADD_TMP = """\
prdcr_add name={name} host={host} port={port} xprt={xprt} type=active interval={interval}
"""

PRDCR_START_REGEX = """\
prdcr_start_regex regex=.*
"""

UPDTR_ADD_START_TMP = """
updtr_add name=all interval={interval} offset={offset}
updtr_prdcr_add name=all regex=.*
updtr_start name=all
"""

class LDMSD_L1(LDMSD):
    agg_level = 1

    def __init__(self, name):
        super().__init__(name)
        self.idx = L1_IDX[name]

    def getConfig(self):
        _tmp = io.StringIO()
        _tmp.write(LISTEN_CFG_TMP.format(**vars(self)))
        for name in self.getPrdcr():
            host, port = name.rsplit("-", 1)
            xprt = XPRT
            interval = CONN_INTERVAL
            _tmp.write(PRDCR_ADD_TMP.format(**locals()))
        _tmp.write(PRDCR_START_REGEX)
        interval = SAMP_INTERVAL
        offset = int(SAMP_INTERVAL/10)
        _tmp.write(UPDTR_ADD_START_TMP.format(**locals()))
        return _tmp.getvalue()

    def getExpectedDir(self):
        """Returns expected dir result of the daemon"""
        return self.getPrdcrDir(prdcr_cls = LDMSDSampler)


class LDMSD_L2(LDMSD):
    agg_level = 2
    def __init__(self, name):
        super().__init__(name)
        self.idx = L2_IDX[name]

    def getConfig(self):
        _tmp = io.StringIO()
        _tmp.write(LISTEN_CFG_TMP.format(**vars(self)))
        for name in self.getPrdcr():
            host, port = name.rsplit("-", 1)
            xprt = XPRT
            interval = CONN_INTERVAL
            _tmp.write(PRDCR_ADD_TMP.format(**locals()))
        _tmp.write(PRDCR_START_REGEX)
        interval = SAMP_INTERVAL
        offset = int(2.5 * SAMP_INTERVAL/10)
        _tmp.write(UPDTR_ADD_START_TMP.format(**locals()))
        return _tmp.getvalue()

    def getExpectedDir(self):
        """Returns expected dir result of the daemon"""
        return self.getPrdcrDir(prdcr_cls = LDMSD_L1)


L3_STORE_CFG_TMP = """
load name=store_sos
config name=store_sos path={store_root}

strgp_add name=test_sos plugin=store_sos container=test schema=test
strgp_prdcr_add name=test_sos regex=.*
strgp_start name=test_sos
"""

class LDMSD_L3(LDMSD):
    agg_level = 3
    def __init__(self, name):
        super().__init__(name)
        self.idx = 0

    def getConfig(self):
        _tmp = io.StringIO()
        _tmp.write(LISTEN_CFG_TMP.format(**vars(self)))
        for name in L2_AGGS:
            host, port = name.rsplit("-", 1)
            xprt = XPRT
            interval = CONN_INTERVAL
            _tmp.write(PRDCR_ADD_TMP.format(**locals()))
        _tmp.write(PRDCR_START_REGEX)
        # strgp
        if L3_STORE_ROOT:
            store_root = L3_STORE_ROOT
            _tmp.write(L3_STORE_CFG_TMP.format(**locals()))
        # updtr
        interval = SAMP_INTERVAL
        offset = int(4.5 * SAMP_INTERVAL/10)
        _tmp.write(UPDTR_ADD_START_TMP.format(**locals()))
        return _tmp.getvalue()

    def getExpectedDir(self):
        """Returns expected dir result of the daemon"""
        return self.getPrdcrDir(prdcr_cls = LDMSD_L2)


def get_daemons(fltr = None):
    """Get handlers of daemons on MYHOST that satisfy `fltr()`"""
    daemons = list()
    if MYHOST in SAMP_HOSTS:
        daemons += [ LDMSDSampler("{}-{}".format(MYHOST, SAMP_BASE_PORT + i)) \
                                      for i in range(SAMP_PER_HOST) ]
    if MYHOST in L1_HOSTS:
        daemons += [ LDMSD_L1("{}-{}".format(MYHOST, L1_BASE_PORT + i)) \
                                      for i in range(L1_PER_HOST) ]

    if MYHOST in L2_HOSTS:
        daemons += [ LDMSD_L2("{}-{}".format(MYHOST, L2_BASE_PORT + i)) \
                                      for i in range(L2_PER_HOST) ]

    if MYHOST == L3_HOST:
        daemons.append(LDMSD_L3("{}-{}".format(MYHOST, L3_BASE_PORT)))
    if fltr:
        daemons = list(filter(fltr, daemons))
    return daemons

class Conn(object):
    """Utility class wrapping ldms.Xprt"""
    def __init__(self, name, level):
        _host, _port = name.rsplit("-", 1)
        self.name = name
        self.host = _host
        self.port = int(_port)
        self.level = level
        self.xprt = ldms.Xprt(name = XPRT)

    def connect(self):
        return self.xprt.connect(host=self.host, port=self.port)

    def dir(self):
        return self.xprt.dir()

    @classmethod
    def getAggXprt(cls):
        """Get transports for all L1, L2 and L3 aggregators"""
        _list = list()
        # L1
        _list.extend( cls(name, 1) for name in L1_AGGS )
        # L2
        _list.extend( cls(name, 2) for name in L2_AGGS )
        # L3
        _list.append( cls(L3_AGG, 3) )
        return _list

class Control(object):
    """Routines to control daemons in the participating hosts"""
    def __init__(self):
        samps = LDMSDSampler.allDaemons()
        l1_aggs = LDMSD_L1.allDaemons()
        l2_aggs = LDMSD_L2.allDaemons()
        l3_aggs = LDMSD_L3.allDaemons()
        self.ldmsd_list = samps + l1_aggs + l2_aggs + l3_aggs
        self.ldmsd_by_name = { d.name: d for d in self.ldmsd_list }
        self.ldmsd_by_lvl = [ samps, l1_aggs, l2_aggs, l3_aggs ]

    def _ctrl(self, cmd, names):
        if names:
            daemons = [ self.ldmsd_by_name[s] for s in names ]
        else:
            daemons = self.ldmsd_list
        host_list = dict() # list of daemons by host
        for d in daemons:
            l = host_list.setdefault(d.host, list())
            l.append(d)
        procs = list()
        for h, l in host_list.items():
            sio = io.StringIO()
            if h != MYHOST:
                _ssh_port = " -p {} ".format(SSH_PORT)
                sio.write("ssh " + _ssh_port + h + " ")
            sio.write(SRC_DIR + "/ldmsd_ctl.py " + cmd)
            if names:
                for d in l:
                    sio.write(" " + d.name)
            _cmd = sio.getvalue()
            p = sp.run(_cmd, shell=True, executable="/bin/bash",
                        stdout=sp.PIPE, stderr=sp.PIPE)
            if p.returncode:
                raise RuntimeError("cmd `{}` error, rc: {}" \
                                   .format(_cmd, p.returncode))
            procs.append(p)
        return procs

    def start(self, names=None, timeout=-1):
        """Start daemons in the list of `names` (ALL if names=None)"""
        self._ctrl("start", names)
        if timeout < 0:
            return
        def cond():
            return 0 == len([ v for v in self.status(names).values() if not v ])
        cond_wait(cond, timeout=timeout, cond_name="ldmsds started")


    def stop(self, names=None, timeout=-1):
        """Stop daemons in the list of `names` (ALL if names=None)"""
        self._ctrl("stop", names)
        if timeout < 0:
            return
        def cond():
            return 0 == len([ v for v in self.status(names).values() if v ])
        cond_wait(cond, timeout=timeout, cond_name="ldmsds stopped")

    def status(self, names=None):
        """Get status of the daemons in the list of `names` (ALL if names=None)"""
        procs = self._ctrl("-jl status", names)
        coll = dict()
        for p in procs:
            obj = json.loads( p.stdout.decode() )
            coll.update(obj)
        return coll
