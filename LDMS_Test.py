import os
import re
import io
import pdb
import sys
import pwd
import json
import time
import shlex
import errno
import socket
import signal
import subprocess as sp
import logging

from collections import namedtuple
from dataclasses import dataclass, field

import fcntl
from array import array
import struct
import ipaddress

import TADA

from abc import abstractmethod, ABC
from io import StringIO
from distutils.spawn import find_executable
from configparser import ConfigParser, ExtendedInterpolation

from functools import reduce

# `D` Debug object to store values for debugging
class Debug(object): pass
D = Debug()


# `G` being the convenient global object holder
class Global(object): pass
G = Global()

logger = logging.getLogger()

#############
#  Helpers  #
#############

class cached_property(object):
    """`@cached_property` decorator to make a cached property (getter-only)

    NOTE: The property's value is stored in `self.__cache__` dictionary.
    """
    def __init__(self, func):
        self.func = func
        self.name = func.__name__

    def __get__(self, obj, _type):
        cache = getattr(obj, "__cache__", dict())
        if not cache: # newly created cache
            obj.__cache__ = cache
        try:
            return cache[self.name]
        except KeyError:
            cache[self.name] = val = self.func(obj)
            return val

_PRE_META_KV = r'(?:^(?P<pre_meta_key>.*\w)\s+:\s+(?P<pre_meta_val>.*)$)'
_META_BEGIN = r'(?P<meta_begin>(?:Schema Digest\s+)?Schema\s+Instance\s+Flags.*\s+Info)'
_META_DASHES = r'(?:^(?P<meta_dashes>[ -]+)$)'
_META_SUMMARY = '(?:'+ \
                r'Total Sets: (?P<meta_total_sets>\d+), ' + \
                r'Meta Data \(kB\):? (?P<meta_sz>\d+(?:\.\d+)?), ' + \
                r'Data \(kB\):? (?P<data_sz>\d+(?:\.\d+)?), ' + \
                r'Memory \(kB\):? (?P<mem_sz>\d+(?:\.\d+)?)' + \
                ')'
_META_DATA = r'(?:' + \
             r'(?:(?P<meta_schema_digest>[0-9A-Fa-f]+)\s+)?' + \
             r'(?P<meta_schema>\S+)\s+' + \
             r'(?P<meta_inst>\S+)\s+' + \
             r'(?P<meta_flags>\D+)\s+' + \
             r'(?P<meta_msize>\d+)\s+' + \
             r'(?P<meta_dsize>\d+)\s+' + \
             r'(?P<meta_hsize>\d+)\s+' + \
             r'(?P<meta_uid>\d+)\s+' + \
             r'(?P<meta_gid>\d+)\s+' + \
             r'(?P<meta_perm>-(?:[r-][w-][x-]){3})\s+' + \
             r'(?P<meta_update>\d+\.\d+)\s+' + \
             r'(?P<meta_duration>\d+\.\d+)' + \
             r'(?:\s+(?P<meta_info>.*))?' + \
             r')'
_META_END = r'(?:^(?P<meta_end>[=]+)$)'
_LS_L_HDR = r'(?:(?P<set_name>[^:]+): .* last update: (?P<ts>.*))'
_LS_L_DATA = r'(?:(?P<F>.) (?P<type>\S+)\s+(?P<metric_name>\S+)\s+' \
             r'(?P<metric_value>.*))'
_LS_RE = re.compile(
            _PRE_META_KV + "|" +
            _META_BEGIN + "|" +
            _META_DASHES + "|" +
            _META_DATA + "|" +
            _META_SUMMARY + "|" +
            _META_END + "|" +
            _LS_L_HDR + "|" +
            _LS_L_DATA
         )
def int0(s):
    return int(s, base=0)
_TYPE_FN = {
    "char": lambda x: str(x).strip("'"),
    "char[]": lambda x: str(x).strip('"'),

    "u8": int0,
    "s8": int0,
    "u16": int0,
    "s16": int0,
    "u32": int0,
    "s32": int0,
    "u64": int0,
    "s64": int0,
    "f32": float,
    "d64": float,

    "u8[]": lambda x: list(map(int0, x.split(','))),
    "s8[]": lambda x: list(map(int0, x.split(','))),
    "u16[]": lambda x: list(map(int0, x.split(','))),
    "s16[]": lambda x: list(map(int0, x.split(','))),
    "u32[]": lambda x: list(map(int0, x.split(','))),
    "s32[]": lambda x: list(map(int0, x.split(','))),
    "u64[]": lambda x: list(map(int0, x.split(','))),
    "s64[]": lambda x: list(map(int0, x.split(','))),
    "f32[]": lambda x: list(map(float, x.split(','))),
    "d64[]": lambda x: list(map(float, x.split(','))),
}

def parse_ldms_ls(txt):
    """Parse output of `ldms_ls -l [-v]` into { SET_NAME : SET_DICT } dict

    Each SET_DICT is {
        "name" : SET_NAME,
        "ts" : UPDATE_TIMESTAMP_STR,
        "meta" : {
            "schema"    :  SCHEMA_NAME,
            "instance"  :  INSTANCE_NAME,
            "flags"     :  FLAGS,
            "meta_sz"   :  META_SZ,
            "data_sz"   :  DATA_SZ,
            "heap_sz"   :  HEAP_SZ,
            "uid"       :  UID,
            "gid"       :  GID,
            "perm"      :  PERM,
            "update"    :  UPDATE_TIME,
            "duration"  :  UPDATE_DURATION,
            "info"      :  APP_INFO,
        },
        "data" : {
            METRIC_NAME : METRIC_VALUE,
            ...
        },
        "data_type" : {
            METRIC_NAME : METRIC_TYPE,
        },
    }
    """
    ret = dict()
    lines = txt.splitlines()
    D.txt = txt
    D.lines = lines
    itr = iter(lines)
    section = 0 # 0-pre_meta, 1-meta, 2-data
    for l in itr:
        l = l.strip()
        if not l: # empty line, end of set
            lset = None
            data = None
            meta = None
            continue
        m = _LS_RE.match(l)
        if not m:
            D.l = l
            raise RuntimeError("Bad line format: {}".format(l))
        m = m.groupdict()
        if m["meta_begin"]: # start meta section
            if section != 0:
                raise RuntimeError("Unexpected meta info: {}".format(l))
            section = 1
            continue
        elif m["meta_schema"]: # meta data
            if section != 1:
                raise RuntimeError("Unexpected meta info: {}".format(l))
            meta = dict( schema_digest = m.get("meta_schema_digest", ""),
                         schema = m["meta_schema"],
                         instance = m["meta_inst"],
                         flags = m["meta_flags"],
                         meta_sz = m["meta_msize"],
                         data_sz = m["meta_dsize"],
                         heap_sz = m["meta_hsize"],
                         uid = m["meta_uid"],
                         gid = m["meta_gid"],
                         perm = m["meta_perm"],
                         update = m["meta_update"],
                         duration = m["meta_duration"],
                         info = m["meta_info"],
                    )
            _set = ret.setdefault(m["meta_inst"], dict())
            _set["meta"] = meta
            _set["name"] = m["meta_inst"]
        elif m["meta_total_sets"]: # the summary line
            if section != 1:
                raise RuntimeError("Unexpected meta info: {}".format(l))
            # else do nothing
            continue
        elif m["meta_dashes"]: # dashes
            if section != 1:
                raise RuntimeError("Unexpected meta info: {}".format(l))
            continue
        elif m["meta_end"]: # end meta section
            if section != 1:
                raise RuntimeError("Unexpected meta info: {}".format(l))
            section = 2
            continue
        elif m["set_name"]: # new set
            if section == 0: # we go straight into data (i.e. no -v or -vv)
                section = 2
            if section != 2:
                raise RuntimeError("Unexpected data info: {}".format(l))
            data = dict() # placeholder for metric data
            data_type = dict() # placeholder for metric data type
            lset = ret.setdefault(m["set_name"], dict())
            lset["name"] = m["set_name"]
            lset["ts"] = m["ts"]
            lset["data"] = data
            lset["data_type"] = data_type
        elif m["metric_name"]: # data
            if section != 2:
                raise RuntimeError("Unexpected data info: {}".format(l))
            if m["type"] == "char[]":
                _val = m["metric_value"]
            else:
                _val = m["metric_value"].split(' ', 1)[0] # remove units
            mname = m["metric_name"]
            mtype = m["type"]
            data[mname] = _TYPE_FN[mtype](_val)
            data_type[mname] = mtype
        elif m["pre_meta_key"]: # pre-meta (host name stuff)
            if section != 0:
                raise RuntimeError("Unexpected pre-meta info: {}".format(l))
            continue # ignore
        else:
            raise RuntimeError("Unable to process line: {}".format(l))
    return ret

def create_suite_from_C_test_results(txt, tada_addr):
    import json

    r = json.loads(txt)
    cnt = len(r)

    test = TADA.Test(test_suite = r[0]["test-suite"],
                     test_type = r[0]["test-type"],
                     test_name = r[0]["test-name"],
                     test_desc = r[0]["test-desc"],
                     test_user = r[0]["test-user"],
                     commit_id = r[0]["commit-id"],
                     tada_addr = tada_addr)

    for msg in r:
        if msg["msg-type"] == "assert-status":
            test.add_assertion(msg["assert-no"], msg["assert-desc"])

    test.start()

    try:
        for msg in r:
            if msg["msg-type"] == "assert-status":
                result = True if (msg["test-status"] == "passed") else False
                test.assert_test(msg["assert-no"], result, msg["assert-cond"])
    except:
        test.finish()
        raise

    test.finish()
    return test.exit_code()

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

def jprint(obj):
    """Pretty print JSON object"""
    print(json.dumps(obj, indent=2))

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
    return "NONE"

def guess_ovis_prefix():
    """Guess ovis prefix from the environment"""
    sbin_ldmsd = find_executable("ldmsd")
    if sbin_ldmsd:
        prefix, a, b = sbin_ldmsd.rsplit('/', 2)
    else:
        prefix = "/opt/ovis"
    return prefix

ADDR_RE = re.compile(r'^(?P<addr>[^:]+)(?:[:](?P<port>\d+))?$')
def tada_addr(s):
    m = ADDR_RE.match(s)
    if not m:
        raise ValueError("Bad address format")
    m = m.groupdict()
    if m["port"]:
        return s # already has port specified
    else:
        return s + ":9862" # default port

def get_cluster_name(parsed_args):
    """Derive `clustername` from the parsed CLI arguments"""
    if parsed_args.clustername:
        return parsed_args.clustername
    uname = parsed_args.user
    test = os.path.basename(sys.argv[0])
    if test.endswith('.py'):
        test = test[:-3]
    commit_id = get_ovis_commit_id(parsed_args.prefix)
    parsed_args.clustername = "{}-{}-{:.7}".format(uname, test, commit_id)
    return parsed_args.clustername

def add_common_args(parser):
    """Add common arguments for test scripts"""
    G.parser = parser # global parser
    _USER = pwd.getpwuid(os.geteuid())[0]
    parser.add_argument("--config", type = str, help = "The configuration file")
    parser.add_argument("--runtime", type = str, default="docker",
            help = "The runtime plugin (default: docker).")
    parser.add_argument("--clustername", type = str,
            help = "The name of the cluster. The default is "
            "{USER}-{TEST_NAME}-{COMMIT_ID}.")
    parser.add_argument("--user", default = _USER,
            help = "Specify the user who run the test.")
    parser.add_argument("--prefix", type = str,
            default = guess_ovis_prefix(),
            help = "The OVIS installation path on the host. This will be mounted to /opt/ovis in containers.")
    parser.add_argument("--direct-prefix", type = str,
            default = guess_ovis_prefix(),
            help = "The path of the OVIS binaries installation path for the host.")
    parser.add_argument("--src", type = str,
            help = "The path to OVIS source tree (for gdb). " \
            "If not specified, src tree won't be mounted.")
    parser.add_argument("--data_root", "--data-root", type = str,
            help = "The path to host db directory. The default is "
                   "'/home/{user}/db/{clustername}'" )
    parser.add_argument("--tada_addr", "--tada-addr", type=tada_addr,
            help="The test automation server host and port as host:port.",
            default="tada-host:9862")
    parser.add_argument("--debug", default=0, type=int,
            help="Turn on TADA.DEBUG flag.")
    parser.add_argument("--mount", action="append",
            metavar = "SRC:DST[:MODE]", default = [],
            help="Add additional mount point to the container. "
                 "SRC is the path on the host. DST is the path in the "
                 "container. MODE can be `ro` or `rw`. If MODE is not given, "
                 "the default is `rw`. Example: --mount /mnt/abc:/home:ro."
        )
    parser.add_argument("--image", type = str, default="ovis-centos-build",
            help = "The image to run containers with (default: ovis-centos-build)")

def process_config_file(path = None):
    """Process `ldms-test` configuration file"""
    G.conf = conf = ConfigParser(interpolation = ExtendedInterpolation(),
                                 allow_no_value=True)
    # default config
    _USER = pwd.getpwuid(os.geteuid())[0]
    _DEFAULT_CONFIG = {
            "ldms-test": {
                "prefix": guess_ovis_prefix(),
                "direct_prefix": guess_ovis_prefix(),
                "runtime": "docker",
                "clustername": None,
                "user": _USER,
                "src": None,
                "data_root": None,
                "tada_addr": "tada-host:9862",
                "debug": 0,
                "mount": "",
                "image": "ovis-centos-build",
                },
            "docker": {
                },
            "singularity": {
                "hosts": "localhost",
                },
            }
    conf.read_dict(_DEFAULT_CONFIG)
    if not path: # config not spefified, check default location
        path = sys.path[0]+"/ldms-test.conf"
        if not os.path.exists(path):
            return conf
    conf.read(path)
    if conf.has_section("ldms-test") and hasattr(G, "parser"):
        # update args defaults with values from config
        sect = conf["ldms-test"]
        keys = set(sect).intersection(G.args.__dict__)
        df = { k: sect[k] for k in keys }
        df["mount"] = bash_items(df["mount"])
        G.parser.set_defaults( **df )
        # re-parse args
        args = G.parser.parse_args()
        G.args.__dict__.update(args.__dict__)
    return conf

# Process the default config file at load
process_config_file()

def process_args(parsed_args):
    """Further process the parsed common arguments"""
    G.args = parsed_args
    args = parsed_args
    process_config_file(args.config)
    args.clustername = get_cluster_name(args)
    if not args.data_root:
        args.data_root = os.path.expanduser("~{a.user}/db/{a.clustername}".format(a = args))
    if not os.path.exists(args.data_root):
        os.makedirs(args.data_root)
    args.commit_id = get_ovis_commit_id(args.prefix)
    # then, apply the final args values to the ldms-test config section
    for k, v in G.args.__dict__.items():
        if type(v) == list:
            v = "\n".join(v)
        G.conf.set("ldms-test", k, str(v))
    # Then, let the runtime environment module process the configuration as well
    from importlib import import_module
    m = import_module("runtime." + G.args.runtime)
    m.process_config(G.conf)
    if args.debug:
        TADA.DEBUG = True

DEEP_COPY_TBL = {
        dict: lambda x: { k:deep_copy(v) for k,v in x.items() },
        list: lambda x: [ deep_copy(v) for v in x ],
        tuple: lambda x: tuple( deep_copy(v) for v in x ),
        int: lambda x: x,
        float: lambda x: x,
        str: lambda x: x,
        bool: lambda x: x,
    }

def deep_copy(obj):
    t = type(obj)
    f = DEEP_COPY_TBL.get(t)
    if not f:
        raise TypeError("Unsupported type: {.__name__}".format(t))
    return f(obj)

def debug_prompt(prompt = "Press ENTER to continue or Ctrl-C to debug"):
    """Display a given prompt if DEBUG mode and interactive flag are on"""
    if sys.flags.interactive and TADA.DEBUG:
        if sys.version_info.major == 3: # python3
            return input(prompt)
        elif sys.version_info.major == 2: # python2
            return raw_input(prompt)
        else:
            raise RuntimeError("Unknown python version: {}" \
                               .format(sys.version_info.major))

def bash_items(exp):
    """List of str from `exp` expanded under bash

    Parameters
    ----------
    exp: a str of expression to be expanded

    Returns
    -------
    list(str): a list of str from the expansion

    Examples
    --------
    >>> bash_items('''
    ... a{1..5}
    ... b{3,5,7} "item with space"
    ... x y z
    ... ''')
    ['a1', 'a2', 'a3', 'a4', 'a5', 'b3', 'b5', 'b7', 'item with space', 'x', 'y', 'z']
    >>>
    """
    cmd = """A=({})
             for ((I=0;I<${{#A[*]}};I++)); do
                 echo ${{A[$I]}}
             done
    """.format(exp)
    out = sp.check_output(cmd, shell=True, executable="/bin/bash")
    return out.decode().splitlines()

class Spec(dict):
    """Spec object -- handling spec object extension and substitution

    Synopsis:
    >>> spec_def = {...} # see example below
    >>> spec = Spec(spec_def)

    A Spec is a dictionary with a reserved top-level "templates" attribute for
    defining object (dict) templates. The objects in the spec can extend
    a template using a reserved attribute "!extends".  The local attributes
    override those from the template. The "%VAR%" in the string inside the Spec
    is substituted with the value of the attribute of the nearest parent object
    (self is the nearest).

    The substitution is done after the recursive extension is done.

    For example,

    {
        "USER": "root",
        "templates": {
            "prog-base": {
                "path": "/bin/%prog%",
                "desc": "This is %prog%",
                "user": "%USER%",
            },
            "prog-sbin": {
                "!extends": "prog-base",
                "path": "/sbin/%prog%",
            },
        },
        "programs": [
            {
                "prog": "ssh",
                "!extends": "prog-base",
            },
            {
                "prog": "sshd",
                "!extends": "prog-sbin",
            },
        ],
        "jail": {
            "USER": "jail",
            "programs": [
                {
                    "prog": "ls",
                    "!extends": "prog-base",
                },
            ],
        },
    }

    will be extended and substituted as:

    {
        "USER": "root",
        "templates": {
            "prog-base": {
                "path": "/bin/%prog%",
                "desc": "This is %prog%",
                "user": "%USER%",
            },
            "prog-sbin": {
                "!extends": "prog-base",
                "path": "/sbin/%prog%",
            },
        },
        "programs": [
            {
                "prog": "ssh",
                "path": "/bin/ssh",
                "desc": "This is ssh",
                "user": "root",
            },
            {
                "prog": "sshd",
                "path": "/sbin/sshd",
                "desc": "This is sshd",
                "user": "root",
            },
        ],
        "jail": {
            "USER": "jail",
            "programs": [
                {
                    "prog": "ls",
                    "path": "/bin/ls",
                    "desc": "This is ls",
                    "user": "jail",
                },
            ],
        },
    }

    """
    MAX_DEPTH = 64
    VAR_RE = re.compile(r'%([^%]+)%')
    PRIMITIVES = set([int, float, bool, str])

    def __init__(self, spec):
        _dict = deep_copy(spec)
        self.templates = _dict.get("templates", {})
        super(Spec, self).__init__(_dict)
        self.SUBST_TBL = {
            dict: self._subst_dict,
            list: self._subst_list,
            tuple: self._subst_tuple,
            str: self._subst_str,
            int: self._subst_scalar,
            float: self._subst_scalar,
            bool: self._subst_scalar,
        }
        self.EXPAND_TBL = {
            dict: self._expand_dict,
            list: self._expand_list,
            tuple: self._expand_tuple,
            int: self._expand_scalar,
            float: self._expand_scalar,
            str: self._expand_scalar,
            bool: self._expand_scalar,
        }
        self._start_expand()
        self._start_subst()

    def _start_expand(self):
        """(private) starting point of template expansion"""
        for k,v in self.items():
            if k == "templates":
                continue # skip the templates
            self[k] = self._expand(v, 0)

    def _start_subst(self):
        """(private) starting point of %VAR% substitute"""
        self.VAR = { k:v for k,v in self.items() \
                         if type(v) in self.PRIMITIVES }
        for k,v in self.items():
            if k == "templates":
                continue
            self[k] = self._subst(v)

    def _expand(self, obj, lvl):
        """(private) Expand the "!extends" and "%VAR%" """
        if lvl > self.MAX_DEPTH:
            raise RuntimeError("Expansion exceeding maximum depth ({})" \
                               .format(self.MAX_DEPTH))
        tp = type(obj)
        fn = self.EXPAND_TBL.get(tp)
        if not fn:
            raise TypeError("Unsupported type {.__name__}".format(tp))
        return fn(obj, lvl)

    def _expand_scalar(self, obj, lvl):
        return obj

    def _expand_list(self, lst, lvl):
        return [ self._expand(x, lvl+1) for x in lst ]

    def _expand_tuple(self, tpl, lvl):
        return tuple( self._expand(x, lvl+1) for x in tpl )

    def _expand_dict(self, dct, lvl):
        lst = [dct] # list of extension
        ext = dct.get("!extends")
        while ext:
            _temp = self.templates.get(ext)
            if _temp == None:
                raise KeyError("`{}` template not found".format(ext))
            lst.append(_temp)
            ext = _temp.get("!extends")
        # new temporary dict
        tmp = dict()
        while lst:
            # update dict by extension order, base first
            d = lst.pop()
            tmp.update(d)
        tmp.pop("!extends", None) # remove the "!extends" keyword
        return { k: self._expand(v, lvl+1) for k,v in tmp.items() }

    def _subst(self, obj):
        """(private) substitute %VAR% """
        tp = type(obj)
        fn = self.SUBST_TBL.get(tp)
        if not fn:
            raise TypeError("Unsupported type {.__name__}".format(tp))
        return fn(obj)

    def _subst_scalar(self, val):
        return val

    def _subst_list(self, lst):
        return [ self._subst(x) for x in lst ]

    def _subst_tuple(self, tpl):
        return tuple( self._subst(x) for x in tpl )

    def _subst_dict(self, dct):
        _save = self.VAR
        # new VAR dict
        var = dict(self.VAR)
        var.update( { k:v for k,v in dct.items() \
                          if type(v) in self.PRIMITIVES } )
        self.VAR = var
        _ret = { k: self._subst(v) for k,v in dct.items() }
        # recover
        self.VAR = _save
        return _ret

    def _subst_str(self, val):
        # string substitution may refer to another variable. So, we have to keep
        # substituting it, with a limit, until there are no more variables left.
        count = 0
        s0 = self.VAR_RE.sub(lambda m: str(self.VAR[m.group(1)]), val)
        s1 = self.VAR_RE.sub(lambda m: str(self.VAR[m.group(1)]), s0)
        while s0 != s1:
            if count > 128: # 128 level of substitution should suffice
                raise ValueError("Too many level of substitutions")
            s0 = s1
            s1 = self.VAR_RE.sub(lambda m: str(self.VAR[m.group(1)]), s0)
        return s0

def get_cluster_class():
    from importlib import import_module
    rt = G.conf["ldms-test"]["runtime"]
    rt = "runtime.{}".format(rt)
    rt_mod = import_module(rt)
    return rt_mod.get_cluster_class()

def get_iface_addrs():
    global G
    if hasattr(G, "iface_addrs") and G.iface_addrs:
        return G.iface_addrs
    # Use ioctl to request SIOCGIFCONF on our socket
    SIOCGIFCONF = 0x8912 # from <sys/ioctl.h>
    IF_NAMESIZE = 16 # <net/if.h>
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    buff = array("B", b'\0' * 4096)
    buff_ptr, buff_len = buff.buffer_info()
    ifconf = struct.Struct("iL") # struct ifconf in <net/if.h>
    ifin = ifconf.pack(buff_len, buff_ptr)
    ifout = fcntl.ioctl(sock.fileno(), SIOCGIFCONF, ifin)
    _len, _ptr = ifconf.unpack(ifout)
    D.ifreq = ifreq = struct.Struct(">16sHHL") # <net/if.h>
    iface_addrs = []
    for off in range(0, _len, 40): # sizeof(struct ifreq) == 40
        iface, family, port, addr = ifreq.unpack(buff[off:off+ifreq.size])
        iface = iface.strip(b'\0').decode()
        addr = ipaddress.ip_address(addr)
        iface_addrs.append((iface, addr))
    D.buff = buff
    G.iface_addrs = iface_addrs
    return iface_addrs

def get_local_addrs():
    global G
    if hasattr(G, "local_addrs") and G.local_addrs:
        return G.local_addrs
    G.local_addrs = set( a for i, a in get_iface_addrs() )
    return G.local_addrs

def is_remote(host):
    """Check if `host` is a remote host"""
    addr = socket.gethostbyname(host)
    addr = ipaddress.ip_address(addr)
    return addr not in get_local_addrs()

def BYTES(x):
    if type(x) == bytes:
        return x
    if type(x) == str:
        return x.encode()
    return bytes(x)

def bash(_input):
    """Execute `_input` in bash, and return (rc, output)"""
    p = sp.Popen("/bin/bash", shell=True, stdin=sp.PIPE, stdout=sp.PIPE,
                 stderr=sp.STDOUT)
    # don't use buffer
    p.stdin = p.stdin.detach()
    p.stdout = p.stdout.detach()
    if _input:
        p.stdin.write(BYTES(_input))
        p.stdin.close()
    bio = io.BytesIO()
    while True:
        out = p.stdout.read()
        if len(out) == 0 and p.poll() is not None:
            break
        bio.write(out)
    return (p.returncode, bio.getvalue().decode())

def ssh(host, _input=None, port=22):
    """Similar to $ echo _input | ssh -p port"""
    cmd = "ssh -q -p {} {}".format(port, host)
    p = sp.Popen(cmd, shell=True, stdin=sp.PIPE, stdout=sp.PIPE,
                 stderr=sp.STDOUT)
    # don't use buffer
    p.stdin = p.stdin.detach()
    p.stdout = p.stdout.detach()
    if _input:
        p.stdin.write(BYTES(_input))
        p.stdin.close()
    bio = io.BytesIO()
    while True:
        out = p.stdout.read()
        if len(out) == 0 and p.poll() is not None:
            break
        bio.write(out)
    return (p.returncode, bio.getvalue().decode())

def EXPECT(val, expected):
    if val != expected:
        raise RuntimeError("\n  EXPECTING: {}\n  GOT: {}".format(expected, val))

def pycmd(tty, cmd, retry = 3):
    """cmd must be single command w/o new line"""
    global loggger
    log = logger
    _begin = time.time()
    sio = io.StringIO()
    tty.write(cmd)
    _mark0 = time.time()
    # flush the echo
    while tty.read(idle_timeout = 0.1) != '':
        continue
    _mark1 = time.time()
    # ENTER to execute
    tty.write("\n")
    _mark2 = time.time()
    count = 0
    end = False
    _mark3 = time.time()
    t0 = time.time() # for debugging
    _count = 0 # for debugging
    while count < retry and not end:
        _count += 1
        o = tty.read(idle_timeout=0.1)
        if len(o):
            count = 0 # reset
        else:
            count += 1
        sio.write(o)
        if sio.getvalue().endswith(">>> "):
            t1 = time.time()
            # print(f"HERE; count: {count}; _count: {_count}; dt: {t1 - t0}")
            end = True
            break
    _mark4 = time.time()
    if not end:
        raise RuntimeError("Python '{cmd}` not responding".format(**vars()))
    o = sio.getvalue()
    D.pyout = o
    _end = time.time()
    if False:
        log.info(f"[pycmd] t1-t0: {t1-t0} secs")
        log.info(f"[pycmd] begin-to-end: {_end - _begin} secs")
        log.info(f"[pycmd] _mark0: {_mark0 - _begin:.3f} secs")
        log.info(f"[pycmd] _mark1: {_mark1 - _begin:.3f} secs")
        log.info(f"[pycmd] _mark2: {_mark2 - _begin:.3f} secs")
        log.info(f"[pycmd] _mark3: {_mark3 - _begin:.3f} secs")
        log.info(f"[pycmd] _mark4: {_mark4 - _begin:.3f} secs")
    # remove the echoed cmd and the prompt
    return o[ 2 : -4 ]

def py_pty(node, script_path, user = None):
    global loggger
    log = logger
    as_user = f"as {user}" if user is not None else ""
    log.info(f"starting {script_path} on {node.name} {as_user}")
    _cmd = f"ZAP_POOLS=8 /usr/bin/python3 -i {script_path}"
    if user:
        shell = f"su -s /bin/bash {user}"
    else:
        shell = f"/bin/bash"
    cmd = f"{shell} -c '{_cmd}'"
    _pty = node.exec_interact(cmd)
    time.sleep(2)
    _out = _pty.read()
    EXPECT(_out, ">>> ")
    return _pty

class PyPty(object):
    """PyPty(node, script_path, user = None)

    PTY for Pyton program inside a container"""
    def __init__(self, node, script_path, user = None):
        self.pty = py_pty(node, script_path, user)

    def cmd(self, cmd, retry = 3):
        """Issue a `cmd` to the Python PTY and returns the output"""
        return pycmd(self.pty, cmd, retry)


class StreamData(object):
    """StreamData representation"""

    __slots__ = ('name', 'src', 'tid', 'uid', 'gid', 'perm', 'is_json', 'data')
    __cmp_fields__ = ('name', 'src', 'uid', 'gid', 'perm', 'is_json', 'data')
    # cmp_fields omitted 'tid'

    def __init__(self, *args, **kwargs):
        if args and kwargs:
            raise ValueError("StreamData can be initialized with either *args or **kwargs but not both")
        for k in self.__slots__:
            setattr(self, k, None)
        if args:
            if len(args) != len(self.__slots__):
                raise ValueError()
            for k,v in zip(self.__slots__, args):
                setattr(self, k, v)
        elif kwargs:
            for k, v in kwargs.items():
                setattr(self, k, v)
        else:
            raise ValueError("Missing initialize parameters")

    def as_list(self):
        return [ getattr(self, f) for f in self.__slots__ ]

    def as_tuple(self):
        return tuple( getattr(self, f) for f in self.__slots__ )

    def __eq__(self, other):
        if type(other) != StreamData:
            return False
        # compare all fields, except when it is None
        for k in self.__cmp_fields__:
            v0 = getattr(self, k)
            v1 = getattr(other, k)
            if v0 is None or v1 is None:
                continue # skip
            if v0 != v1:
                return False
        return True

    @classmethod
    def fromRepr(cls, _str):
        if not _str:
            return None
        o = eval(_str)
        return o

    def __repr__(self):
        return f"StreamData{self.as_tuple()}"

def obj_xeq(a, b):
    if isinstance(a, list):
        return list_xeq(a, b)
    if isinstance(a, dict):
        return dict_xeq(a, b)
    if isinstance(a, XCmp):
        return a.xeq(b)
    return a == b

def list_xeq(l0, l1):
    if len(l0) != len(l1):
        return False
    lx = list(l1)
    for a in l0:
        for i in range(0, len(lx)):
            b = lx[i]
            if obj_xeq(a, b):
                break
        else:
            return False
        lx.pop(i)
    return True

def dict_xeq(d0, d1):
    k0 = list(d0.keys())
    k0.sort()
    k1 = list(d1.keys())
    k1.sort()
    if k0 != k1:
        return False
    for k in k0:
        v0 = d0[k]
        v1 = d1[k]
        if not obj_xeq(v0, v1):
            return False
    return True

def list_xsort(l):
    for o in l:
        if isinstance(o,XCmp):
            o.xsort()
    l.sort()

def dict_xsort(d):
    for o in d.values():
        if isinstance(o,XCmp):
            o.xsort()

class XCmp(object):
    def xeq(self, other):
        if type(self) != type(other):
            raise TypeError(f'TypeMismatch, expecting {type(self)}, got {type(other)}')
        for k in self.__dict__.keys():
            v0 = getattr(self, k)
            v1 = getattr(other, k)
            if v0 is None or v1 is None:
                continue # skip on None attributes
            if not obj_xeq(v0, v1):
                return False
        return True

    def xsort(self):
        for k,v in self.__dict__.items():
            if isinstance(v, list):
                list_xsort(v)
            elif isinstance(v, dict):
                dict_xsort(v)
            elif isinstance(v, XCmp):
                v.xsort()

    def __lt__(self, other):
        if type(self) != type(other):
            raise TypeError(f'TypeMismatch, expecting {type(self)}, got {type(other)}')
        for k in type(self).__dataclass_fields__.keys():
            v0 = getattr(self, k)
            v1 = getattr(other, k)
            if v0 is None:
                if v1 is None:
                    continue
                return True
            if v0 < v1:
                return True
        return False

IP4_ADDR_PORT = re.compile('(\d+)\.(\d+)\.(\d+).(\d+)(?:\:(\d+))?')

@dataclass(frozen = True)
class StreamAddr(XCmp):
    addr: tuple = None
    port: int   = None

    @classmethod
    def from_str(cls, s):
        m = IP4_ADDR_PORT.match(s)
        if not m:
            raise ValueError(f"'{s}' is not in the form of IP4_ADDR[:PORT]")
        g = m.groups()
        g = tuple( int(a) if a is not None else None for a in g )
        ip_addr = g[0:4]
        port = g[4]
        return cls(ip_addr, port)

@dataclass()
class TimeSpec(XCmp):
    tv_sec:  int = None
    tv_nsec: int = None

@dataclass()
class StreamCounters(XCmp):
    first_ts: TimeSpec = None
    last_ts:  TimeSpec = None
    count:    int      = None
    bytes:    int      = None

@dataclass()
class StreamSrcStats(XCmp):
    src: StreamAddr     = None
    rx:  StreamCounters = None

@dataclass()
class StreamClientPairStats(XCmp):
    stream_name:  str            = None
    client_match: str            = None
    client_desc:  str            = None
    is_regex:     int            = None
    tx:           StreamCounters = None
    drops:        StreamCounters = None

@dataclass()
class StreamStats(XCmp):
    rx:      StreamCounters = None
    sources: dict           = None
    clients: dict           = None
    name:    str            = None

@dataclass()
class StreamClientStats(XCmp):
    tx:       StreamCounters = None
    drops:    StreamCounters = None
    streams:  dict           = None
    dest:     StreamAddr     = None
    is_regex: int            = None
    match:    str            = None
    desc:     str            = None

################################################################################


##################################
#  Container/Cluster Interfaces  #
##################################

class LDMSDContainerTTY(ABC):
    EOT = b'\x04' # end of transmission (ctrl-d)

    @abstractmethod
    def read(self, idle_timeout = 1):
        """Read the TTY until idle

        Returns
        -------
        str: data read from the TTY
        """
        raise NotImplementedError()

    @abstractmethod
    def write(self, data):
        """Write `data` to TTY"""
        raise NotImplementedError()

    @abstractmethod
    def term(self):
        """Terminate TTY connection"""
        raise NotImplementedError()

class LDMSDContainer(ABC):
    """Container wrapper for a container being a part of LDMSDCluster

    LDMSDContainer extends DockerClusterContainer -- adding ldmsd-specific
    routines and properties.

    Application does not normally direcly create LDMSDContainer. LDMSDContainer
    should be obtained by calling `get_container()` or accessing `containers` of
    LDMSDCluster.
    """
    def __init__(self, obj, cluster):
        if not issubclass(type(cluster), LDMSDCluster):
            raise TypeError("`clutser` must be a subclass of LDMSDCluster")
        self.cluster = cluster
        self.DAEMON_TBL = {
            "etcd": self.start_etcd,
            "ldmsd": self.start_ldmsd,
            "sshd": self.start_sshd,
            "munged": self.start_munged,
            "slurmd": self.start_slurmd,
            "slurmctld": self.start_slurmctld,
        }
        self.munged = dict()

    @abstractmethod
    def start(self):
        raise NotImplementedError()

    @abstractmethod
    def stop(self):
        raise NotImplementedError()

    @abstractmethod
    def exec_run(self, cmd, env=None, user=None):
        """[ABSTRACT] Execute `cmd` in the container.

        Parameters
        ----------
        cmd:  a `str` of the command to execute
        env:  (optional) a `dict` of additional environment variables
        user: (optional) a username (`str`) that execute the command

        Returns
        -------
        (rc, out) a pair of return code (rc:int) and output (out:str) from the
                  command.
        """
        raise NotImplementedError()

    @abstractmethod
    def pipe(self, cmd, content):
        """[ABSTRACT] Execute `cmd` (str) in the container with `content` (str) feeding to STDIN

        Returns
        -------
        (rc, out) a pair of return code (rc:int) and output (out:str) from the
                  command.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_file(self, path, content, user = None):
        """Write `content` to `path` in the container"""
        raise NotImplementedError()

    @abstractmethod
    def read_file(self, path):
        """Read file specified by `path` from the container"""
        raise NotImplementedError()

    @abstractmethod
    def exec_interact(self, cmd):
        """[ABSTRACT] Execute `cmd` in the container interactively

        Returns
        -------
        tty: an object representing a TTY that is a subclass of
             `LDMSDContainerTTY`.
        """
        raise NotImplementedError()

    def pgrep(self, *args):
        """Return (rc, output) of `pgrep *args`"""
        return self.exec_run("pgrep " + " ".join(args))

    def pgrepc(self, prog):
        """Reurn the number from `pgrep -c {prog}`"""
        rc, out = self.pgrep("-c", prog)
        return int(out)

    def check_etcd(self):
        """Check if etcd is running"""
        rc, out = self.exec_run("pgrep -c etcd")
        return rc == 0

    def start_etcd(self, spec_overrid = {}, **kwargs):
        """Start etcd in the container"""
        if self.check_etcd():
            return
        # collect etcd daemons from the cluster spec
        etcd_conts = []
        for node in self.cluster.spec.get("nodes", []):
            for d in node.get("daemons", []):
                if d.get("type") == "etcd":
                    hostname = node["hostname"]
                    cont = self.cluster.get_container(hostname)
                    etcd_conts.append(cont)
        initial_cluster = ",".join([
                f"{c.hostname}=http://{c.ip_addr}:2380" for c in etcd_conts
            ])
        cmd = f"bash -c 'etcd --data-dir /var/lib/etcd/default.etcd" \
              f" --name {self.hostname}" \
              f" --initial-advertise-peer-urls http://{self.ip_addr}:2380" \
              f" --listen-peer-urls http://{self.ip_addr}:2380" \
              f" --advertise-client-urls http://{self.ip_addr}:2379" \
              f" --listen-client-urls http://0.0.0.0:2379" \
              f" --initial-cluster {initial_cluster}" \
              f" --initial-cluster-state new" \
              f" --initial-cluster-token token" \
              f" >/var/log/etcd.log 2>&1 &'"
        rc, out = self.exec_run(cmd)
        if rc:
            raise RuntimeError("sshd failed, rc: {}, output: {}" \
                              .format(rc, out))

    def check_ldmsd(self):
        """Check if ldmsd is running"""
        rc, out = self.exec_run("pgrep -c ldmsd")
        return rc == 0

    def start_ldmsd(self, spec_override = {}, **kwargs):
        """Start ldmsd in the container"""
        if self.check_ldmsd():
            return # already running
        spec = deep_copy(self.ldmsd_spec)
        spec.update(spec_override)
        if not spec:
            return # no ldmsd spec for this node and no spec given
        cfg = self.get_ldmsd_config(spec)
        self.write_file(spec["config_file"], cfg)
        cmd = self.get_ldmsd_cmd(spec)
        rc, out = self.exec_run(cmd, env = env_dict(spec["env"]))
        if rc:
            raise RuntimeError("ldmsd exec error {}: {}".format(rc, out))
        port = spec.get("listen_port")
        if not port:
            lstn = spec.get("listen")
            if lstn:
                port = lstn[0].get("port")
        if not port:
            raise RuntimeError("Cannot determine ldmsd port")
        grp = ":\\<{:x}\\>".format(int(port))
        grp_cmd = "grep {} /proc/net/tcp".format(shlex.quote(grp))
        def _check_listen():
            rc, out = self.exec_run(grp_cmd)
            return rc == 0
        cond_timedwait(_check_listen, 5)

    def kill_ldmsd(self):
        """Kill ldmsd in the container"""
        self.exec_run("pkill ldmsd")

    @cached_property
    def spec(self):
        """Get container spec"""
        for node in self.cluster.spec["nodes"]:
            if self.hostname == node["hostname"]:
                return node
        return None

    @property
    def interfaces(self):
        return self.get_interfaces()

    @abstractmethod
    def get_interfaces(self):
        """[ABSTRACT] Returns a list of (IF_NAME, IP_ADDR) tuples"""
        raise NotImplementedError()

    @property
    def ip_addr(self):
        return self.get_ip_addr()

    @abstractmethod
    def get_ip_addr(self):
        """[ABSTRACT] Returns a `str` of IP address"""
        raise NotImplementedError()

    @cached_property
    def name(self):
        """The container name"""
        return self.get_name()

    @abstractmethod
    def get_name(self):
        """[ABSTRACT] Returns the name of the container"""
        raise NotImplementedError()

    @property
    def hostname(self):
        """Return hostname of the container"""
        return self.get_hostname()

    @abstractmethod
    def get_hostname(self):
        """[ABSTRACT] Returns hostname of the container"""
        raise NotImplementedError()

    @cached_property
    def host(self):
        """The host that hosts the container"""
        return self.get_host()

    @abstractmethod
    def get_host(self):
        raise NotImplementedError()

    @property
    def aliases(self):
        """The list of aliases of the container hostname"""
        return self.get_aliases()

    @abstractmethod
    def get_aliases(self):
        """[ABSTRACT] Returns a list of hostname alises of the container"""
        raise NotImplementedError()

    @property
    def env(self):
        return self.get_env()

    @abstractmethod
    def get_env(self):
        """[ABSTRACT] Returns `env` from the container configuration"""
        raise NotImplementedError()

    @cached_property
    def ldmsd_spec(self):
        """Get the spec for this ldmsd (from the associated service)"""
        ldmsd = next( (d for d in self.spec.get("daemons", []) \
                          if d["type"] == "ldmsd"), None )
        if not ldmsd:
            return {} # empty dict
        dspec = deep_copy(ldmsd)

        # apply cluster-level env
        w = env_dict(dspec.get("env", []))
        v = env_dict(self.cluster.spec.get("env", []))
        v.update(w) # dspec precede svc_spec
        dspec["env"] = v
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
        return get_ldmsd_config(spec, ver = self.ldmsd_version)

    @cached_property
    def ldmsd_cmd(self):
        """Command to run ldmsd"""
        return self.get_ldmsd_cmd(self.ldmsd_spec)

    def get_ldmsd_cmd(self, spec):
        """Get ldmsd command line according to spec"""
        if "listen_xprt" in spec and "listen_port" in spec:
            XPRT_OPT = "-x {listen_xprt}:{listen_port}".format(**spec)
        else:
            XPRT_OPT = ""
        if "listen_auth" in spec:
            AUTH_OPT = "-a {listen_auth}".format(**spec)
        else:
            AUTH_OPT = ""
        CREDIT_OPT = f"-C {spec['credits']}" if "credits" in spec else ""
        VARS = dict(locals())
        VARS.update(spec)
        cmd = "ldmsd {XPRT_OPT} {AUTH_OPT}" \
              " -c {config_file} -l {log_file} -v {log_level}" \
              " {CREDIT_OPT}".format(**VARS)
        return cmd

    def ldms_ls(self, *args):
        """Executes `ldms_ls` with *args, and returns (rc, output)"""
        cmd = "ldms_ls " + (" ".join(args))
        return self.exec_run(cmd)

    def get_munged(self, dom = None):
        """Get the munged handle"""
        return self.munged.get(dom)

    def set_munged(self, dom, m):
        self.munged[dom] = m

    def start_munged(self, name = None, dom = None, key = None, **kwargs):
        """Start Munge Daemon"""
        m = self.get_munged(dom)
        if not m:
            for d in self.spec["daemons"]:
                if name == d.get("name"):
                    break
                if not name and dom == d.get("dom"):
                    break
            else: # not found
                d = dict(dom = dom, key = key)
            key = key if key else d.get("key")
            dom = dom if dom else d.get("dom")
            m = Munged(self, dom, key)
            self.set_munged(dom, m)
        m.start()

    def kill_munged(self, dom = None):
        """Kill munged"""
        m = self.get_munged(dom)
        if m:
            m.kill()

    def prep_slurm_conf(self):
        """Prepare slurm configurations"""
        self.write_file("/etc/slurm/cgroup.conf", "CgroupAutomount=yes")
        self.write_file("/etc/slurm/slurm.conf", self.cluster.slurm_conf)

    def start_slurm(self):
        """Start slurmd in all sampler nodes, and slurmctld on svc node"""
        # determine our role
        daemons = self.spec.get("daemons", [])
        slurm_daemons = [ d for d in daemons \
                            if d.get("type") in set(["slurmctld", "slurmd"]) ]
        will_start = [d for d in slurm_daemons if self.pgrepc(d["type"]) == 0]
        if not will_start:
            return # no daemon to start
        self.prep_slurm_conf()
        for d in will_start:
            prog = d["type"]
            plugstack = d.get("plugstack")
            if plugstack:
                sio = StringIO()
                for p in plugstack:
                    sio.write("required" if p.get("required") else "optional")
                    sio.write(" " + p["path"])
                    for arg in p.get("args", []):
                        sio.write(" " + arg)
                    sio.write("\n")
                self.write_file("/etc/slurm/plugstack.conf", sio.getvalue())
            rc = -1
            retry = 3
            while rc != 0 and retry > 0:
                retry -= 1
                rc, out = self.exec_run(prog)
            if rc:
                raise RuntimeError("{} failed, rc: {}, output: {}" \
                               .format(prog, rc, out))

    def kill_slurm(self):
        """Kill slurmd and slurmctld"""
        self.exec_run("pkill slurmd slurmctld")

    def _start_slurmx(self, prog):
        """(private) Start slurmd or slurmctld"""
        # get default munge spec
        self.start_munged() # slurm depends on munged
        if self.pgrepc(prog) > 0:
            return # already running
        self.prep_slurm_conf()
        d = next( ( x for x in self.spec.get("daemons", []) \
                      if x.get("type") == prog) )
        plugstack = d.get("plugstack")
        if plugstack:
            sio = StringIO()
            for p in plugstack:
                sio.write("required" if p.get("required") else "optional")
                sio.write(" " + p["path"])
                for arg in p.get("args", []):
                    sio.write(" " + arg)
                sio.write("\n")
            self.write_file("/etc/slurm/plugstack.conf", sio.getvalue())
        rc = -1
        retry = 3
        while rc != 0 and retry > 0:
            retry -= 1
            rc, out = self.exec_run(prog)
        if rc:
            raise RuntimeError("{} failed, rc: {}, output: {}" \
                               .format(prog, rc, out))

    def start_slurmd(self, **kwargs):
        """Start slurmd"""
        self._start_slurmx("slurmd")

    def start_slurmctld(self, **kwargs):
        """Start slurmctld"""
        self._start_slurmx("slurmctld")

    def start_sshd(self, **kwargs):
        """Start sshd"""
        rc, out = self.pgrep("-c -x sshd")
        if rc == 0: # already running
            return
        rc, out = self.exec_run("/usr/sbin/sshd")
        if rc:
            raise RuntimeError("sshd failed, rc: {}, output: {}" \
                               .format(rc, out))

    def start_daemons(self):
        """Start all daemons according to spec"""
        for daemon in self.spec.get("daemons", []):
            tp = daemon["type"]
            fn = self.DAEMON_TBL.get(tp)
            if not fn:
                raise RuntimeError("Unsupported daemon type:{}".format(tp))
            fn(**daemon)

    def config_ldmsd(self, cmds):
        if not self.pgrepc('ldmsd'):
            raise RuntimeError("There is no running ldmsd to configure")
        spec = self.ldmsd_spec
        if type(cmds) not in (list, tuple):
            cmds = [ cmds ]
        sio = StringIO()
        D.sio = sio
        for _cmd in cmds:
            sio.write(_cmd)
            sio.write('\n')
        _listen = spec.get("listen")
        if _listen:
            _xprt = _listen[0].get("xprt", "sock")
            _port = _listen[0].get("port", 10000)
            _auth = _listen[0].get("auth", "none")
        else:
            _xprt = spec["listen_xprt"]
            _port = spec["listen_port"]
            _auth = spec["listen_auth"]
        cmd = 'bash -c \'ldmsd_controller --host {host} ' \
              '--xprt {xprt} ' \
              '--port {port} ' \
              '--auth {auth} ' \
              ' && true \' ' \
                  .format(
                      host=self.hostname,
                      xprt=_xprt,
                      port=_port,
                      auth=_auth,
                  )
        D.cmd = cmd
        return self.pipe(cmd, sio.getvalue())

    @cached_property
    def ldmsd_version(self):
        rc, out = self.exec_run("ldmsd -V")
        _drop, _ver = out.splitlines()[0].split(': ')
        m = re.match(r'(\d+)\.(\d+)\.(\d+)', _ver)
        if not m:
            raise ValueError("Bad ldmsd version format: {}".format(_ver))
        ver = tuple(map(int, m.groups()))
        return ver

    def chmod(self, mode, path):
        """chmod `mode` `path`"""
        cmd = "chmod {:o} {}".format(int(mode), path)
        rc, output = self.exec_run(cmd)
        if rc:
            raise RuntimeError("Error {} {}".format(rc, output))

    def chown(self, owner, path):
        """chown `owner` `path`"""
        cmd = "chown {} {}".format(owner, path)
        rc, output = self.exec_run(cmd)
        if rc:
            raise RuntimeError("Error {} {}".format(rc, output))

    def proc_environ(self, pid):
        """Returns environment (dict) of process `pid`"""
        _env = self.read_file("/proc/{}/environ".format(pid))
        _env = _env.split('\x00')
        _env = dict( v.split('=', 1) for v in _env if v )
        return _env

    def files_exist(self, files, timeout=None, interval=0.1):
        t0 = time.time()
        if type(files) == str:
            files = [ files ]
        cmd = " && ".join( "test -e {}".format(s) for s in files)
        cmd = "bash -c '" + cmd + "'"
        count = 0
        while timeout is None or time.time() - t0 < timeout:
            rc, out = self.exec_run(cmd)
            if rc == 0:
                return True
            count += 1
            time.sleep(interval)
        return False

# ---------------------------------------------------------- LDMSDContainer -- #


class LDMSDCluster(ABC):
    """LDMSD Cluster - a virtual cluster for LDMSD

    Get or optionally create the virtual cluster:
    >>> cluster = LDMSDCluster.get(name, create=True, spec= spec)

    """
    @classmethod
    def create(cls, spec):
        """Create a virtual cluster for ldmsd with given `spec`

        `spec` is a dictionary describing the docker virtual cluster, nodes in
        the virtual cluster, and daemons running on them. The following list
        describes attributes in the `spec`:
          - "name" is the name of the virtual cluster.
          - "description" is a short description of the virtual cluster.
          - "templates" is a dictionary of templates to apply with
                        "!extends" special keyword attribute.
          - "cap_add" is a list of capabilities to add to the containers.
          - "cap_drop" is a list of capabilities to drop from the containers.
          - "image" is the name of the docker image to use.
          - "ovis_prefix" is the path to ovis installation in the host machine.
          - "env" is a dictionary of cluster-wide environment variables.
          - "mounts" is a list of mount points with "SRC:DST:MODE" format in
            which SRC being the source path in the HOST, DST being the
            destination path in the CONTAINER, and MODE being `rw` or `ro`.
          - "nodes" is a list of nodes, each item of which describes a node in
            the cluster.
        Templates and "%ATTR%" substitution can be used to reduce repititive
        descriptions in the spec. The "!extends" object attribute is reserved
        for instructing the spec mechanism to apply a template referred to by
        the value of "!extends" attribute. Consider the following example:
        ```
        {
            "templates": {
                "node-temp": {
                    "daemons": [
                        { "name": "sshd", "type": "sshd" },
                        {
                            "name": "sampler",
                            "type": "ldmsd",
                            "samplers": [
                                {
                                    "plugin": "meminfo",
                                    "config": [
                                        "instance=%hostname%/%plugin%",
                                    ],
                                },
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
        The nodes extend "node-temp" template resulting in them having "daemons"
        defined in the template. The "%hostname%" and "%plugin%" in
        "instance=%hostname%/%plugin%" is later substituted with the nearest
        attributes by containment hierarchy. For "node-1", the string becomes
        "instance=node-1/meminfo" because the nearest "hostname" is the hostname
        attribute defined by the node object, and the nearest "plugin" attribute
        is the attribute defined in sampler plugin object. A template can also
        extends another template. The local attriute declaration overrides that
        declared by the template.

        The following paragraphs explain the node and daemon objects in the
        spec.

        The node in `spec["nodes"]` is a dictionary describing a node in the
        cluster containing the following attributes:
          - "hostname" defines the hostname of the container and is used to
            construct container name with '{spec["name"]}-{hostname}' format.
          - "env" is a dicionary of environment variables for the node which is
            merged with cluster-wide env (node-level precedes cluster-level).
          - "daemons" is a list of objects describing daemons running on the
            node.

        The daemon in `spec["nodes"][X]["daemons"]` is a dictionary describing
        supported daemons with the following common attributes:
          - "name" is the name of the daemon.
          - "type" is the type of the supported daemons, which are "etcd",
            "sshd", "munged", "slurmctld", "slurmd", and "ldmsd".

        "sshd", "munged" and "slurmd" daemons do not have extra attributes other
        than the common daemon attributes described above.

        "slurmd" daemon has the following extra attributes:
          - "plugstack" is a list of dictionary describing Slurm plugin. Each
            entry in the "plugstack" list contains the following attributes:
              - "required" can be True or False describing whether the plugin
                is required or optional. slurmd won't start if the required
                plugin failed to load.
              - "path" is the path to the plugin.
              - "args" is a list of arguments (strings) to the plugin.

        "ldmsd" daemon contains the following extra attributes:
          - "listen_port" is an integer describing the daemon listening port.
          - "listen_xprt" is the LDMS transport to use ("sock", "ugni" or
            "rdma").
          - "listen_auth" is the LDMS authentication method to use.
          - "samplers" (optional) is a list of sampler plugins (see below).
          - "prdcrs" (optional) is a list of producers (see below).
          - "config" (optional) is a list of strings for ldmsd configuration
            commands.
        The "samplers" list is processed first (if specified), then "prdcrs" (if
        specified), and "config" (if specified) is processed last.

        The sampler object in ldmsd daemon "samplers" list is described as
        follows:
          - "plugin" is the plugin name.
          - "interval" is the sample interval (in micro seconds).
          - "offset" is the sample offset (in micro seconds).
          - "start" can be True or False -- marking whether the plugin needs
            a start command (some plugins update data by events and do not
            require start command).
          - "config" is a list of strings "NAME=VALUE" for plugin
            configuration arguments.

        The producer object in the ldmsd daemon "prdcrs" list is described as
        follows:
          - "host" is the hostname of the ldmsd to connect to.
          - "port" is an integer describing the port of the target ldmsd.
          - "xprt" is the transport type of the target ldmsd.
          - "type" is currently be "active" only.
          - "interval" is the connection retry interval (in micro-seconds).

        The following is the `spec` skeleton:
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
        """
        _cls = get_cluster_class()
        cluster = _cls._create(spec)
        cluster.make_ovis_env()
        return cluster

    @classmethod
    @abstractmethod
    def _create(cls, spec):
        """[ABSTRACT] Create the virtual cluster according to `spec`.

        The subclass shall implement virtual cluster creation in this method.

        Returns
        -------
        obj: An object of LDMSDCluster subclass representing a virtual cluster
        """
        raise NotImplementedError()

    @classmethod
    def get(cls, name, create = False, spec = None):
        """Obtain an existing ldmsd virtual cluster (or create if `create=True`)"""
        _cls = get_cluster_class()
        try:
            cluster = _cls._get(name)
            if spec and Spec(spec) != cluster.spec:
                raise RuntimeError("spec mismatch")
            return cluster
        except LookupError:
            if not create:
                raise
            return _cls.create(spec)

    @classmethod
    @abstractmethod
    def _get(cls, name):
        """[ABSTRACT] Obtain a handle to a running virtual cluster with `name`.

        Returns
        -------
        obj:  An object of LDMSDCluster subclass representing a virtual cluster
              if the cluster of given `name` existed.

        Raises
        ------
        LookupError: If the `name` cluster does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove(self):
        """[ABSTRACT] Remove the cluster"""
        raise NotImplementedError()

    @cached_property
    def name(self):
        return self.get_name()

    @abstractmethod
    def get_name(self):
        """[ABSTRACT] Returns the name of the cluster"""
        raise NotImplementedError()

    @cached_property
    def containers(self):
        """A list of containers wrapped by DockerClusterContainer"""
        return self.get_containers()

    @abstractmethod
    def get_containers(self, timeout = 10):
        """[ABSTRACT] Returns a list of LDMSDContainer subclass objects in the cluster"""
        raise NotImplementedError()

    @abstractmethod
    def get_container(self, name):
        """[ABSTRACT] Returns the matching container in the cluster"""
        raise NotImplementedError()

    @cached_property
    def spec(self):
        return self.get_spec()

    @abstractmethod
    def get_spec(self):
        """[ABSTRACT] Yield the spec used to create the cluster"""
        raise NotImplementedError()

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

    @cached_property
    def node_aliases(self):
        return self.get_node_aliases()

    @abstractmethod
    def get_node_aliases(self):
        """[ABSTRACT] Returns a dict of { NODE_NAME: [ ALIASES ] }"""
        raise NotImplementedError()

    def start_ldmsd(self):
        """Start ldmsd in each node in the cluster"""
        for cont in self.containers:
            cont.start_ldmsd()

    def check_ldmsd(self):
        """Returns a dict(hostname:bool) indicating if each ldmsd is running"""
        return { cont.hostname : cont.check_ldmsd() \
                                        for cont in self.containers }

    @cached_property
    def slurm_version(self):
        rc, out = self.exec_run("slurmd -V")
        _slurm, _ver = out.split(' ')
        ver = tuple( int(v) for v in _ver.split('.') )
        return ver

    @property
    def slurm_conf(self):
        """Content for `/etc/slurm/slurm.conf`"""
        nodes = self.spec["nodes"]
        cpu_per_node = self.spec.get("cpu_per_node", 1)
        oversubscribe = self.spec.get("oversubscribe", "NO")
        slurm_loglevel = self.spec.get("slurm_loglevel", "info")
        slurmd_nodes = []
        slurmctld_node = None
        for node in nodes:
            daemons = node.get("daemons", [])
            daemons = set( d["type"] for d in daemons )
            if "slurmd" in daemons:
                slurmd_nodes.append(node["hostname"])
            if "slurmctld" in daemons:
                slurmctld_node = node["hostname"]
        slurmd_nodes = ",".join(slurmd_nodes)
        # Check slurmd version
        if self.slurm_version >= (18, 0, 0):
            slurmctld_key = "SlurmctldHost"
        else:
            slurmctld_key = "ControlMachine"
        slurmconf = \
            "{slurmctld_key}={slurmctld_node}\n"\
            "MpiDefault=none\n"\
            "ProctrackType=proctrack/linuxproc\n"\
            "ReturnToService=1\n"\
            "SlurmctldPidFile=/var/run/slurmctld.pid\n"\
            "SlurmctldPort=6817\n"\
            "SlurmdPidFile=/var/run/slurmd.pid\n"\
            "SlurmdPort=6818\n"\
            "SlurmdSpoolDir=/var/spool/slurmd\n"\
            "SlurmUser=root\n"\
            "StateSaveLocation=/var/spool\n"\
            "SwitchType=switch/none\n"\
            "#TaskPlugin=task/none\n"\
            "#TaskPluginParam=Sched\n"\
            "InactiveLimit=0\n"\
            "KillWait=30\n"\
            "MinJobAge=300\n"\
            "SlurmctldTimeout=120\n"\
            "SlurmdTimeout=300\n"\
            "Waittime=0\n"\
            "#FastSchedule=1\n"\
            "SchedulerType=sched/builtin\n"\
            "SelectType=select/cons_res\n"\
            "SelectTypeParameters=CR_CPU\n"\
            "AccountingStorageType=accounting_storage/none\n"\
            "#AccountingStoreJobComment=YES\n"\
            "ClusterName=cluster\n"\
            "JobCompType=jobcomp/none\n"\
            "JobAcctGatherFrequency=30\n"\
            "JobAcctGatherType=jobacct_gather/none\n"\
            "SlurmctldDebug={slurm_loglevel}\n"\
            "SlurmctldLogFile=/var/log/slurmctld.log\n"\
            "SlurmdDebug={slurm_loglevel}\n"\
            "SlurmdLogFile=/var/log/slurmd.log\n"\
            "NodeName={slurmd_nodes} CPUs={cpu_per_node} State=UNKNOWN\n"\
            "PartitionName=debug Nodes={slurmd_nodes} OverSubscribe={oversubscribe} Default=YES MaxTime=INFINITE State=UP\n"\
            "LogTimeFormat=thread_id\n"\
            .format( slurmctld_key = slurmctld_key,
                    slurmctld_node = slurmctld_node,
                    slurmd_nodes = slurmd_nodes,
                    cpu_per_node = cpu_per_node,
                    oversubscribe = oversubscribe,
                    slurm_loglevel = slurm_loglevel )
        return slurmconf

    def start_munged(self, **kwargs):
        """Start Munge Daemon"""
        for cont in self.containers:
            cont.start_munged(**kwargs)

    def start_slurm(self):
        """Start slurmd in all sampler nodes, and slurmctld on svc node"""
        self.start_munged()
        for cont in self.containers:
            cont.start_slurm()

    def start_sshd(self):
        """Start sshd in all containers"""
        for cont in self.containers:
            cont.start_sshd()
        self.make_known_hosts()

    def ssh_keyscan(self):
        """Report ssh-keyscan result of all nodes in the cluster"""
        hosts = reduce(lambda x,y: x+y,
                         ([c.hostname, c.ip_addr] + c.aliases \
                                             for c in self.containers) )
        cmd = "bash -c '" + "ssh-keyscan " + " ".join(hosts) + " 2>/dev/null'"
        cont = self.containers[-1]
        rc, out = cont.exec_run(cmd)
        return out

    def make_known_hosts(self):
        """Make `/root/.ssh/known_hosts` in all nodes"""
        ks = self.ssh_keyscan()
        for cont in self.containers:
            cont.exec_run("mkdir -m 0700 -p /root/.ssh")
            cont.write_file("/root/.ssh/known_hosts", ks)

    def make_ssh_id(self):
        """Make `/root/.ssh/id_rsa` and authorized_keys"""
        cont = self.containers[-1]
        cont.exec_run("mkdir -m 0700 -p /root/.ssh/")
        cont.exec_run("rm -f /root/.ssh/id_rsa id_rsa.pub")
        cont.exec_run("ssh-keygen -q -N '' -f /root/.ssh/id_rsa")
        D.id_rsa = id_rsa = cont.read_file("/root/.ssh/id_rsa")
        D.id_rsa_pub = id_rsa_pub = cont.read_file("/root/.ssh/id_rsa.pub")
        for cont in self.containers:
            cont.exec_run("mkdir -m 0700 -p /root/.ssh/")
            cont.write_file("/root/.ssh/id_rsa", id_rsa)
            cont.exec_run("chmod 600 /root/.ssh/id_rsa")
            cont.write_file("/root/.ssh/id_rsa.pub", id_rsa_pub)
            cont.write_file("/root/.ssh/authorized_keys", id_rsa_pub)
            cont.exec_run("chmod 600 /root/.ssh/authorized_keys")

    def exec_run(self, cmd, env=None):
        """A pass-through to last_cont.exec_run()

        The `last_cont` is the last container in the virtual cluster, which does
        NOT have any `ldmsd` role (i.e. no `ldmsd` running on it). If
        `start_slurm()` was called, the `last_cont` is also the slurm head node
        where slurmctld is running.
        """
        cont = self.containers[-1]
        return cont.exec_run(cmd, env=env)

    def sbatch(self, script_path, *argv):
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
        opts = " ".join(argv) if len(argv) else ""
        rc, out = self.exec_run(f"bash -c \"cd {_dir} && sbatch {opts} {_base}\"")
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
        allhosts = set([ c.hostname for c in self.containers ])
        allhosts_txt = ' '.join(allhosts)
        for cont in self.containers:
            cont.write_file("/etc/ld.so.conf.d/ovis.conf",
                            "/opt/ovis/lib\n"
                            "/opt/ovis/lib64\n"
                            "/opt/ovis/lib/ovis-ldms\n"
                            "/opt/ovis/lib64/ovis-ldms\n"
                            )
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
                _add LD_LIBRARY_PATH $PREFIX/lib/ovis-ldms
                _add LD_LIBRARY_PATH $PREFIX/lib64/ovis-ldms
                _add MANPATH $PREFIX/share/man
                _add PYTHONPATH $(echo $PREFIX/lib/python*/site-packages)
                _add PYTHONPATH $(echo $PREFIX/lib/python*/dist-packages)

                _add ZAP_LIBPATH $PREFIX/lib/ovis-ldms
                _add ZAP_LIBPATH $PREFIX/lib64/ovis-ldms
                _add ZAP_LIBPATH $PREFIX/lib/ovis-lib
                _add ZAP_LIBPATH $PREFIX/lib64/ovis-lib
                export ZAP_LIBPATH
                _add LDMSD_PLUGIN_LIBPATH $PREFIX/lib/ovis-ldms
                _add LDMSD_PLUGIN_LIBPATH $PREFIX/lib64/ovis-ldms
                export LDMSD_PLUGIN_LIBPATH
            """
            cont.write_file("/etc/profile.d/ovis.sh", profile)
            otherhosts = allhosts - set([cont.hostname])
            otherhosts_txt = ' '.join(otherhosts)
            pssh_profile = """
                export ALLHOSTS='{allhosts_txt}'
                export OTHERHOSTS='{otherhosts_txt}'
                alias pssh.others='pssh -H "${{OTHERHOSTS}}"'
                alias pscp.others='pscp.pssh -H "${{OTHERHOSTS}}"'
            """.format(
                    allhosts_txt = allhosts_txt,
                    otherhosts_txt = otherhosts_txt,
                )
            cont.write_file("/etc/profile.d/pssh.sh", pssh_profile)

    def all_pgrepc(self, prog):
        """Perform `cont.pgrepc(prog)` for each cont in self.containers"""
        return { cont.hostname : cont.pgrepc(prog) for cont in self.containers }

    def start_daemons(self):
        """Start daemons according to spec"""
        for cont in self.containers:
            cont.start_daemons()

    def all_exec_run(self, cmd):
        return { c.hostname : c.exec_run(cmd) for c in self.containers }

    @cached_property
    def ldmsd_version(self):
        rc, out = self.exec_run("ldmsd -V")
        _drop, _ver = out.splitlines()[0].split(': ')
        ver = tuple( int(v) for v in _ver.split('.') )
        return ver

    @classmethod
    def list(cls):
        _cls = get_cluster_class()
        return _cls._list()

    @classmethod
    @abstractmethod
    def _list(cls):
        """Returns a list of LDMSDCluster subclass objects of running clusters"""
        raise NotImplementedError()

    def files_exist(self, files, timeout=None, interval=0.1):
        t0 = time.time()
        if type(files) == str:
            files = [ files ]
        cmd = " && ".join( "test -e {}".format(s) for s in files)
        cmd = "bash -c '" + cmd + "'"
        count = 0
        while timeout is None or time.time() - t0 < timeout:
            for _cont in self.containers:
                rc, out = _cont.exec_run(cmd)
                if rc:
                    break
            else: # loop does not break
                return True
            count += 1
            time.sleep(interval)
        return False
# ------------------------------------------------------------ LDMSDCluster -- #

def read_msg(_file):
    """Read a message "\x01...\x03" from `_file` file handle"""
    pos = _file.tell()
    sio = StringIO()
    c = _file.read(1)
    if not c:
        raise ValueError("End of file")
    if c != "\x01":
        _file.seek(pos)
        raise ValueError("Not a start of message")
    c = _file.read(1)
    while c and c != "\x02":
        sio.write(c)
        c = _file.read(1)
    if c != "\x02":
        _file.seek(pos)
        raise ValueError("Bad message header")
    _type = sio.getvalue()
    sio = StringIO() # reset sio
    c = _file.read(1)
    while c and c != "\x03":
        sio.write(c)
        c = _file.read(1)
    if c != "\x03":
        _file.seek(pos)
        raise ValueError("Incomplete message")
    text = sio.getvalue()
    text = text.strip('\x00')
    obj = None
    if _type == "json":
        obj = json.loads(text)
    return { "type": _type, "text": text, "obj": obj }

LDMSD_STR_VER_RE = re.compile(r'LDMSD_VERSION (\d+).(\d+).(\d+)')
LDMSD_EXE_VER_RE = re.compile(r'LDMSD Version: (\d+).(\d+).(\d+)')
def ldmsd_version(prefix):
    """Get LDMSD version from the installation prefix"""
    try:
        _cmd = "strings {}/sbin/ldmsd | grep 'LDMSD_VERSION '".format(prefix)
        out = sp.check_output(_cmd, shell = True, executable="/bin/bash").decode()
    except:
        out = ""
    m = LDMSD_STR_VER_RE.match(out)
    if not m:
        # try `ldmsd -V`
        try:
            _cmd = "{}/sbin/ldmsd -V | grep 'LDMSD Version: '".format(prefix)
            out = sp.check_output(_cmd, shell = True, executable="/bin/bash").decode()
        except:
            out = ""
        m = LDMSD_EXE_VER_RE.match(out)
        if not m:
            raise ValueError("Cannot determine ldmsd version")
    return tuple(map(int, m.groups()))

def is_ldmsd_version_4(ver):
    return ver < (4, 100, 0)

class Munged(object):
    """Munged(cont)
    Munged(cont, dom = "DOM_NAME", key = "KEY")

    A munged handler in a docker container `cont`. If `dom` is not given, use
    the default file locations. If `key` is not given, use the existing key.

    If `dom` is given, all of the files will be put under `/munge/DOM_NAME`.
    This is so that we can run multiple `munged` to serve multiple
    authentication domain.

    Examples:
    >>> m0 = Munged(cont) # default domain, use existing key
    >>> m1 = Munged(cont, dom = "dom1", key = 'x'*1024) # custom domain
    ... # using /munge/dom1 directory
    """
    def __init__(self, cont, dom = None, key = None):
        self.cont = cont
        self.dom = dom
        self.key = key
        if dom:
            self.key_file = "/munge/{}/key".format(dom)
            self.pid_file = "/munge/{}/pid".format(dom)
            self.sock_file = "/munge/{}/sock".format(dom)
        else:
            self.key_file = "/etc/munge/munge.key"
            self.pid_file = "/run/munge/munged.pid"
            self.sock_file = "/run/munge/munge.socket.2"

    def _prep_dom(self):
        cont = self.cont
        _dir = "/munge/{}".format(self.dom) if self.dom else "/run/munge"
        rc, out = cont.exec_run("mkdir -m 0755 -p {}".format(_dir))
        if rc:
            raise RuntimeError("Cannot create directory '{}', rc: {}, out: {}"\
                               .format(_dir, rc, out))
        self.cont.chown("munge:munge", _dir)

    def _prep_key_file(self):
        _key = self.key
        if not _key: # key not given
            rc, out = self.cont.exec_run("ls {}".format(self.key_file))
            if rc == 0: # file existed, and no key given .. use existing key
                self.cont.chown("munge:munge", self.key_file)
                return
            _key = "0"*4096 # use default key if key_file not existed
        self.cont.write_file(self.key_file, _key)
        self.cont.chmod(0o600, self.key_file)
        self.cont.chown("munge:munge", self.key_file)

    def get_pid(self):
        """PID of the running munged, `None` if it is not running"""
        rc, out = self.cont.exec_run("cat {}".format(self.pid_file))
        if rc:
            return None
        return int(out)

    def is_running(self):
        """Returns `True` if munged is running"""
        pid = self.get_pid()
        if not pid:
            return False
        rc, out = self.cont.exec_run("ps -p {}".format(pid))
        return rc == 0

    def start(self):
        """Start the daemon"""
        if self.is_running():
            return # do nothing
        rc, out = self.cont.exec_run("munged --help | grep pid-file")
        has_pidfile = (rc == 0)
        self._prep_dom()
        self._prep_key_file()
        cmd = "munged"
        if self.dom:
            cmd += f" -S {self.sock_file} --key-file {self.key_file}"
            if has_pidfile:
                cmd += f" --pid-file {self.pid_file}"
        cmd = f"su -s /bin/bash munge -c '{cmd}'"
        rc, out = self.cont.exec_run(cmd)
        if rc:
            raise RuntimeError("`{}` error, rc: {}, out: {}"\
                               .format(cmd, rc, out))

    def kill(self):
        """Kill the daemon"""
        pid = self.get_pid()
        self.cont.exec_run("kill {}".format(pid))


class Proc(object):
    """Local/Remote process interaction base class"""

    def __init__(self, pid_file, host, ssh_port=22, env=None):
        self.pid_file = pid_file
        self._pid = None
        self.host = host
        self.ssh_port = ssh_port
        self.is_remote = is_remote(host)
        self.env = env
        self.env_cmd = self.getEnvCmd()
        # setup self._exec(_input)
        self._exec = bash if not self.is_remote else \
                     lambda _input: ssh(host=host, port=ssh_port, _input=_input)

    @classmethod
    def fromSpec(cls, data_root, spec):
        _type = spec.get("type")
        if _type == "ldmsd":
            return LDMSDProc(data_root, spec)
        if _type == "munged":
            return MungedProc(data_root, spec)
        if _type == None:
            raise TypeError("'type' not specified")
        raise TypeError("Unknown type: {}".format(_type))

    def comm_validate(self, comm):
        raise NotImplementedError()

    def cmdline_validate(self, cmdline):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def _remote_stop(self):
        script = """
            if ! test -f {pid_file}; then
                echo "pid file not found {pid_file}"
                exit -1
            fi
            PID=$(cat {pid_file})
            if ! test -d /proc/${{PID}}; then
                echo "process not running"
                exit -1
            fi
            kill ${{PID}}
        """.format(**vars(self))
        rc, out = ssh(self.host, port=self.ssh_port, _input=script)
        if rc != 0:
            raise RuntimeError("remote stop error: {}, msg: {}".format(rc, out))

    def stop(self):
        if self.is_remote:
            return self._remote_stop()
        # otherwise, do it locally
        _pid = self.getpid()
        if not _pid:
            logger.info("{} not running".format(self.name))
            return # already running
        os.kill(_pid, signal.SIGTERM)

    def cleanup(self):
        raise NotImplementedError()

    @property
    def pid(self):
        if not self._pid:
            self._pid = self.getpid()
        return self._pid

    def _get_remote_pid(self):
        script = """
            if ! test -f {pid_file}; then
                echo "pid file not found {pid_file}"
                exit -1
            fi
            PID=$(cat {pid_file})
            if ! test -d /proc/${{PID}}; then
                echo "process not running"
                exit -1
            fi
            echo ${{PID}}
            cat /proc/${{PID}}/comm
            cat /proc/${{PID}}/cmdline
        """.format(**vars(self))
        rc, out = ssh(self.host, port=self.ssh_port, _input=script)
        if rc:
            return None
        pid, _comm, _cmdline = out.split("\n", 2)
        _comm = _comm.strip()
        _cmdline = _cmdline.strip()
        if not self.comm_validate(_comm) or \
                not self.cmdline_validate(_cmdline):
            logger.warn("stale pidfile: {}".format(self.pid_file))
            return None
        return int(pid)

    def getpid(self):
        if self.is_remote:
            return self._get_remote_pid()
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

    def getEnvCmd(self):
        if not self.env:
            return ""
        sio = io.StringIO()
        for k, v in self.env.items():
            sio.write('export {k}="{v}"\n'.format(**locals()))
        return sio.getvalue()


class LDMSDProc(Proc):
    """LDMS Daemon Handler"""
    def __init__(self, data_root, spec):
        self.data_root = data_root
        if spec.get("type") != "ldmsd":
            raise ValueError("Expecting a spec with 'type': 'munged'.")
        lstn = spec.get("listen")
        if not lstn:
            raise KeyError("LDMSD spec requires `listen: [ {LISTEN_OBJ} ]` configuration")
        if type(lstn) != list:
            raise ValueError("`listen` must be a `list` of {LISTEN_OBJ}")
        lstn = lstn[0]
        self.spec = spec
        self.host = lstn.get("host")
        if not self.host:
            raise ValueError("Listen objects must specify `host`")
        self.port = lstn.get("port")
        if not self.port:
            raise ValueError("Listen objects must specify `port`")
        self.xprt = lstn.get("xprt", "sock")
        self.name = "{host}-{port}".format(**vars(self))
        _pid_file = "{data_root}/pid/{host}/{name}.pid".format(**vars(self))
        self.ssh_port = spec.get("ssh_port", 22)
        super().__init__(_pid_file, self.host, ssh_port = self.ssh_port, env = spec.get("env"))
        self.conf_file = "{data_root}/conf/{host}/{name}.conf".format(**vars(self))
        self.log_file = "{data_root}/log/{host}/{name}.log".format(**vars(self))
        self.log_level = spec.get("log_level", "ERROR")
        self._conn = None # the LDMS connection
        mem = spec.get("memory")
        self.mem_opt = " -m {}".format(mem) if mem else ""
        self.ldmsd_config = get_ldmsd_config(self.spec)

    def comm_validate(self, comm):
        return comm == "ldmsd"

    def cmdline_validate(self, cmdline):
        return cmdline.find(self.name) >= 0

    def _remote_start(self):
        rc, out = ssh(self.host, port = self.ssh_port, _input = """
            if test -f {pid_file}; then
                PID=$(cat {pid_file})
                if test -d /proc/${{PID}}; then
                    if [[ $(cat /proc/${{PID}}/comm) == ldmsd ]]; then
                        echo "{name} already running (pid ${{PID}})"
                        exit 114 # EALREADY
                    fi
                fi
            fi
            mkdir -p $(dirname {conf_file})
            mkdir -p $(dirname {pid_file})
            mkdir -p $(dirname {log_file})
            cat >{conf_file} <<EOF\n{ldmsd_config}\nEOF
            {env_cmd}
            ldmsd -c {conf_file} -r {pid_file} -l {log_file} \
                    -v {log_level} {mem_opt}
        """.format(**vars(self)))
        if rc == errno.EALREADY:
            logger.info(out)
            return
        if rc:
            raise RuntimeError("remote start error {}, msg: {}".format(rc, out))
        pass

    def start(self):
        if self.is_remote:
            return self._remote_start()
        _pid = self.getpid()
        if _pid:
            logger.info("{} already running (pid {})".format(self.name, _pid))
            return # already running
        ret = bash("""
            mkdir -p $(dirname {conf_file})
            mkdir -p $(dirname {pid_file})
            mkdir -p $(dirname {log_file})
            cat >{conf_file} <<EOF\n{ldmsd_config}\nEOF
            {env_cmd}
            ldmsd -c {conf_file} -r {pid_file} -l {log_file} -v {log_level} {mem_opt}
        """.format(**vars(self)))

    def _remote_cleanup(self):
        rc, out = ssh(self.host, port = self.ssh_port, script = """
            if test -f {pid_file}; then
                PID=$(cat {pid_file})
                if test -d /proc/${{PID}}; then
                    if [[ $(cat /proc/${{PID}}/comm) == ldmsd ]]; then
                        echo "{name} still running (pid ${{PID}})"
                        exit 16 # EBUSY
                    fi
                fi
            fi
            rm -f {conf_file} {pid_file} {log_file}
        """.format(**vars(self)))
        if rc:
            raise RuntimeError("remote cleanup error {}, msg: {}".format(rc, out))

    def cleanup(self):
        if self.is_remote:
            return self._remote_cleanup()
        _pid = self.getpid()
        if _pid:
            raise RuntimeError("Cannot cleanup, {} is running (pid: {})"\
                               .format(self.name, _pid))
        for f in [ self.conf_file, self.pid_file, self.log_file ]:
            try:
                os.unlink(f)
            except:
                pass

    def connect(self, auth=None, auth_opts=None):
        from ovis_ldms import ldms
        if self._conn:
            self._conn.close()
        self._conn = ldms.Xprt(self.xprt, auth=auth, auth_opts=auth_opts)
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
        from ldmsd.ldmsd_communicator import LDMSD_Request, LDMSD_Req_Attr
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
        if not self._conn:
            self.connect()
        return self._conn.dir()


class MungedProc(Proc):
    def __init__(self, data_root, spec):
        # spec should contain "host", "dom", and "key"
        if spec.get("type") != "munged":
            raise ValueError("Expecting a spec with 'type': 'munged'.")
        self.host = spec.get("host")
        if not self.host:
            raise ValueError("spec['host'] is required")
        self.ssh_port = spec.get("ssh_port", 22)
        self.dom = spec.get("dom", "domain0")
        self.key = spec.get("key", "0"*128)
        self.name = "munged-{host}-{dom}".format(**vars(self))
        self.data_root = data_root
        self.data_dir = "{data_root}/{host}/{dom}".format(**vars(self))
        self.key_file = "{data_dir}/key".format(**vars(self))
        self.pid_file = "{data_dir}/pid".format(**vars(self))
        self.sock_file = "{data_dir}/sock".format(**vars(self))
        self.log_file = "{data_dir}/log".format(**vars(self))
        self.seed_file = "{data_dir}/seed".format(**vars(self))
        super().__init__(self.pid_file, self.host, ssh_port = self.ssh_port, env = spec.get("env"))

    def comm_validate(self, comm):
        return comm == "munged"

    def cmdline_validate(self, cmdline):
        return cmdline.find(self.sock_file) >= 0

    def start(self):
        script = """
            if test -f {pid_file}; then
                PID=$(cat {pid_file})
                if test -d /proc/${{PID}}; then
                    if [[ $(cat /proc/${{PID}}/comm) == ldmsd ]]; then
                        echo "{name} already running (pid ${{PID}})"
                        exit 114 # EALREADY
                    fi
                fi
            fi
            umask 0022
            mkdir -p {data_dir}
            cat >{key_file} <<EOF\n{key}\nEOF
            chmod 600 {key_file}
            touch {log_file}
            chmod 600 {log_file}
            {env_cmd}
            munged -S {sock_file} --pid-file {pid_file} --key-file {key_file} \
                    --log-file {log_file} --seed-file {seed_file}
        """.format(**vars(self))
        rc, out = self._exec(script)
        if rc != 0:
            raise RuntimeError("start error {}, output: {}".format(rc, out))

    def cleanup(self):
        script = """
            if test -f {pid_file}; then
                PID=$(cat {pid_file})
                if test -d /proc/${{PID}}; then
                    if [[ $(cat /proc/${{PID}}/comm) == ldmsd ]]; then
                        echo "{name} still running (pid ${{PID}})"
                        exit 16 # EBUSY
                    fi
                fi
            fi
            rm -f {key_file} {pid_file} {sock_file}
        """.format(**vars(self))
        rc, out = self._exec(script)
        if rc != 0:
            raise RuntimeError("cleanup error {}, output: {}".format(rc, out))


# control sequence regex
CS_RE = re.compile("""
(?:
    \x1b\\[       # ESC[ -- the control sequence introducer
    [\x30-\x3f]*  # parameter bytes
    [\x20-\x2f]*  # intermediate bytes
    [\x40-\x7e]   # final byte
)
""", re.VERBOSE)
def cs_rm(s):
    """Remove control sequences from the string `s`"""
    return CS_RE.sub("", s)

def cond_timedwait(cond, timeout=10, interval=0.1):
    """Check `cond` in interval, return True when `cond` is True; return False
       if timeout"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return True
        time.sleep(interval)
    return False

def get_ldmsd_config(spec, ver=None):
    """Generate ldmsd config `str` from given spec"""
    sio = StringIO()
    # process `auth`
    for auth in spec.get("auth", []):
        _a = auth.copy() # shallow copy
        cfgcmd = "auth_add name={}".format(_a.pop("name")) \
                 +"".join([" {}={}".format(k, v) for k,v in _a.items()])\
                 +"\n"
        sio.write(cfgcmd)
    # process `listen`
    for listen in spec.get("listen", []):
        _l = listen.copy() # shallow copy
        cfgcmd = "listen " \
                 +"".join([" {}={}".format(k, v) for k,v in _l.items()])\
                 +"\n"
        sio.write(cfgcmd)
    # process `samplers`
    for samp in spec.get("samplers", []):
        plugin = samp["plugin"]
        interval = samp.get("interval", 2000000)
        if interval != "":
            interval = "interval={}".format(interval)
        offset = samp.get("offset", "")
        if offset != "":
            offset = "offset={}".format(offset)
        if ver and ver >= (4,100,0):
            samp_temp = \
                "load name={plugin}\n" \
                "config name={plugin} {config}\n"
            if samp.get("start"):
                samp_temp += \
                    "smplr_add name={plugin}_smplr instance={plugin} " \
                    "          {interval} {offset}\n" \
                    "smplr_start name={plugin}_smplr\n"
        else:
            samp_temp = \
                "load name={plugin}\n" \
                "config name={plugin} {config}\n"
            if samp.get("start"):
                samp_temp += "start name={plugin} {interval} {offset}\n"
        samp_cfg = samp_temp.format(
            plugin = plugin, config = " ".join(samp["config"]),
            interval = interval, offset = offset
        )

        # replaces all %VAR%
        samp_cfg = re.sub(r'%(\w+)%', lambda m: samp[m.group(1)], samp_cfg)
        sio.write(samp_cfg)
    # process `prdcrs`
    for prdcr in spec.get("prdcrs", []):
        prdcr = deep_copy(prdcr)
        prdcr_add = "prdcr_add name={}".format(prdcr.pop("name"))
        for k, v in prdcr.items():
            prdcr_add += " {}={}".format(k, v)
        sio.write(prdcr_add)
        sio.write("\n")
    # process `config`
    cfg = spec.get("config")
    if cfg:
        for x in cfg:
            sio.write(x)
            sio.write("\n")
    return sio.getvalue()

def assertion_id_get():
    id = 1
    while True:
        yield id
        id += 1

def create_updtr_status(name, interval, offset, state, prdcrs,
                        sync = True, mode = "Pull", auto = False,
                        outstanding = 0, oversampled = 0):
    return {'name' : name,
             'interval' : str(interval),
             'offset' : str(offset),
             'sync' : "true" if sync else "false",
             'mode' : mode,
             'auto' : "true" if auto else "false",
             'state' : state,
             'producers' : prdcrs,
             'outstanding count' : outstanding,
             'oversampled count' : oversampled}

def create_updtr_prdcr_status(name, host, port, xprt, state):
    return {'name' : name,
             'host' : host,
             'port' : int(port),
             'transport' : xprt,
             'state' : state}

# Class for tests to be run insides containers

import pickle
from abc import ABC, abstractmethod

class ContainerTest(ABC):
    """The parent class of a test to be run inside a container

    The test scripts that are intended to be run inside a container 
    must define a _single_ class that inherits from ContainerTest.

    If a test script contains more than one classes that are 
    ContainerTest's children, it will not be run and an error is reported 
    by run_inside_cont_test.py.
    """


    @property
    @abstractmethod
    def test_name(self):
        """The test name"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def test_suite(self):
        """The test suite the test belongs to"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def test_type(self):
        """The test type"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def test_desc(self):
        """The test's description"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def spec(self):
        """The spec oject"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def test_node_name(self):
        """A node (container)'s hostname to run the test script

        The test script author specifies the hostname of a node, listed in the spec object,
        that will run the test script.
        """
        raise NotImplementedError()

    # @property
    # @abstractmethod
    # def assertions(self):
    #     """The list of (assertion ID, assertion name)"""
    #     raise NotImplementedError()

    def get_outdir(self):
        return self._outdir

    def set_outdir(self, outdir):
        self._outdir = outdir

    outdir = property(get_outdir, set_outdir)

    @classmethod
    def add_common_args(cls, parser):
        G.parser = parser
        parser.add_argument("--outdir", type = str,
                            help = "the path to the directory to store the " \
                                   "file containing the result and log messages")

    def __init__(self, outdir = None):
        self._outdir = outdir
        self._assertions = {}

    def load_assertions(self):
        """Load the assertions and their results. Do not overridden

        The script that runs the test scripts calls this method
        to gather the assertions and the results.
        """
        with open(f"{self.outdir}/{self.test_name}.out", 'rb') as fin:
            while True:
                try:
                    yield pickle.load(fin)
                except EOFError:
                    break

    def save_assertion(self, assert_id, cond, cond_str):
        """Save an assertion's result. Do not overridden

        The test script calls the method to save an assertion's result.
        """
        d = {   'assert_id' : assert_id,
                'cond' : cond,
                'cond_str' : cond_str
             }
        # Director in the container
        with open(f"{self.outdir}/{self.test_name}.out", "ab") as fout:
            pickle.dump(d, fout)

    def log(self, msg):
        """Log a message. Do not overridden

        The test script calls the method to log a message.
        """
        with open(f"{self.outdir}/{self.test_name}.log", "a") as fout:
            fout.write(f"{msg}\n")

#####################################################################

if __name__ == "__main__":
    exec(open(os.getenv('PYTHONSTARTUP', '/dev/null')).read())
