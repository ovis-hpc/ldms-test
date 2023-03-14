#!/usr/bin/env python3

import argparse

from ldmsd.ldmsd_config import ldmsdInbandConfig as Ctrl
from ldmsd.ldmsd_communicator import  LDMSD_Request as Req
from ldmsd.ldmsd_communicator import LDMSD_Req_Attr as ReqAttr
import sys

def parse_auth_args(args):
    if args is None:
        return None
    auth_opt = {}
    for x in args:
        n, v = x.split('=')
        auth_opt[n] = v
    return auth_opt

def __check_command_syntax(self, attr_value):
    tokens = attr_value.split(" ")
    for tk in tokens:
        if tk.endswith("="):
            return False
    return True

def reqNew(cmd):
    args = None
    try:
        verb, args = cmd.split(' ', 1)
    except:
        verb = cmd

    attr_list = []
    if args:
        attr_s = []
        attr_str_list = args.split()

        for attr_str in attr_str_list:
            name = None
            value = None
            try:
                name, value = attr_str.split("=")
            except ValueError:
                # keyword
                name = attr_str.split("=")
            except:
                raise
            if (verb == "config" and name != "name") or (verb == "env"):
                attr_s.append(attr_str)
            elif (verb == "auth_add" and name not in ["name", "plugin"]):
                attr_s.append(attr_str)
            else:
                try:
                    attr = ReqAttr(value = value, attr_name = name)
                except KeyError:
                    attr_s.append(attr_str)
                except Exception:
                    raise
                else:
                    attr_list.append(attr)

        if len(attr_s) > 0:
            attr_str = " ".join(attr_s)
            attr = ReqAttr(value = attr_str, attr_id = LDMSD_Req_Attr.STRING)
            attr_list.append(attr)
    return Req(command = verb, attrs = attr_list)

def communicate(ctrl, cmd):
    r = reqNew(cmd)
    r.send(ctrl)
    return r.receive(ctrl)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description = "Send a configuration request " \
                                         "to an LDMSD and print the result to stdout")
        parser.add_argument('-x', '--xprt', help = "LDMSD listen transport")
        parser.add_argument('-p', '--port', help = "LDMSD listen port")
        parser.add_argument('-H', '--host', help = "LDMSD host")
        parser.add_argument('-a', '--auth', help = "Authentication method")
        parser.add_argument('-A', '--auth_args', help = "Authentication method arguments",
                            action = "append")
        parser.add_argument('-c', '--cmd', help = "Configuration command as given in ldmsd_controller")
        args = parser.parse_args()

        ctrl = Ctrl(host = args.host, xprt = args.xprt, port = args.port,
                    auth = args.auth, auth_opt = parse_auth_args(args.auth_args))

        resp = communicate(ctrl, args.cmd)
        print(resp['msg'])
        sys.exit(resp['errcode'])
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        raise