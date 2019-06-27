#!/usr/bin/env python

import os
import sys
import json
import docker

from LDMS_Test import DockerClusterService

_pystart = os.getenv('PYTHONSTARTUP')
if _pystart:
    execfile(_pystart)

def jprint(obj):
    """Pretty print JSON object"""
    print json.dumps(obj, indent=2)

dc = docker.from_env()
c0 = DockerClusterService.get("narate", create = True, nodes = 16,
                    env = {"VAR": "VALUE"},
                    mounts = [
                        "{}/db:/db:rw".format(os.path.realpath(sys.path[0])),
                        "/home/narate/opt/ovis:/opt/ovis:ro",
                    ],
                    node_aliases = {
                        "narate-1": ["login"],
                    })
c1 = DockerClusterService.get("cluster", create = True)

cont = c0.get_container('narate-1')
