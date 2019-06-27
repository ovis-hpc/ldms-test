#!/usr/bin/env python

import os
import sys
import docker

def Service_task_containers(self):
    """Get containers of all tasks of the service"""
    tasks = self.tasks()
    cont = []
    for t in tasks:
        nid = t['NodeID']
        node = self.client.nodes.get(nid)
        addr = node.attrs['Description']['Hostname'] + ":2375"
        # client to remote dockerd
        ctl = docker.client.from_env(environment={'DOCKER_HOST': addr})
        cont_id = t['Status']['ContainerStatus']['ContainerID']
        cont.append(ctl.containers.get(cont_id))
    return cont

# bind to Service class
docker.models.services.Service.task_containers = Service_task_containers

_pystart = os.getenv('PYTHONSTARTUP')
if _pystart:
    execfile(_pystart)

C = docker.from_env()
s = C.services.get("narate")
