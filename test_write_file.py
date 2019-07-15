#!/usr/bin/env python

import os
import sys
from LDMS_Test import DockerCluster

if __name__ != "__main__":
    raise RuntimeError("This is not a module.")

execfile(os.getenv("PYTHONSTARTUP", "/dev/null"))

USER = os.getlogin()
CLUSTERNAME = "{}-test-write-file".format(USER)

cluster = DockerCluster.get(CLUSTERNAME, create = True, nodes = 1)

[cont] = cluster.containers

def test_write_read(cont, path, txt):
    cont.write_file(path, txt)
    rtxt = cont.read_file(path)
    assert(txt == rtxt)

# Simple short write
test_write_read(cont, "/tmp/file0", "Short Write")

# 4k write
txt = "0" * 4096
test_write_read(cont, "/tmp/file1", txt)

# 1M write
txt = "0" * (1024*1024)
test_write_read(cont, "/tmp/file2", txt)

cluster.remove()
