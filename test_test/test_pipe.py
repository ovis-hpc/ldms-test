#!/usr/bin/python3

import os
import sys

from LDMS_Test import DockerCluster

exec(open(os.getenv("PYTHONSTARTUP", "/dev/null")).read())

cluster = DockerCluster.get(
            name = "test_pipe",
            create = True,
            nodes = 1,
          )
running = cluster.wait_running()
assert(running)

cont = cluster.containers[0]

text = "Verification"
rc, out = cont.pipe("cat", text)

op = "==" if text == out else "!="
msg = "{} {} {}".format(text, op, out)
print(msg)
assert(text == out)
cluster.remove()
