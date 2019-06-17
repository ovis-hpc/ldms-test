#!/usr/bin/env python
import LDMS_Test
import time
conts = []
network = LDMS_Test.Network('t2-net')
hosts = {}
cleanup = True
for i in range(0, 4):
    t = LDMS_Test.LDMSD('t2-ldmsd-{0}'.format(i), network,
                        prefix='/opt/tom/ovis', db_root='/DATA15',
                        log_level='DEBUG', config_file='/opt/ovis/etc/sampler-4.2.conf')

    rc = t.start_ldmsd()
    if rc != 0:
        cleanup = False
        continue

    hosts[t.name] = t.ip4_address
    conts.append(t)

time.sleep(4)

# Have each container ldms_ls every other container
for c in conts:
    for h in hosts:
        if h == c.name:
            continue
        print("{0} running ldms_ls to {1}:{2}".format(c.name, h, hosts[h]))
        c.ldms_ls(hosts[h])

if cleanup:
    for c in conts:
        print("Destroying container {0}".format(c.name))
        c.kill()

    print("Removing network {0}".format(network.name))
    network.rm()
