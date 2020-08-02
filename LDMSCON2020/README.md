LDMSCON2020
===========

This directory contains `cluster` script that manages the cluster of containers.
There are other script files and data files mentioned in this docuemnt
that are intentionally not checked into the repository for privacy and
security purposes.

The instructions in this document is prepared specifically for `ovs-5416`
machine that is used for hosting LDMSCON2020 cluster, but the script
itself is generic and can be used on any Linux machine. The `cluster`
script depends on docker and docker swarm setup that can be found here:
[Docker Setup](../README.md#docker-setup).

TOC:
- [build image](#build-image)
- [create cluster](#create-cluster)
- [recover cluster](#recover-cluster)
- [delete clsuter](#delete-cluster)
- [container access](#container-access)

build image
-----------
```sh
# Need to build ovis-centos-build first
$ cd /opt/ldms-test/docker
$ docker build -t ovis-centos-build - < Dockerfile
# This will take a while

# Then build ldmscon2020 image.
# If you need to install additional packages on ldmscon2020
# image, please see yum command example in
# ldms-test/docker/Dockerfile and add something
# similar to ldms-test/LDMSCON2020/Dockerfile
$ cd /opt/ldms-test/LDMSCON2020
$ docker build -t ldmscon2020 - < Dockerfile

```

create cluster
--------------
```sh
# for ovs-5416
$ cd /opt/ldms-test/LDMSCON2020
$ ./create-cluster.sh # This file existed only on ovs-5416

# or
$ cd /opt/ldms-test/LDMSCON2020
$ CLUSTER_HOME=/SOME/WHERE/OVER/THE/RAINBOW
$ mkdir -p $CLUSTER_HOME
$ ./cluster create \
      --home $CLUSTER_HOME \
      --ndoes 64 \
      --user-file users-example.csv \
      --ssh-bind-addr HOST_IP_ADDR_FOR_HEADHODE_SSH \
      --timezone America/Chicago
```

This is an example of creating a 64-node cluster with a headnode. 
If we don't want to bind the SSH of the headnode to the hosting
machine, drop the `--ssh-bind-addr` option. 65 containers
(1 headnode, node-1..node-64) will be created and started.


recover cluster
---------------
In the event of docker service restart (manually, 
system restart, or by power failure), the containers 
will become `stopped` and they can be started and
recover. Use the following command to check and
recover the containers.

```sh
# on ovs-5416
$ cd /opt/ldms-test/LDMSCON2020
$ ./check-recover-cluster.sh

# or on other machine
$ cd /opt/ldms-test/LDMSCON2020
$ ./cluster check-recover

# checking could take roughly 5 sec,
# if a recovery is needed, it will
# take longer.
```

delete cluster
--------------
Deleting cluster means all containers will be removed.
The changes made outside of `/home` will be blown away.
This is required if the image has been rebuild and
we want the changes on the image to appear on the
cluster.

```sh
$ cd /opt/ldms-test/LDMSCON2020
$ ./cluster delete
```

container access
----------------
This is useful for debugging, or when SSH is
not available.
```sh
$ docker exec -it ldmscon2020-headnode bash
# replace ldmscon2020-headnode for the other node
# you want to access.
```
