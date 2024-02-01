#!/bin/bash -l
#
# SYNOPSIS:
# Remove ${PREFIX}, checkout latest ovis, sos, maestro and build/install them.

PREFIX=${PREFIX:-/opt/ovis}
WORK_DIR=${WORK_DIR:-~/src}

LOG_MSG() {
	echo $( date +"%F %T" ) "$@"
}
export -f LOG_MSG

ERROR() {
	LOG_MSG "ERROR:" "$@"
}
export -f ERROR

INFO() {
	LOG_MSG "INFO:" "$@"
}
export -f INFO

set -e

INFO "WORK_DIR: ${WORK_DIR}"
mkdir -p ${WORK_DIR}

MAESTRO_COMMIT=${MAESTRO_COMMIT:-}

SOS_BUILD_OPTS=(
	--prefix=${PREFIX}
	--enable-python
	CFLAGS="-Wall -Werror -O0 -ggdb3"
)

OVIS_BUILD_OPTS=(
	--prefix=${PREFIX}
	--enable-python

	# test stuff
	--enable-ldms-test
	--enable-zaptest
	--enable-test_sampler
	--enable-list_sampler
	--enable-record_sampler

	--enable-munge

	--enable-sos
	--with-sos=${PREFIX}

	# xprt
	--enable-rdma
	--enable-fabric
	--with-libfabric=/usr

	# app stuff
	--enable-store-app
	--enable-app-sampler

	# etc and doc
	--enable-etc
	--enable-doc
	--enable-doc-man

	# kafka
	--with-kafka

	CFLAGS="-O0 -ggdb3"
)

pushd ${WORK_DIR}

export OVIS_REPO=${OVIS_REPO:-https://github.com/ovis-hpc/ovis}
export OVIS_BRANCH=${OVIS_BRANCH:-OVIS-4}
export SOS_REPO=${SOS_REPO:-https://github.com/ovis-hpc/sos}
export SOS_BRANCH=${SOS_BRANCH:-SOS-6}
export MAESTRO_REPO=${MAESTRO_REPO:-https://github.com/ovis-hpc/maestro}
export MAESTRO_BRANCH=${MAESTRO_BRANCH:-master}

INFO "== Checking out SOS =="
[[ -d sos ]] || {
	mkdir sos
	pushd sos
	git init .
	git remote add github ${SOS_REPO}
	git fetch github --tags
	git fetch github ${SOS_BRANCH}
	git checkout FETCH_HEAD
	popd
}
INFO "== Checking out OVIS =="
[[ -d ovis ]] || {
	mkdir ovis
	pushd ovis
	git init .
	git remote add github ${OVIS_REPO}
	git fetch github --tags
	git fetch github ${OVIS_BRANCH}
	git checkout FETCH_HEAD
	popd
}
INFO "== Checkout maestro =="
[[ -d maestro ]] || {
	mkdir maestro
	pushd maestro
	git init .
	git remote add github ${MAESTRO_REPO}
	git fetch github --tags
	git fetch github ${MAESTRO_BRANCH}
	git checkout FETCH_HEAD
	popd
}

INFO "Purging /opt/ovis/"
sudo rm -rf /opt/ovis/* # we want to keep the directory, just purge stuff inside

BUILD_DIR="build-${HOSTNAME}"

INFO "== Building/Installing SOS =="
pushd sos
./autogen.sh 2>&1
mkdir -p ${BUILD_DIR}
pushd ${BUILD_DIR}
../configure "${SOS_BUILD_OPTS[@]}" 2>&1
make
sudo make install
popd # back to sos
popd # back to ${WORK_DIR}

INFO "== Building/Installing OVIS =="
pushd ovis
./autogen.sh 2>&1
mkdir -p ${BUILD_DIR}
pushd ${BUILD_DIR}
../configure "${OVIS_BUILD_OPTS[@]}" 2>&1
make
sudo make install
popd # back to ovis
popd # back to ${WORK_DIR}

INFO "== Installing maestro =="
pushd maestro
sudo pip3 install --prefix ${PREFIX} .
popd
