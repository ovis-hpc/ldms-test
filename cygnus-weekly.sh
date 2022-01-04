#!/bin/bash
#
# WARNINGS:
# - This script uses `sudo` to purge /opt/ovis/ directory.
# - This script installs OVIS-4 at /opt/ovis/ on the host.
# - This script kill all running virtual clusters.
# - This script make a soft link in /etc/ld.so.conf.d/
# - This script purge $WORK_DIR/
#
# DESCRIPTION
# -----------
# This script is meant to be run on cygnus cluster at OGC. The following is an
# overview of what this script does:
# - Kill all running virtual clusters
# - Checkout OVIS-4 and SOS from github
# - (sudo) purge /opt/ovis on the host
# - build and (sudo) install SOS and OVIS-4 in /opt/ovis on the host
# - run `direct_*_test` test cases
# - run containerized test cases
#
#
# ENVIRONMENT VARIABLES
# ---------------------
# - `FORCE_TEST=0|1`:
#    By default, if the git SHA of the newly checked out OVIS-4 is
#    still the same as the installation in /opt/ovis, the script will end
#    without running the test. To force the test, set environment variable
#    `FORCE_TEST=1`.
#
# - `FORCE_BUILD=0|1`:
#    By default, if the git SHA of the newly checked out OVIS-4 is still the
#    same as the installation in /opt/ovis, the script won't build/install the
#    new binaries.  To force the build, set environment variable
#    `FORCE_BUILD=1`.
#
# - `GITHUB_REPORT=0|1`:
#   Set `GITHUB_REPORT=0` to skip reporting test results to github (e.g. for
#   debugging). By default, `GITHUB_REPORT` is 1.
#
#
# REQUIREMENTS
# ------------
# - you must be able to `sudo`
# - if you use singularity, make sure that you build and own the singularity
#   image, and you have to be in the /etc/subuid and /etc/subgid list. See
#   details in "Singularity Cluster Set" section in README.md.

export PATH=/opt/ovis/sbin:/opt/ovis/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
export PYTHONPATH=$( echo /opt/ovis/lib/python*/site-packages )
export GITHUB_REPORT=${GITHUB_REPORT:-1}

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

assert() {
	"$@" || {
		ERROR "cmd: '$*' failed"
		exit -1
	}
}
export -f assert

SCRIPT_DIR=$(realpath $(dirname $0))

WORK_DIR=${WORK_DIR:-/mnt/cygnus/data/$(date +"%F-%H%M%S")}
WORK_DIR=$(realpath ${WORK_DIR})
export WORK_DIR

INFO "Purging workdir: ${WORK_DIR}"
rm -rf ${WORK_DIR}
DATA_ROOT=${WORK_DIR}/data
assert mkdir -p ${DATA_ROOT}

LOG=${LOG:-${WORK_DIR}/cygnus-weekly.log}
D=$(dirname $LOG)
assert mkdir -p $D

PREFIX=/opt/ovis

MAESTRO_COMMIT=${MAESTRO_COMMIT:-78af06a}

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

	CFLAGS="-Wall -Werror -O0 -ggdb3"
)

#### subshell that print to both stdout and log file ####
{
INFO "WORK_DIR: ${WORK_DIR}"
INFO "LOG: ${LOG}"

pushd ${SCRIPT_DIR} # it is easier to call scripts from the script dir

set -e
# remove existing clusters
./remove_cluster --all

NEW_GIT_SHA=( $( git ls-remote https://github.com/ovis-hpc/ovis OVIS-4 ) )
OLD_GIT_SHA=$( [[ -x /opt/ovis/sbin/ldmsd ]] && {
		/opt/ovis/sbin/ldmsd -V | grep git | sed 's/git-SHA: //'
	} || echo "" )

INFO "NEW_GIT_SHA: ${NEW_GIT_SHA}"
INFO "OLD_GIT_SHA: ${OLD_GIT_SHA}"

pushd ${WORK_DIR}
if [[ "$NEW_GIT_SHA" != "$OLD_GIT_SHA" ]] || [[ "${FORCE_BUILD}0" -gt 0 ]]; then

	INFO "== Checking out SOS =="
	git clone https://github.com/ovis-hpc/sos
	INFO "== Checking out OVIS =="
	git clone https://github.com/ovis-hpc/ovis
	INFO "== Checkout maestro =="
	git clone https://github.com/ovis-hpc/maestro
	[[ -z "${MAESTRO_COMMIT}" ]] || {
		INFO "checking out maestro commit id: ${MAESTRO_COMMIT}"
		pushd maestro
		git checkout ${MAESTRO_COMMIT}
		popd
	}

	INFO "Purging /opt/ovis/"
	sudo rm -rf /opt/ovis/* # we want to keep the directory, just purge stuff inside

	INFO "== Building/Installing SOS =="
	pushd sos
	./autogen.sh 2>&1
	mkdir -p build
	pushd build
	../configure "${SOS_BUILD_OPTS[@]}" 2>&1
	make
	sudo make install
	popd # back to sos
	popd # back to ${WORK_DIR}

	INFO "== Building/Installing OVIS =="
	pushd ovis
	./autogen.sh 2>&1
	mkdir -p build
	pushd build
	../configure "${OVIS_BUILD_OPTS[@]}" 2>&1
	make
	sudo make install
	popd # back to ovis
	popd # back to ${WORK_DIR}

	INFO "== Installing maestro =="
	sudo cp maestro/*.py maestro/maestro* ${PREFIX}/bin
else
	INFO "skip building because GIT SHA has not changed: ${OLD_GIT_SHA}"
fi

if [[  "$NEW_GIT_SHA" == "$OLD_GIT_SHA" ]] && [[ "${FORCE_TEST}0" -eq 0 ]]; then
	INFO "Skip the test. SHA does not change: ${NEW_GIT_SHA}"
	INFO "----------------------------------------------"
	exit 0
fi

[[ "${GITHUB_REPORT}0" -eq 0 ]] || ${SCRIPT_DIR}/github-report.sh # test start report

set +e
INFO "==== OVIS+SOS Installation Completed ===="


INFO "==== Start batch testing ===="

pushd ${SCRIPT_DIR} # it is easier to call scripts from the script dir

TEST_OPTS=(
	--prefix ${PREFIX}
	--src ${WORK_DIR}
)

[[ -z "${FAIL_FAST}" ]] || set -e

declare -A RCS

for T in direct_ldms_ls_conn_test direct_prdcr_subscribe_test; do
	INFO "======== ${T} ========"
	CMD="python3 ${T} ${TEST_OPTS[@]} --data_root ${DATA_ROOT}/${T}"
	INFO "CMD: ${CMD}"
	${CMD}
	RCS["$T"]=$?
	INFO "----------------------------------------------"
done 2>&1

[[ -z "${SKIP_PAPI}" ]] && {
	PAPI_LIST=(
		agg_slurm_test
		papi_sampler_test
		papi_store_test
		store_app_test
		syspapi_test
	)
} || {
	PAPI_LIST=
}

LIST=(
${PAPI_LIST[*]}
agg_test
failover_test
ldmsd_auth_ovis_test
ldmsd_auth_test
ldmsd_ctrl_test
ldmsd_stream_test
maestro_cfg_test
mt-slurm-test
ovis_ev_test
prdcr_subscribe_test
set_array_test
setgroup_test
slurm_stream_test
spank_notifier_test
ldms_list_test
quick_set_add_rm_test
set_array_hang_test
ldmsd_autointerval_test
)

for T in ${LIST[*]}; do
	INFO "======== ${T} ========"
	CMD="python3 ${T} ${TEST_OPTS[@]} --data_root ${DATA_ROOT}/${T}"
	INFO "CMD: ${CMD}"
	${CMD}
	RCS["$T"]=$?
	sleep 10 # allow some clean-up break between tests
	INFO "----------------------------------------------"
	./remove_cluster --all
	rm -rf ${DATA_ROOT}/${T}
done 2>&1

export RED='\033[01;31m'
export GREEN='\033[01;32m'
export RESET='\033[0m'
INFO "==== Summary ===="
N=${#RCS[*]}
PASSED=0
FAILED=0
for K in "${!RCS[@]}"; do
	V=${RCS["$K"]}
	if (( $V == 0 )); then
		PASSED=$((PASSED+1))
		V="${GREEN}PASSED${RESET}"
	else
		FAILED=$((FAILED+1))
		V="${RED}FAILED${RESET}"
	fi
	echo -e "${K}: ${V}"
done
echo "------------------------------------------"
echo -e "Total tests passed: ${PASSED}/${N}"
echo "------------------------------------------"
} | tee ${LOG}

[[ "${GITHUB_REPORT}0" -eq 0 ]] || ${SCRIPT_DIR}/github-report.sh # make a test end report
