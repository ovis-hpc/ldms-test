#!/bin/bash -l
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
# - `SKIP_BUILD=0|1`:
#   Force skip the build, default :0. This precedes FORCE_BUILD.
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

WORK_DIR_POOL=${WORK_DIR_POOL:-/mnt/300G/data}
WORK_DIR=${WORK_DIR:-${WORK_DIR_POOL}/$(date +"%F-%H%M%S")}
CONT_PREFIX=${WORK_DIR_POOL}/opt-ovis
INFO "WORK_DIR: ${WORK_DIR}"
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

NEW_GIT_SHA=( $( git ls-remote https://github.com/ovis-hpc/ovis OVIS-4 ) )
OLD_GIT_SHA=$( [[ -x /opt/ovis/sbin/ldmsd ]] && {
		/opt/ovis/sbin/ldmsd -V | grep git | sed 's/git-SHA: //'
	} || echo "" )

INFO "NEW_GIT_SHA: ${NEW_GIT_SHA}"
INFO "OLD_GIT_SHA: ${OLD_GIT_SHA}"

export NEW_GIT_SHA
export OLD_GIT_SHA
export OVIS_GIT_SHA=${NEW_GIT_SHA}

#### subshell that print to both stdout and log file ####
{
INFO "WORK_DIR: ${WORK_DIR}"
INFO "LOG: ${LOG}"

pushd ${SCRIPT_DIR} # it is easier to call scripts from the script dir

set -e
# remove existing clusters
./remove_cluster --all

# build on host
pushd ${WORK_DIR}
if [[ "$SKIP_BUILD" -ne 0 ]]; then
	INFO "Force-skip building on host (SKIP_BUILD: ${SKIP_BUILD})"
elif [[ "$NEW_GIT_SHA" != "$OLD_GIT_SHA" ]] || [[ "${FORCE_BUILD}0" -gt 0 ]]; then
	INFO "==== start building on host ===="
	${SCRIPT_DIR}/scripts/clean-build.sh
else
	INFO "Skip building on host because GIT SHA has not changed: ${OLD_GIT_SHA}"
fi

if [[ "$SKIP_BUILD" -ne 0 ]]; then
	INFO "Force-skip building containerized binary (SKIP_BUILD: ${SKIP_BUILD})"
elif [[ ! -f ${CONT_PREFIX}/sbin/ldmsd ]] ||
     ! strings ${CONT_PREFIX}/sbin/ldmsd | grep ${NEW_GIT_SHA}; then
	TS=$(date +%s)
	NAME="build-${TS}"

	mkdir -p ${CONT_PREFIX}
	sudo rm -rf ${CONT_PREFIX}/*
	# build inside the container
	INFO "==== start building in a container ===="
	docker run --rm -i --name ${NAME} --hostname ${NAME} \
			-e WORK_DIR \
			-v ${CONT_PREFIX}:/opt/ovis:rw \
			-v ${SCRIPT_DIR}:${SCRIPT_DIR}:ro \
			ovishpc/ovis-centos-build \
			${SCRIPT_DIR}/scripts/clean-build.sh
else
	INFO "Skip building containerized binary because GIT SHA has not changed: ${OLD_GIT_SHA}"
fi

export PYTHONPATH=$( echo /opt/ovis/lib/python*/site-packages )

INFO "-- Installation process succeeded --"
INFO "---------------------------------------------------------------"

if [[  "$NEW_GIT_SHA" == "$OLD_GIT_SHA" ]] && [[ "${FORCE_TEST}0" -eq 0 ]]; then
	INFO "Skip the test. SHA does not change: ${NEW_GIT_SHA}"
	INFO "----------------------------------------------"
	exit 0
fi

[[ "${GITHUB_REPORT}0" -eq 0 ]] || ${SCRIPT_DIR}/github-report.sh # test start report

set +e
INFO "==== OVIS+SOS Installation Completed ===="

source ${PREFIX}/etc/profile.d/set-ovis-variables.sh

INFO "==== Start batch testing ===="

pushd ${SCRIPT_DIR} # it is easier to call scripts from the script dir

TEST_OPTS=(
	--prefix ${PREFIX}
	--src ${WORK_DIR_POOL}
)

[[ -z "${FAIL_FAST}" ]] || set -e

declare -A RCS

source ${SCRIPT_DIR}/test-list.sh
# This defines DIRECT_TEST_LIST, CONT_TEST_LIST, PAPI_CONT_TEST_LIST

for T in ${DIRECT_TEST_LIST[@]}; do
	INFO "======== ${T} ========"
	CMD="python3 ${T} ${TEST_OPTS[@]} --data_root ${DATA_ROOT}/${T}"
	INFO "CMD: ${CMD}"
	${CMD}
	RCS["$T"]=$?
	INFO "----------------------------------------------"
done 2>&1

[[ -z "${SKIP_PAPI}" ]] && {
	LIST=( ${PAPI_CONT_TEST_LIST[*]} ${CONT_TEST_LIST[*]} )
} || {
	LIST=( ${CONT_TEST_LIST[*]} )
}

TEST_OPTS=(
	--prefix ${CONT_PREFIX}
	--src ${WORK_DIR_POOL}
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
} |& tee ${LOG}

[[ "${GITHUB_REPORT}0" -eq 0 ]] || ${SCRIPT_DIR}/github-report.sh # make a test end report
