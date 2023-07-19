#!/bin/bash -l
#
# Similar to cygnus-weekly.sy, but with ovishpc/ldms-* container build and test.
#
# REQUIREMENTS
# - This script needs "https://github.com/ovis-hpc/ldms-containers" to be cloned
#   to "${PWD}/ldms-containers"
#
# WARNINGS:
# - This scirpt modifies ldms-containers/config.sh file.
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
# - `DOCKER_PUSH=0|1`
#   Set `DOCKER_PUSH=0` to skip pusing ovishpc/ldms-* images to docker hub at
#   the end of the test (must pass all tests). The default is 1.
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

if [[ -d "/sys/class/infiniband" ]]; then
	HAVE_RDMA=y
else
	HAVE_RDMA=
fi

SCRIPT_DIR=$(realpath $(dirname $0))
LDMS_CONTAINERS_DIR=${LDMS_CONTAINERS_DIR:-${SCRIPT_DIR}/ldms-containers}

[[ -f "${LDMS_CONTAINERS_DIR}/config.sh" ]] || {
	ERROR "${LDMS_CONTAINERS_DIR}/config.sh not found. " \
		"cygnus-weekly-cont.sh needs " \
		"the ldms-containers repository to build containers. " \
		"LDMS_CONTAINERS_DIR environment variable may be set to point "\
		"to the non-default location."
	exit -1
}

WORK_DIR_POOL=${WORK_DIR_POOL:-/mnt/300G/data}
WORK_DIR=${WORK_DIR:-${WORK_DIR_POOL}/$(date +"%F-%H%M%S")}

CONT_OVIS=${CONT_OVIS:-${LDMS_CONTAINERS_DIR}/ovis}
CONT_ROOT=${CONT_ROOT:-${LDMS_CONTAINERS_DIR}/root}

INFO "WORK_DIR: ${WORK_DIR}"
WORK_DIR=$(realpath ${WORK_DIR})
export WORK_DIR

export PASSED_FILE=${WORK_DIR}/passed
export FAILED_FILE=${WORK_DIR}/failed
export TEST_LOG_DIR=${WORK_DIR}/test-log
export SUMMARY_FILE=${WORK_DIR}/summary.md
export DOCKER_PUSH=${DOCKER_PUSH:-1}

INFO "Purging workdir: ${WORK_DIR}"
rm -rf ${WORK_DIR}
DATA_ROOT=${WORK_DIR}/data
assert mkdir -p ${DATA_ROOT}

LOG=${LOG:-${WORK_DIR}/cygnus-weekly.log}
D=$(dirname $LOG)
assert mkdir -p $D

# PREFIX for host
PREFIX=/opt/ovis

CFG=${SCRIPT_DIR}/cygnus-weekly-cont-config.sh
if [[ -e ${CFG} ]]; then
	source ${CFG}
else
	INFO "Using default parameters, config file does not exists: ${CFG}"
fi

export OVIS_REPO=${OVIS_REPO:-https://github.com/ovis-hpc/ovis}
export OVIS_BRANCH=${OVIS_BRANCH:-OVIS-4}
export SOS_REPO=${SOS_REPO:-https://github.com/ovis-hpc/sos}
export SOS_BRANCH=${SOS_BRANCH:-SOS-6}
export MAESTRO_REPO=${MAESTRO_REPO:-https://github.com/ovis-hpc/maestro}
export MAESTRO_BRANCH=${MAESTRO_BRANCH:-master}

OVIS_NEW_GIT_SHA=( $( git ls-remote ${OVIS_REPO} ${OVIS_BRANCH} ) )
OVIS_OLD_GIT_SHA=$( [[ -x /opt/ovis/sbin/ldmsd ]] && {
		/opt/ovis/sbin/ldmsd -V | grep git | sed 's/git-SHA: //'
	} || echo "" )

# Probe container stuff
if [[ -f "${CONT_OVIS}/sbin/ldmsd" ]] ; then
	CONT_GIT_SHA=$( strings "${CONT_OVIS}/sbin/ldmsd" | grep -E '^[0-9a-f]{40}$' )
else
	CONT_GIT_SHA=""
fi

export OVIS_NEW_GIT_SHA
export OVIS_OLD_GIT_SHA
export CONT_GIT_SHA
export OVIS_GIT_SHA=${OVIS_NEW_GIT_SHA}

# Get the ldms-test repo's old and new git sha
LDMS_TEST_REPO=${LDMS_TEST_REPO:-https://github.com/ovis-hpc/ldms-test}
LDMS_TEST_BRANCH=${LDMS_TEST_BRANCH:-master}
LDMS_TEST_NEW_GIT_SHA=( $( git ls-remote ${LDMS_TEST_REPO} ${LDMS_TEST_BRANCH} ) )
LDMS_TEST_OLD_GIT_SHA=$( git rev-parse ${LDMS_TEST_BRANCH} )
LDMS_TEST_GIT_SHA=${LDMS_TEST_NEW_GIT_SHA}

export LDMS_TEST_NEW_GIT_SHA
export LDMS_TEST_OLD_GIT_SHA
export LDMS_TEST_GIT_SHA

export PASSED=0
export FAILED=0

set -o pipefail

#### subshell that print to both stdout and log file ####
{
INFO "WORK_DIR: ${WORK_DIR}"
INFO "LOG: ${LOG}"

INFO "OVIS_NEW_GIT_SHA: ${OVIS_NEW_GIT_SHA}"
INFO "OVIS_OLD_GIT_SHA: ${OVIS_OLD_GIT_SHA}"
INFO "CONT_GIT_SHA: ${CONT_GIT_SHA}"

INFO "-----------------------------------------------"
INFO "LDMS_TEST_REPO: ${LDMS_TEST_REPO}"
INFO "LDMS_TEST_BRANCH: ${LDMS_TEST_BRANCH}"
INFO "LDMS_TEST_NEW_GIT_SHA: ${LDMS_TEST_NEW_GIT_SHA}"
INFO "LDMS_TEST_OLD_GIT_SHA: ${LDMS_TEST_OLD_GIT_SHA}"

pushd ${SCRIPT_DIR} # it is easier to call scripts from the script dir

set -e
set -o pipefail
# remove existing clusters
./remove_cluster --all

# build on host
pushd ${WORK_DIR}
if [[ "$SKIP_BUILD" -ne 0 ]]; then
	INFO "Force-skip building on host (SKIP_BUILD: ${SKIP_BUILD})"
elif [[ -z "$HAVE_RDMA" ]]; then
	INFO "Does not have RDMA, skip building on host"
elif [[ "$OVIS_NEW_GIT_SHA" != "$OVIS_OLD_GIT_SHA" ]] || [[ "${FORCE_BUILD}0" -gt 0 ]]; then
	INFO "==== start building on host ===="
	${SCRIPT_DIR}/scripts/clean-build.sh
else
	INFO "Skip building on host because GIT SHA has not changed: ${OLD_GIT_SHA}"
fi

# build containers
if [[ "$SKIP_BUILD" -ne 0 ]]; then
	INFO "Force-skip building containerized binary (SKIP_BUILD: ${SKIP_BUILD})"
elif [[ ! -f ${CONT_OVIS}/sbin/ldmsd ]] ||
     [[ "${FORCE_BUILD}0" -gt 0 ]] ||
     ! strings ${CONT_OVIS}/sbin/ldmsd | grep ${OVIS_NEW_GIT_SHA} ; then
	pushd ${LDMS_CONTAINERS_DIR}
	sed -i "s|^\\s*OVIS=.*|OVIS=${CONT_OVIS}|" config.sh
	sed -i "s|^\\s*OVIS_REPO=.*|OVIS_REPO=${OVIS_REPO}|" config.sh
	sed -i "s|^\\s*OVIS_BRANCH=.*|OVIS_BRANCH=${OVIS_BRANCH}|" config.sh
	sed -i "s|^\\s*SOS_REPO=.*|SOS_REPO=${SOS_REPO}|" config.sh
	sed -i "s|^\\s*SOS_BRANCH=.*|SOS_BRANCH=${SOS_BRANCH}|" config.sh
	sed -i "s|^\\s*MAESTRO_REPO=.*|MAESTRO_REPO=${MAESTRO_REPO}|" config.sh
	sed -i "s|^\\s*MAESTRO_BRANCH=.*|MAESTRO_BRANCH=${MAESTRO_BRANCH}|" config.sh
	${LDMS_CONTAINERS_DIR}/scripts/build-all.sh
	popd # ${LDMS_CONTAINERS_DIR}
else
	INFO "Skip building containerized binary because GIT SHA has not changed: ${OLD_GIT_SHA}"
fi

export PYTHONPATH=$( echo /opt/ovis/lib/python*/site-packages )

INFO "-- Installation process succeeded --"
INFO "---------------------------------------------------------------"

if [[  "$OVIS_NEW_GIT_SHA" == "$CONT_GIT_SHA" ]] && [[ "${FORCE_TEST}0" -eq 0 ]]; then
	INFO "Skip the test. SHA does not change: ${OVIS_NEW_GIT_SHA}"
	INFO "----------------------------------------------"
	exit 0
fi

[[ "${GITHUB_REPORT}0" -eq 0 ]] || ${SCRIPT_DIR}/github-report.sh # test start report

set +e
INFO "==== OVIS+SOS Installation Completed ===="

[[ -z "$HAVE_RDMA" ]] || source ${PREFIX}/etc/profile.d/set-ovis-variables.sh

INFO "==== Start batch testing ===="

mkdir -p ${TEST_LOG_DIR}

pushd ${SCRIPT_DIR} # it is easier to call scripts from the script dir

TEST_OPTS=(
	--prefix ${PREFIX}
	--src ${WORK_DIR_POOL}
)

[[ -z "${FAIL_FAST}" ]] || set -e

declare -A RCS

source ${SCRIPT_DIR}/test-list.sh
# This defines DIRECT_TEST_LIST, CONT_TEST_LIST, PAPI_CONT_TEST_LIST

if [[ -z "$HAVE_RDMA" ]]; then
	# Does not have RDMA, do not execute the "direct" tests.
	DIRECT_TEST_LIST=( )
fi

append_summary() {
	local K=$1
	local RC=$2
	if (( $RC == 0 )); then
		V_TEX='$\\textcolor{lightgreen}{\\text{PASSED}}$'
	else
		V_TEX='$\\textcolor{red}{\\text{FAILED}}$'
	fi
	echo -e "* [${K}](test-log/${K}.log): ${V_TEX}" >> ${SUMMARY_FILE}
}

echo -e "Test Summary" > ${SUMMARY_FILE}
echo -e "============" >> ${SUMMARY_FILE}

for T in ${DIRECT_TEST_LIST[@]}; do
	INFO "======== ${T} ========"
	CMD="python3 ${T} ${TEST_OPTS[@]} --data_root ${DATA_ROOT}/${T}"
	INFO "CMD: ${CMD}"
	${CMD} |& tee ${TEST_LOG_DIR}/${T}.log
	RC=$?
	RCS["$T"]=$RC
	append_summary $T $RC
	INFO "----------------------------------------------"
done 2>&1

[[ -z "${SKIP_PAPI}" ]] && {
	LIST=( ${PAPI_CONT_TEST_LIST[*]} ${CONT_TEST_LIST[*]} )
} || {
	LIST=( ${CONT_TEST_LIST[*]} )
}

TEST_OPTS=(
	--prefix ${CONT_OVIS}
	--runtime docker
	--image ovishpc/ldms-dev
	--src ${WORK_DIR_POOL}
)
for T in ${LIST[*]}; do
	INFO "======== ${T} ========"
	CMD="python3 ${T} ${TEST_OPTS[@]} --data_root ${DATA_ROOT}/${T}"
	INFO "CMD: ${CMD}"
	${CMD} |& tee ${TEST_LOG_DIR}/${T}.log
	RC=$?
	RCS["$T"]=$RC
	append_summary $T $RC
	sleep 10 # allow some clean-up break between tests
	INFO "----------------------------------------------"
	./remove_cluster --all
	sudo rm -rf ${DATA_ROOT}/${T} || true
done 2>&1

# Container tests
CONT_TEST_LIST=( $( ls ${LDMS_CONTAINERS_DIR}/test ) )
for T in "${CONT_TEST_LIST[@]}"; do
	INFO "======== ${T} ========"
	CMD="${LDMS_CONTAINERS_DIR}/test/${T}/test.sh"
	INFO "CMD: ${CMD}"
	${CMD} |& tee ${TEST_LOG_DIR}/cont-${T}.log
	RC=$?
	RCS["cont-$T"]=$RC
	append_summary "cont-$T" $RC
	sleep 10 # allow some clean-up break between tests
	INFO "----------------------------------------------"
done

export RED='\033[01;31m'
export GREEN='\033[01;32m'
export RESET='\033[0m'
INFO "==== Summary ===="
N=${#RCS[*]}
for K in "${!RCS[@]}"; do
	V=${RCS["$K"]}
	if (( $V == 0 )); then
		PASSED=$((PASSED+1))
		V_COLOR="${GREEN}PASSED${RESET}"
	else
		FAILED=$((FAILED+1))
		V_COLOR="${RED}FAILED${RESET}"
	fi
	echo -e "${K}: ${V_COLOR}"
done

echo "${PASSED}" > "$PASSED_FILE"
echo "${FAILED}" > "$FAILED_FILE"

echo "------------------------------------------"
echo -e "Total tests passed: ${PASSED}/${N}"
echo "------------------------------------------"
} |& tee ${LOG}

[[ "${GITHUB_REPORT}0" -eq 0 ]] || ${SCRIPT_DIR}/github-report.sh # make a test end report

PASSED=$(cat ${PASSED_FILE} 2>/dev/null || echo 0)
FAILED=$(cat ${FAILED_FILE} 2>/dev/null || echo 0)

if (( DOCKER_PUSH == 0 )); then
	INFO "Skip docker push ... (DOCKER_PUSH=${DOCKER_PUSH})"
elif (( PASSED > 0 )) && (( FAILED == 0 )); then
	INFO "docker push ..."
	for C in ovishpc/ldms-{dev,samp,agg,ui,grafana,maestro} ; do
		docker push $C
	done
fi

exit ${FAILED}
