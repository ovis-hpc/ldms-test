#!/bin/bash
#
# Usage: [NAME=VAUE ...] ./test-all.sh [OPTIONS-PASSED-TO-TESTS]
#
# Environment variables:
# - LOG: Path to the log file. ${SCRIPT_DIR}/test-all.log is the default.
#
# - SKIP_PAPI: set to non-empty string (e.g. 'y') to skip tests that use PAPI.
# - FAIL_FAST: set to non-empty string (e.g. 'y') to fail immediately if a test
#              failed.
#
# Examples:
# ```sh
# #### pass `--prefix /opt/ovis` to all test scripts, and stop the batch test
# #### immediately if a test failed
# $ LOG=test.log FAIL_FAST=y ./test-all.sh --prefix /opt/ovis
# ```

SCRIPT_DIR=$( dirname $0 )
LOG=${LOG:-${SCRIPT_DIR}/test-all.log}

SUMMARY_DIR=${SUMMARY_DIR:-${SCRIPT_DIR}/summary}

echo "LOG: ${LOG}"

[[ -d ${SUMMARY_DIR} ]] && rm -rf ${SUMMARY_DIR}
mkdir -p ${SUMMARY_DIR} || exit -1

source ${SCRIPT_DIR}/test-list.sh
# This defines DIRECT_TEST_LIST, CONT_TEST_LIST, PAPI_CONT_TEST_LIST

[[ -z "${SKIP_PAPI}" ]] && {
	LIST=( ${PAPI_CONT_TEST_LIST[*]} ${CONT_TEST_LIST[*]} )
} || {
	LIST=( ${CONT_TEST_LIST[*]} )
}

[[ -z "${FAIL_FAST}" ]] || set -e

cat_summary() {
	local NAME=$1
	local RC=$2
	if [[ "${RC}" == "-" ]]; then
		S="TESTING"
	elif (( RC == 0 )); then
		S="${GREEN}PASSED${RESET}"
	else
		S="${RED}FAILED${RESET}"
	fi
	echo -e "${NAME}: ${S}" > ${SUMMARY_DIR}/${NAME}
}

{ # printing in this subshell will be logged
declare -A RCS
for T in ${LIST[*]}; do
	echo "======== ${T} ========"
	CMD="./${T} $@"
	echo ${CMD}
	cat_summary ${T} -
	${CMD}
	RC=$?
	RCS["$T"]=${RC}
	echo "EXIT_CODE: ${RC}"
	cat_summary ${T} ${RC}
	sleep 10 # allow some clean-up break between tests
	echo "----------------------------------------------"
done

for T in ${INSIDE_CONT_TEST_LIST[*]}; do
	echo "======== ${T} ========"
	CMD="./run_inside_cont_test.py --suite ${T} $@"
	echo ${CMD}
	cat_summary ${T} -
	${CMD}
	RC=$?
	RCS["$T"]=${RC}
	echo "EXIT_CODE: ${RC}"
	cat_summary ${T} ${RC}
	sleep 10 # allow some clean-up break between tests
	echo "----------------------------------------------"
done

export RED='\033[01;31m'
export GREEN='\033[01;32m'
export RESET='\033[0m'
echo "==== Summary ===="
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
} 2>&1 | tee ${LOG}
