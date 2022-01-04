#!/bin/bash
#
# Usage: [NAME=VAUE ...] ./test-all.sh [OPTIONS-PASSED-TO-TESTS]
#
# Environment variables:
# - LOG: Path to the log file. ${HOME}/test-all.log is the default.
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

LOG=${LOG:-${HOME}/test-all.log}

echo "LOG: ${LOG}"

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
ldms_record_test
)

[[ -z "${FAIL_FAST}" ]] || set -e

{ # printing in this subshell will be logged
declare -A RCS
for T in ${LIST[*]}; do
	echo "======== ${T} ========"
	CMD="./${T} $@"
	echo ${CMD}
	${CMD}
	RC=$?
	RCS["$T"]=${RC}
	echo "EXIT_CODE: ${RC}"
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
