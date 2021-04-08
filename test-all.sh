#!/bin/bash
#
# Usage: [NAME=VAUE ...] ./test-all.sh
#
# Environment variables:
# - OVIS_PREFIX: The path to ovis installation prefix. If not specified,
#                `/opt/ovis` is used.
# - LOG: Path to the log file. ${HOME}/test-all.log is the default.
#
# - SKIP_PAPI: set to non-empty string (e.g. 'y') to skip tests that use PAPI.
# - FAIL_FAST: set to non-empty string (e.g. 'y') to fail immediately if a test
#              failed.

# Use /opt/ovis by default
_default=$( which ldmsd 2>/dev/null )
_default=${_default%/sbin/ldmsd}
_default=${_default:-/opt/ovis}
OVIS_PREFIX=${OVIS_PREFIX:-${_default}}
LOG=${LOG:-${HOME}/test-all.log}

echo "LOG: ${LOG}"
echo "OVIS_PREFIX: ${OVIS_PREFIX}"

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
ldmsd_auth_ovis_test
ldmsd_auth_test
ldmsd_ctrl_test
ldmsd_stream_test
maestro_cfg_test
mt-slurm-test
ovis_ev_test
set_array_test
setgroup_test
slurm_stream_test
spank_notifier_test
)

[[ -d "${OVIS_PREFIX}" ]] || {
	echo "ERROR: ${OVIS_PREFIX} is not found or is not a directory."
	echo "       Please set OVIS_PREFIX environment variable to"
	echo "       the ovis installation prefix."
	exit -1
}

[[ -e "${OVIS_PREFIX}/sbin/ldmsd" ]] || {
	echo "ERROR: ldmsd not found in ${OVIS_PREFIX}/sbin."
	echo "       Please set OVIS_PREFIX environment variable to"
	echo "       the ovis installation prefix."
	exit -1
}

[[ -z "${FAIL_FAST}" ]] || set -e

for T in ${LIST[*]}; do
	echo "======== ${T} ========"
	CMD="./${T} --prefix ${OVIS_PREFIX} $@"
	echo ${CMD}
	${CMD}
	sleep 10 # allow some clean-up break between tests
	echo "----------------------------------------------"
done 2>&1 | tee ${LOG}
