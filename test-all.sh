#!/bin/bash
#
# Usage: ./test-all.sh         # or
#        OVIS_PREFIX=/my/ovis LOG=/my/log ./test-all.sh
#
# If OVIS_PREFIX environment variable is not specified, `/opt/ovis` is used.
#
# The output is printed to STDOUT and is also logged to a log file pointed to by
# LOG environment varaible. The default LOG is ${HOME}/test-all.log.

# Use /opt/ovis by default
OVIS_PREFIX=${OVIS_PREFIX:-/opt/ovis}
LOG=${LOG:-${HOME}/test-all.log}

LIST=(
agg_slurm_test
agg_test
ldmsd_auth_ovis_test
ldmsd_auth_test
ldmsd_ctrl_test
ldmsd_stream_test
ovis_ev_test
papi_sampler_test
papi_store_test
set_array_test
setgroup_test
slurm_stream_test
spank_notifier_test
store_app_test
syspapi_test
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

for T in ${LIST[*]}; do
	echo "======== ${T} ========"
	CMD="./${T} --prefix ${OVIS_PREFIX}"
	echo ${CMD}
	${CMD}
	echo "----------------------------------------------"
done 2>&1 | tee ${LOG}
