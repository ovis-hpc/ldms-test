#!/bin/bash
#
# github-report.sh
# ----------------
# Publish weekly test results to https://github.com/ldms-test/weekly-report.
#
# This script expects WORK_DIR environment variable. WORK_DIR is set by
# `cygnus-weekly.sh`.

SCRIPT_DIR=$(realpath $(dirname $0))

pushd "${SCRIPT_DIR}"

LOG() {
	echo $( date +"%F %T" ) "$@"
}
export -f LOG

ERROR() {
	LOG "ERROR:" "$@"
}
export -f ERROR

INFO() {
	LOG "INFO:" "$@"
}
export -f INFO

assert() {
	"$@" || {
		ERROR "cmd: '$*' failed"
		exit -1
	}
}
export -f assert

[[ -n "${WORK_DIR}" ]] ||  {
	ERROR "WORK_DIR environment variable is not defined "
	exit -1
}
[[ -d "${WORK_DIR}" ]] || {
	ERROR "WORK_DIR '${WORK_DIR}' is not a directory"
	exit -1
}

[[ -d "weekly-report" ]] || {
	# clone the weekly-report repo
	INFO "weekly-report directory not found"
	INFO "cloning weekly-report from github"
	# the current working directory is SCRIPT_DIR
	set -e
	git clone git@github.com:ldms-test/weekly-report
}

# Check if the tests have skipped due to unchanged SHA
if grep 'Skip the test. SHA does not change:' ${WORK_DIR}/cygnus-weekly.log >/dev/null 2>&1; then
	# If so, don't report
	INFO "The tests have skipped (SHA does not change). Do not report to github"
	exit 0
fi

pushd weekly-report
git fetch
git reset --hard origin/master
cp ${WORK_DIR}/cygnus-weekly.log test-all.log
if [[ -d ${WORK_DIR}/test-log ]]; then
	if [[ -d test-log ]]; then
		rm -rf test-log
	fi
	cp -r ${WORK_DIR}/test-log ./
	cp ${WORK_DIR}/summary.md ./
	git add test-log
	git add summary.md
fi

if TP=$(grep 'Total tests passed' test-all.log); then
	TP=${TP#Total tests passed: }
	NUM=( ${TP/\// } )
	if [[ ${NUM[0]} == ${NUM[1]} ]]; then
		COLOR='brightgreen'
	else
		COLOR='red'
	fi
	MESSAGE="${TP} tests pased"
elif grep 'Installation process succeeded' test-all.log 1>/dev/null; then
	MESSAGE='in progress'
	COLOR='blue'
else
	MESSAGE='build/install failed'
	COLOR='red'
fi


# caller set OVIS_GIT_SHA
OVIS_GIT_SHA_SHORT=${OVIS_GIT_SHA:0:7} # abcdef0
LDMS_TEST_GIT_SHA_SHORT=${LDMS_TEST_GIT_SHA:0:7}

LABEL="weekly test on ovis:${OVIS_GIT_SHA_SHORT} and ldms-test:${LDMS_TEST_GIT_SHA_SHORT}"

cat > status.json <<EOF
{
  "schemaVersion": 1,
  "label": "${LABEL}",
  "message": "${MESSAGE}",
  "color": "${COLOR}"
}
EOF
git commit -a -m  "$(basename ${WORK_DIR})"
git push -f origin
popd # back to SCRIPT_DIR
