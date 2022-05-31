#!/bin/bash

SDIR=$(realpath $(dirname $0))
TOPDIR=$(realpath ${SDIR}/..)

for E in "$@"; do
	export "${E}"
done

IMAGE=${IMAGE:-${TOPDIR}/ovis-centos-build}
DOCKER_IMAGE=${DOCKER_IMAGE:-docker://ovishpc/ovis-centos-build}

[[ -t 1 ]] && {
	# under a tty
	BLD="\e[01m"
	RED="\e[31m"
	GRN="\e[32m"
	YLW="\e[33m"
	BLU="\e[34m"
	MGT="\e[35m"
	TEA="\e[36m"
	WHT="\e[37m"
	RST="\e[00m"
}

INFO() {
	echo -e "$(date +'%F %T')" "${BLU}INFO:${RST}" "$@"
}

WARN() {
	echo -e "$(date +'%F %T')" "${YLW}WARN:${RST}" "$@"
}

ERROR() {
	echo -e "$(date +'%F %T')" "${RED}ERROR:${RST}" "$@"
}

set -e

# === build the image === #
[[ -e ${IMAGE} ]] && {
	INFO "Image ${IMAGE} existed"
} || {
	INFO "Building ${IMAGE} ..."
	singularity build --sandbox ${IMAGE} ${DOCKER_IMAGE}
}

# === amend the image === #
INFO "Amending the image ..."
umask 0022
pushd ${IMAGE} >/dev/null

# These are mount points used by ldms-test
INFO "  making directories used by ldms-test scripts"
mkdir -p .ldms-test data db munge local store tada-src
touch etc/ldmsd.conf
touch etc/profile.d/ovis.sh

INFO "  copying startscript"
cp ${SDIR}/startscript .singularity.d/startscript
INFO "  env link"
ln -fs ../../.ldms-test/env.sh .singularity.d/env/env.sh

INFO "  making etc/ld.so.conf.d/ovis.conf"
cat > etc/ld.so.conf.d/ovis.conf <<EOF
/opt/ovis/lib
/opt/ovis/lib64
EOF

INFO " correcting etc/shadow, etc/gshadow permission (for sshd in containers)"
chmod 400 etc/shadow etc/gshadow

INFO "DONE"
