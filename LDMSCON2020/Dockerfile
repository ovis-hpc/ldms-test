# SYNOPSIS
#     docker build -t ldmscon2020 - < Dockerfile
#
# PREREQUISITE
#     `ovis-centos-build` image (in ldms-test/docker) must be built first.
FROM ovis-centos-build

RUN /bin/bash -c '\
set -e ; \
set -x ; \
yum -y install emacs wget ; \
'

# SOS-4 will be installed manually from the headnode
#RUN /bin/bash -c '\
#git clone -b SOS-4 https://github.com/ovis-hpc/sos /root/sos || exit -1 ; \
#cd /root/sos || exit -1 ; \
#./autogen.sh || exit -1 ; \
#mkdir build || exit -1 ; \
#cd build || exit -1 ; \
#../configure \
#	--prefix=/opt/ovis \
#	PYTHON="/usr/bin/python2" \
#	CFLAGS="-ggdb3 -O0 -Wall -Werror" \
#	|| exit -1 ; \
#make || exit -1; \
#make install || exit -1 ; \
#'

# OVIS-4 will be installed manually from the headnode
#RUN /bin/bash -c '\
#git clone -b OVIS-4 https://github.com/ovis-hpc/ovis /root/ovis || exit -1 ; \
#cd /root/ovis || exit -1 ; \
#./autogen.sh || exit -1 ; \
#mkdir build || exit -1 ; \
#cd build || exit -1 ; \
#../configure \
#	--prefix=/opt/ovis \
#	--with-sos=/opt/ovis \
#	--enable-doc \
#	--enable-doc-man \
#	--enable-doc-html \
#	--enable-etc \
#	--enable-python \
#	--enable-swig \
#	--enable-test_sampler \
#	--enable-ldms-test \
#	--enable-sos \
#	--enable-munge \
#	--enable-app-sampler \
#	--enable-store-app \
#	--enable-slurm-sampler \
#	--enable-spank-plugin \
#	--enable-zaptest \
#	--enable-papi-sampler \
#	--enable-syspapi-sampler \
#	--enable-tutorial-sampler \
#	--enable-tutorial-store \
#        PYTHON="/usr/bin/python2" \
#	CFLAGS="-ggdb3 -O0 -Wall -Werror" \
#	|| exit -1 ; \
#make || exit -1; \
#make install || exit -1 ; \
#'
