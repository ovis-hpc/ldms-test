# SYNOPSIS
#     docker build -t ovis-centos-build -f Dockerfile
FROM centos:7

# Do everything in one RUN to save image size as each RUN commit into a docker
# image layer. Since each RUN results in a layer, even though we perform a RUN
# for a clean up at the end, the stuff still live in the layer.
#
# For comparison:
# - single RUN size: ~ 750 MB,
# - Multiple RUN (RUN for each command): ~ 2.7 GB.
RUN /bin/bash -c '\
set -e ; \
set -x ; \
yum -y install epel-release ; \
yum -y install munge which openssh-server gdb gdb-gdbserver iproute \
               openssl openssl-devel vim munge-devel readline-devel \
	       perl-ExtUtils-MakeMaker pam-devel mariadb-devel libcurl-devel \
	       python-devel papi-devel libpfm-devel \
	       ; \
yum -y group install "Development Tools" ; \
yum clean all ; \
yes "" | ssh-keygen ; \
cat /root/.ssh/id*.pub > /root/.ssh/authorized_keys ; \
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
	 && python get-pip.py \
	 && rm -f get-pip.py ; \
yes | pip install Cython ; \
yes | pip install numpy ; \
cd /etc/ssh ; \
yes "" | ssh-keygen -t rsa -f ssh_host_rsa_key ; \
yes "" | ssh-keygen -t ecdsa -f ssh_host_ecdsa_key ; \
yes "" | ssh-keygen -t ed25519 -f ssh_host_ed25519_key ; \
dd if=/dev/zero of=/etc/munge/munge.key bs=4096 count=1 \
    && chmod 600 /etc/munge/munge.key \
    && chown munge:munge /etc/munge/munge.key \
'

RUN /bin/bash -c '\
cd /root ; \
curl -O https://download.schedmd.com/slurm/slurm-17.02.11.tar.bz2 ; \
rpmbuild -ta slurm-17.02.11.tar.bz2 ; \
cd /root/rpmbuild/RPMS/x86_64 ; \
yum -y install *.rpm ; \
cd /root ; \
yum clean all ; \
'
