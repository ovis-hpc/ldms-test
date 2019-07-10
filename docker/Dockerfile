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
yum -y install munge which openssh-server gdb gdb-gdbserver iproute; \
yes "" | ssh-keygen ; \
cat /root/.ssh/id*.pub > /root/.ssh/authorized_keys ; \
yum -y group install "Development Tools" ; \
yum -y install openssl-devel ; \
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
	 && python get-pip.py \
	 && rm -f get-pip.py ; \
yes | pip install Cython ; \
yes | pip install numpy ; \
yum -y install vim ; \
yum -y install munge-devel readline-devel perl-ExtUtils-MakeMaker \
		   pam-devel mariadb-devel ; \
cd /root ; \
curl -O https://download.schedmd.com/slurm/slurm-19.05.0.tar.bz2 ; \
rpmbuild -ta slurm-19.05.0.tar.bz2 ; \
cd /root/rpmbuild/RPMS/x86_64 ; \
yum -y install *.rpm ; \
cd /root ; \
rm -rf /root/rpmbuild ; \
yum clean all ; \
cd /etc/ssh ; \
yes "" | ssh-keygen -t rsa -f ssh_host_rsa_key ; \
yes "" | ssh-keygen -t ecdsa -f ssh_host_ecdsa_key ; \
yes "" | ssh-keygen -t ed25519 -f ssh_host_ed25519_key ; \
dd if=/dev/zero of=/etc/munge/munge.key bs=4096 count=1 \
    && chmod 600 /etc/munge/munge.key \
    && chown munge:munge /etc/munge/munge.key \
'
