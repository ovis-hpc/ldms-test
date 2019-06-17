#
#
import docker
import subprocess
import sys
import time
import os

class Network(object):
    def __init__(self, name, driver='bridge', scope='local'):
        self.client = docker.from_env()
        self.network_name = name
        try:
            self.network = self.client.networks.get(name)
        except:
            self.network = None
        if self.network is None:
            self.network = self.client.networks.create(name=name, driver=driver, scope=scope)

    @property
    def name(self):
        return self.network_name

    @property
    def short_id(self):
        return self.short_id

    def rm(self):
        self.network.remove()
        self.network = None

class LDMSD(object):
    def __init__(self, hostname, network,
                 image="ovis-centos-build",
                 prefix='/opt/ovis', db_root='/DATA',
                 filer='10.10.0.16',
                 listen_port=10000,
                 listen_xprt='sock',
                 config_file=None,
                 log_file=None,
                 log_level='ERROR',
                 auth_name='munge',
                 sample_interval=1000000,
                 component_id=10000,
                 environment=None):
        self.client = docker.from_env()
        self.hostname = hostname
        self.image = image
        self.network_name = network.name
        self.prefix = prefix
        self.db_root = db_root
        if not config_file:
            self.config_file = '/opt/ovis/etc/ldmsd.conf'
        else:
            self.config_file = config_file
        self.listen_port = listen_port
        self.listen_xprt = listen_xprt
        self.cont_name=self.hostname + '-' + str(self.listen_port)
        if log_file:
            self.log_file = log_file
        else:
            self.log_file = '/var/log/' + self.cont_name + '.log'
        self.log_level = log_level
        self.auth_name = auth_name
        self.filer = filer
        self.container = None
        self.component_id = component_id
        self.sample_interval=sample_interval
        if environment:
            self.environment = environment
        else:
            env = []
            p = "LD_LIBRARY_PATH=/opt/ovis/lib64"
            e = os.getenv("LD_LIBRARY_PATH")
            if e:
                p += ':' + e
            env.append(p)

            env.append("PATH=/opt/ovis/bin:/opt/ovis/sbin:" + os.getenv("PATH"))
            env.append("LDMSD_PLUGIN_LIBPATH=/opt/ovis/lib64/ovis-ldms")
            env.append("ZAP_LIBPATH=/opt/ovis/lib64/ovis-lib")
            env.append("COMPONENT_ID={0}".format(self.component_id))
            env.append("LISTEN_PORT={0}".format(self.listen_port))
            env.append("SAMPLE_INTERVAL={0}".format(self.sample_interval))
            self.environment = env

        self.container = self.client.containers.run(
            self.image,
            detach=True,
            network=self.network_name,
            tty=True,
            stdin_open=True,
            user='root',
            name=self.cont_name,
            hostname=self.hostname,
            remove=True,
            volumes = {
                self.prefix : { 'bind' : '/opt/ovis', 'mode' : 'ro' },
                self.db_root : { 'bind' : '/db', 'mode' : 'rw' }
            },
            security_opt = [ 'seccomp=unconfined' ]
        )

    @property
    def name(self):
        return self.container.name

    @property
    def ip4_address(self):
        self.container.reload()
        return self.container.attrs['NetworkSettings']['Networks'][self.network_name]['IPAddress']

    def df(self):
        code, output = self.container.exec_run("/usr/bin/df")
        print output

    def ip_addr(self):
        code, output = self.container.exec_run("/usr/bin/ip addr")
        print output

    def exec_run(self, cmd):
        rc, output = self.container.exec_run(cmd)
        print output

    def kill(self):
        self.container.kill()
        self.container = None

    def test_running(self, cmd):
        rc, pid = self.container.exec_run('pgrep '+cmd)
        if rc:
            return False
        else:
            return True

    def ldms_ls(self, host='localhost'):
        cmd = 'ldms_ls -h {host} -x {xprt} -p {port} -a {auth}'\
        .format(
            xprt=self.listen_xprt,
            port=self.listen_port,
            config=self.config_file,
            host=host,
            log=self.log_file,
            log_level=self.log_level,
            auth=self.auth_name
        )
        rc, output = self.container.exec_run(cmd, environment=self.environment)
        if rc != 0:
            print("Error {0} running \n{1}".format(rc, cmd))
        print(output)

    def kill_ldmsd(self):
        rc, output = self.container.exec_run('pkill ldmsd')

    def start_ldmsd(self):
        if self.auth_name == 'munge':
            if not self.test_running('munged'):
                self.container.exec_run('/usr/sbin/munged')
            if not self.test_running('munged'):
                raise ValueError("Could not start munged but auth=munge")

        if not self.log_file:
            self.log_file = '/var/log/' + self.container.name + '.log'

        cmd = 'ldmsd -x {xprt}:{port} -H {host} -l {log} -v {log_level} -a {auth} -m 1m '
        if self.config_file:
            cmd += '-c {config}'

        cmd = cmd.format(
            xprt=self.listen_xprt,
            port=self.listen_port,
            config=self.config_file,
            host=self.hostname,
            log=self.log_file,
            log_level=self.log_level,
            auth=self.auth_name
        )

        rc, output = self.container.exec_run(cmd, environment=self.environment)
        if rc != 0:
            print("Error {0} running \n{1}".format(rc, cmd))
            print("Output:")
            print(output)
        else:
            if not self.test_running('ldmsd'):
                print("The ldmsd daemon failed to start. Check the log file {0}".\
                      format(self.log_file))
                rc, out = self.exec_run('cat ' + self.log_file)
                print(out)
            else:
                print("Daemon started")
        return rc

    def config_ldmsd(self):
        if not self.test_running('ldmsd'):
            print("There is no running ldmsd to configure")
            return

        cmd = 'echo status | ldmsd_controller --host {host}' \
              '--xprt {xprt} ' \
              '--port {port} ' \
              '--auth {auth} ' \
                  .format(
                      xprt=self.listen_xprt,
                      port=self.listen_port,
                      host=self.hostname)
        rc, output = self.container.exec_run(cmd, environment=self.environment)
        if rc != 0:
            print("Error {0} running \n{1}".format(rc, cmd))
            print("Output:")

        print(output)
