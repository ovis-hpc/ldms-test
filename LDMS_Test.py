#
#
import docker
import subprocess
import sys
import time
import os

class Network(object):
    def __init__(self, name, driver='bridge', scope='local', attachable=True):
        self.client = docker.from_env()
        self.network_name = name
        try:
            self.network = self.client.networks.get(name)
        except:
            self.network = None
        if self.network is None:
            self.network = self.client.networks.create(name=name, driver=driver,
                                        scope=scope, attachable=attachable)

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


class LDMSD_SVC(object):
    """Get or create service in docker swarm and prepare for `ldmsd`

    If the service already existed, all of the other object creation parameters
    are ignored and `LDMSD_SVC` will just wrap the existing service. Otherwise,
    they are used to create the docker service for `ldmsd`.

    Attributes
    ----------
    svc : docker.Service
        The corresponding docker service object.
    cont : docker.Container
        The corresponding docker container object.
    name : str
        The service name (from init()).
    xprt : str
        The transport type (from init).
    port : int
        The ldmsd port (from init).
    log_file : str
        The path to log file in the CONTAINER (from init).
    """
    def __init__(self, name, force_create=False, xprt="sock", port=10000,
                 config=None,
                 config_file = "/etc/ldmsd.conf",
                 auth = "none",
                 auth_opts = {},
                 log_level="INFO", log_file = "/var/log/ldmsd.log",
                 networks = [ "default_overlay" ],
                 ovis_prefix = "/opt/ovis",
                 extra_mounts = [],
                 extra_env = []):
        """
        Parameters
        ----------
        name : str
            The name of the service. This will also be used as a hostname
            for the docker container.
        force_create : bool
            If this flag is set to `True`, enforce the service creation. The
            initialization will fail if the service already existed. By default,
            this flag is `False`, i.e. `LDMSD_SVC` will wrap the existing
            service, or create a new service if it does not exist.
        xprt : str
            The LDMS transport type (default: "sock").
        port : int
            The port of the ldmsd (default: 10000).
        config : str
            The CONTENT of the configuration file. If not `None`, this will
            be written to a configuration file for the `ldmsd` to use. If the
            value is `None`, the config file is left untouched.
            (default: None).
        config_file : str
            The path (in CONTAINER) to config file. This could be useful if the
            config file has already been prepared, or testing out various
            configuration path. Please mind that if the `config` option is not
            `None`, the file specified by `config_file` will be overwritten.
            (default: "/etc/ldmsd.conf").
        auth : str
            The name of the authentication method (default: "none").
        auth_opts : dict( str:str )
            A dictionary of auth options, in which the key being authentication
            option name, and value being the corresponding value (e.g.
            `{ 'uid': '0' }` for `naive` authentication method).
            (default: None).
        log_level : str
            The `ldmsd` log level (default: "INFO").
        log_file : str
            The path in the CONTAINER to the `ldmsd` log file.
            (default: "/var/log/ldmsd.log")
        networks : list( str )
            A list of strings containing docker network names to attach this
            service to. The networks need to exist before creating LDMSD_SVC.
            (default: [ "default_overlay" ]).
        ovis_prefix : str
            The ovis installation prefix path in the HOST. This directory
            will be mounted as `/opt/ovis` in the container and proper
            environment variables will be setup.
        extra_mounts : list (str)
            A list of strings for extra mount points. The format of each entry
            is "SRC_PATH_IN_HOST:DEST_PATH_IN_CONTAINER:MODE", where "MODE" is
            either "ro" or "rw". An example of an entry is
            "/home/bob/data:/data:rw".
        extra_env : list (str)
            A list of strings for extra environment setup. The format of each
            entry is "NAME=VALUE".
        """
        self.client = docker.from_env()
        self.name = name
        self.xprt = xprt
        self.port = port
        self.config = config
        self.config_file = config_file
        self.auth = auth
        self.auth_opts = auth_opts.copy()
        self.log_level = log_level
        self.log_file = log_file
        self.networks = list(networks)
        self.ovis_prefix = ovis_prefix
        self.extra_mounts = list(extra_mounts)
        self.extra_env = list(extra_env)
        self.cont = None
        self.svc = None
        if not force_create:
            try:
                self.svc = self.client.services.get(name)
            except docker.errors.NotFound:
                pass
        if not self.svc:
            self._create_svc()

    def _create_svc(self):
        mounts = [ "{}:/opt/ovis:ro".format(self.ovis_prefix) ]
        mounts.extend(self.extra_mounts)
        env = [
            "PATH=/sbin:/bin:/usr/sbin:/usr/bin:/opt/ovis/bin:/opt/ovis/sbin",
            "LD_LIBRARY_PATH=/opt/ovis/lib:/opt/ovis/lib64",
            "ZAP_LIBPATH=/opt/ovis/lib/ovis-lib:/opt/ovis/lib64/ovis-lib",
            "LDMSD_PLUGIN_LIBPATH=/opt/ovis/lib/ovis-ldms:/opt/ovis/lib64/ovis-ldms",
        ]
        env.extend(self.extra_env)

        self.svc = self.client.services.create(
                image = "ovis-centos-build",
                command = "bash",
                open_stdin = True,
                env = env,
                hostname = self.name,
                name = self.name,
                # REMARK: hostname == (service)name for easy name resolve
                #         in docker overlay network
                networks = self.networks,
                user = "root",
                tty = True, # so that bash keep running ...
                mounts = mounts
            )
        return self.svc


    def is_svc_running(self):
        """Check if the service is running"""
        try:
            tasks = self.svc.tasks()
            return tasks[0]['Status']['State'] == 'running'
        except:
            return False

    def wait_svc_running(self, timeout=10):
        """Wait until the service is running

        Parameters
        ----------
        timeout : int
            The number of seconds for wait timeout.

        Returns
        -------
        True  : if the service is running before timeout.
        False : if the timeout occurred before the service became running.
        """
        t0 = time.time()
        while not self.is_svc_running():
            t1 = time.time()
            if t1-t0 > timeout:
                return False
            time.sleep(1)
        return True

    def kill_svc(self):
        """Kill the docker service (and container)"""
        self.svc.remove()

    def get_cont(self):
        if not self.cont:
            if not self.wait_svc_running():
                raise RuntimeError("Container main task not running")
            task = self.svc.tasks()[0]
            nid = task['NodeID']
            node = self.client.nodes.get(nid)
            addr = node.attrs['Description']['Hostname'] + ":2375"
            # client to remote dockerd
            ctl = docker.client.from_env(environment={'DOCKER_HOST': addr})
            task = self.svc.tasks()[0]
            cont_id = task['Status']['ContainerStatus']['ContainerID']
            self.cont = ctl.containers.get(cont_id)
        return self.cont

    def exec_run(self, cmd):
        """A wrapper of Container.exec_run()"""
        return self.get_cont().exec_run(cmd)

    def start_ldmsd(self):
        """A routine to start `ldmsd` in the container"""
        if self.check_ldmsd():
            return # already running
        if self.config:
            self.write_file(self.config_file, self.config)
        config = "" if not self.config else "-c {}".format(self.config_file)
        cmd = "ldmsd {config} -x {xprt}:{port}" \
               " -v {level} -l {log}".format(
                    config = config,
                    xprt = self.xprt,
                    port = self.port,
                    level = self.log_level,
                    log = self.log_file,
               )
        self.exec_run(cmd)

    def check_ldmsd(self):
        rc, out = self.exec_run("pgrep -c ldmsd")
        return rc == 0

    def kill_ldmsd(self):
        self.exec_run("pkill ldmsd")

    def write_file(self, path, content):
        """Write `content` to `path` in the container"""
        cmd = "/bin/bash -c 'cat -  >{}'".format(path)
        erun = self.get_cont().exec_run(cmd, stdin=True, socket=True)
        sock = erun.output
        sock.send(content)
        sock.close()

    def read_file(self, path):
        """Read file specified by `path` from the container"""
        cmd = "cat {}".format(path)
        erun = self.get_cont().exec_run(cmd)
        return erun.output
