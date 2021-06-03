Scalability Test
================

The scripts in this directory does not run daemons under singlarity nor docker.
The daemons run directly on the hosts (see [config.py](config.py)).

The following is a list of scripts and their synopsis.

- [test.py](test.py): the main test script. It uses configuration specified in
  [config.py](config.py) and other child scripts (e.g. [clsuter.py](cluster.py))
  to control LDMS daemons and perform the test.

- [config.py](config.py): the configuration file. It contains test parameters
  such as list of hosts for sampler/L1/L2/L3, type of transport, number of
  daemons per host, etc.

- [cluster.py](cluster.py): the script to check status, start, or stop ldmsd
  daemons and monitoring daemons on all participating hosts. This script SSH to
  participating hosts and invoke [ldmsd_ctl.py](ldmsd_ctl.py) and
  [ldmsd_mon.py](ldmsd_mon.py). This script is a good tool to stop daemons or
  clear data across participating hosts when things go wrong in
  [test.py](test.py). See `./cluster.py --help` for how to use it.

- [ldmsd_mon.py](ldmsd_mon.py): the script that run locally on a host to check
  status, start, or stop **monitoring** daemons on the host it is running on.
  This script is primarily used by [clsuter.py](cluster.py).

- [ldmsd_ctl.py](ldmsd_ctl.py): the script that run locally on a host to check
  status, start, or stop **ldmsd** on the host it is running on. This script is
  primarily used by [clsuter.py](cluster.py).

- [ldmsdutils.py](ldmsdutils.py): a utility module used by scripts in this
  directory. This module is also useful to interact with LDMS daemons (e.g.
  directory listing, `prdcr_status` fetch) programmatically using python3.


Running the test
================

Running the test is as easy as[./test.py](test.py). The test has multiple sampler
daemons, multiple L1 daemons, multiple L2 daemons, and a single L3 daemon
(optionally with a store). The aggregation load are distributed as even as
possible (see [ldmsdutils.py](ldmsdutils.py), search for `getConfig()`).
The test script will start sampler daemons, aggregator daemons, ldmsd stat
monitoring daemons on the participating hosts over SSH. The script will stop
some daemons, verify the effect, start daemons back up, and verify the recovery
from sampler daemon level, L1, L2 to L3 aggregator. Then, the script finally
verify if there is a file descriptor leak in any of the daemons through stat
monitoring data. Finally, if SOS is enabled, the script check the store to see
if there are missing data. Please see [test.py](test.py) for more details.


Number of daemons and sets
--------------------------
In the [config.py](config.py), `SSH_PORT` is used to specify the SSH port to
connect to the participating hosts. `SAMP_HOSTS`, `L1_HOSTS` and `L2_HOSTS` are
the list of hosts hosting sampler daemons, L1 aggregator daemons, and L2
aggregator daemons respectively. `L3_HOST` on the other hand is not a list but a
`str` variable specifying the host hosting the L3 aggregator. `SAMP_PER_HOST`
specifies the number of ldmsds running on ***each*** host in `SAMP_HOSTS`. The
first daemon listens on port `SAMP_BASE_PORT`, the 2nd daemon listens on port
`SAMP_BASE_PORT+1`, ..., and the i_th daemon listens on port
`SAMP_BASE_PORT+i-1`. The sampler daemons are named by `${HOSTNAME}-${PORT}`.
For each sampler daemon, `SETS_PER_SAMP` specifies the number of sets it
creates. The set names are formatted as `${HOSTNAME}-${PORT}/set_${NUM}`.
`L1_HOSTS`, `L1_PER_HOST`, and `L1_BASE_PORT` work in the similar fasion for L1
aggregators, and `L2_HOSTS`, `L2_PER_HOST`, and `L2_BASE_PORT` work in the
similar fasion for L2 aggregators. At level 3, we have only one aggregator.
Hence, `L3_HOST` specifies the name of the host hosting the L3 aggregator and
`L3_BASE_PORT` is the port of the L3 aggregator.

The number of sets that L3 aggregator will collect is
`len(SAMP_HOSTS)*SAMP_PER_HOST*SETS_PER_SAMP`. The data aggregation from
`len(SAMP_HOSTS)*SAMP_PER_HOST` sampler daemons are distributed as evenly as
possible among L1 aggregators, and the data aggregation from
`len(L1_HOSTS)*L1_PER_HOST` are distributed as evenly as possible among L2
aggregators. The data from all `len(L2_HOSTS)*L2_PER_HOST` L2 aggregators goes
to the single L3 aggregators.

Port ranges
-----------
A host is not required to run a type of daemons exclusively. In other words, a
host may appear in `SAMP_HOSTS`, `L1_HOSTS`, `L2_HOSTS`, and/or `L3_HOST`. In
such case, please be mindful about possible port ranges overlapping (e.g.
`SAMP_BASE_PORT` <= `L1_BASE_PORT` < `SAMP_BASE_PORT+len(SAMP_PER_HOST)`).

Storage
-------
`L3_STORE_ROOT` points to the directory used by L3 aggregator to store SOS
database. If it is set to `None`, `store_sos` is disabled on the L3 aggregator.
