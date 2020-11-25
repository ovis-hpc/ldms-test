Running Tests
=============

The test scripts can run with or without `tadad`. If `tadad` is not running, the
test script simply log the results through `stdout`. If `tadad` is running, the
results are also sent to `tadad` to store in the database, which can later be
queried using `tadaq`.

The following is a list of subtopics to run the tests:
- Prerequisite: docker setup: see [link](README.md#docker-setup)
- [`tadad`](#tadad)
- [`tadaq`](#tadaq)
- [`test-all.sh`](#test-all.sh): a script to run all tests
- [Individual Test](#individual-test): running individual test
- [Debug](#debug)


`tadad`
-------

This is a concise guide to run [`tadad`](tadad.md). For more details, please see
the manual [here](tadad.md).

1. Config: edit `/etc/tadad.conf`
   ```conf
   [tada]
   db_driver = sqlite
   db_path = /DATA15/tada/tada_db.sqlite # path to database
   # tadad will create it if not existed
   log = /DATA15/tada/tadad.log # path to tadad log
   ```

2. Run: simply call `./tadad` to run it.
   ```sh
   $ cd ldms-test
   $ ./tadad
   ```

3. Check: use `pgrep -af` to check whether `tadad` is running.
   ```sh
   $ pgrep -af tadad
   479399 python3 ./tadad
   ```

4. terminate: use `pgrep -af` to get the PID and use `kill` to terminate
   (SIGTERM) the daemon.
   ```sh
   $ pgrep -af tadad
   479399 python3 ./tadad
   $ kill 479399
   ```


`tadaq`
-------

This is a concise guide of [`tadaq`](#tadaq.md). For more details, please see
the manual [here](#tadaq.md).

1. query test results from your latest commit:
   ```sh
   $ ./tadaq | less -R
   ```

2. query for a specific test (latest commit-id by default):
   ```sh
   $ ./tadaq --test-name=agg_slurm_test | less -R
   ```

3. list only failed assertions
   ```sh
   $ ./tadaq --only-failed | less -R
   ```

4. query bob's results (latest commit-id by default):
   ```sh
   $ ./tadaq --test-user=bob | less -R
   ```

5. query bob's results with all commit IDs:
   ```sh
   $ ./tadaq --test-user=bob --commit-id=* | less -R
   ```

6. query results from all users and all commits:
   ```sh
   $ ./tadaq --test-user=* --commit-id=* | less -R
   ```


`test-all.sh`
-------------

This is a convenient script to sequentially run all test scripts in this
repository.

**REMARK** The user executing this script must be able operate `docker`, i.e.
he/she must be in `docker` group.

```sh
# Use default `/opt/ovis` prefix and log output to `${HOME}/test-all.log`
$ ./test-all.sh

# Specify OVIS_PREFIX and LOG
$ LOG=/my/test-all.log OVIS_PREFIX=/my/ovis ./test-all.sh
```

If `tadad` is running, the results can be queried using `tadaq`. Othwerwise, the
results (as well as other informational log messages) are in the log file.


Individual Test
---------------

To run a specific test, simply invoke the test script with appropriate prefix.
For example:

```sh
$ ./agg_test --prefix=/my/ovis
```

The results will be printed to STDOUT. If `tadad` is running, the assertion
results will be forwarded to `tadad` and can later be queried using `tadaq`.


Cleaning Up
-----------

Test scripts that failed or experienced an error may leave docker clusters
running. Use the following commands to list the running clusters and remove the
unwanted clusters.

```sh
$ ./list_cluster
$ ./remove_cluser <CLUSTER_NAME>
```

In addition, the data root directory used for sharing files between the host and
the docker containers are also not automatically deleted so that the tester can
investigate them afterward. The data root for the test script is specified by
`--data-root` option. If it is not specified, the default is
`${HOME}/db/<CLUSTER_NAME>`.


Debug
-----

For debugging, it is advisable to provide `--src` and `--debug` options as
the following example:

```sh
$ python3 -i ./agg_test --prefix=/my/ovis --src=/home/bob/project/ovis --debug
```

- Running the test under interactive python shell allows python-level debugging
  of the test script (e.g. investigate variables).

- `--debug` stops the test right away if a test assertion failed. The docker
  cluster won't be cleaned up.

- `--src` will mount the given source directory on host machine to the same path
  on all containers. This allows gdb debugging with source code inside the
  containers.

To bring up a shell inside a container:

```sh
# generic format
$ docker exec -it <CONTAINER_NAME> bash

# example
$ docker exec -it bob-agg_test-4e3d621-headnode bash
```

The container name of all tests follows the following pattern:

```
{USER}-{TEST_NAME}-{SHORT_COMMIT_ID}-{VIRTUAL_NODE_NAME}

where

- {USER}: the username of the user who run the test
- {TEST_NAME}: the test name (same as the file name of the test script)
- {SHORT_COMMIT_ID}: a short commit-id of OVIS installed in OVIS_PREFIX
- {VIRTUAL_NODE_NAME}: a hostname of the node (container) of the virtual cluster
```

Please also check the list of docker containers to see that it really is up:
```sh
$ docker ps
```

From the shell inside a container, a user can `ssh` to any other node inside the
virtual cluster. `/etc/hosts` in the containers contains a list of all nodes.
`gdb` is also available inside the containers.
