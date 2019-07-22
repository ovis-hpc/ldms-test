NAME
=====

tadaq - TADA database query tool


SYNOPSIS
========

```
tadaq [-c,--config CONFIG_FILE]
      [--db-driver DRIVER] [--db-database DATABASE]
      [--db-path PATH] [--db-host HOST] [--db-port DB_PORT]
      [--db-user USER] [--db-password PASSWORD]
      [--test-suite SUITE] [--test-type TYPE] [--test-name NAME]
      [--test-user USER] [--commit-id COMMIT_ID]
      [--latest] [--purge-old-tests]
```


DESCRIPTION
===========

`tadaq` is a command-line tool to query test results from the TADA database.
The `--db-*` options are for database connection, and `--test-*` options are for
result filtering.

A test may have multiple runs (same suite, type, name, user,
commit-id, but different start time). By default, all runs are reported. The
`--latest` option can be given to limit the results to only the latest run of
each result.

The old runs in each test can also be purged from the database with
`--purge-old-tests` option.


OPTIONS AND CONFIGURATION
=========================

All options can be specified in the configuration file or in the command-line.
The command-line options override those in the configuration file. The config
file must contain `[tada]` section before specifying options. The options in
the config file is the same as command-line options, but all hyphens `-` are
replaced with underscore `_`. For example, `--db-driver` command-line option
becomes `db_driver` parameter in the configuration file.

The following is the list of all options and their descriptions:

<dl>
<dt><b>-c,--config</b> <em>CONFIG_FILE</em></dt>
<dd>
Path to the configuration file. If not specified, <b>$TADAD_CONF</b> environment
variable is looked up first, then <b>$PWD/tadad.conf</b>, and finally
<b>/etc/tadad.conf</b>. Only the first configuration file found in the search
order get processed.
</dd>

<dt><b>-p,--port</b> <em>TADAD_PORT</em></dt>
<dd>
The UDP socket port for TADA Daemon to listen to the test result messages from
the test scripts or test programs.
</dd>

<dt><b>--db-driver</b> <em>DRIVER</em></dt>
<dd>
Database driver. Could be <b>sqlite</b>, <b>mysql</b> or <b>pgsql</b>.
</dd>

<dt><b>--db-path</b> <em>PATH</em></dt>
<dd>
For <b>sqlite</b>, this is the path to the database file. This is ignored if the
database driver is not <b>sqlite</b>. The default is <b>tada_db.sqlite</b>.
</dd>

<dt><b>--db-database</b> <em>DBNAME</em></dt>
<dd>
For <b>mysql</b> and <b>pgsql</b> driver, this is the name of the database to
connect to. This option is ignored if the driver is <b>sqlite</b>. The default
is <b>tada_db</b>.
</dd>

<dt><b>--db-host</b> <em>ADDR</em></dt>
<dd>
For <b>mysql</b> and <b>pgsql</b> driver, this is the address or the hostname to
the host hosting the database daemon. This option is ignored if the driver is
<b>sqlite</b>. The default is <b>localhost</b>.
</dd>

<dt><b>--db-port</b> <em>DB_PORT</em></dt>
<dd>
For <b>mysql</b> and <b>pgsql</b> driver, this is the port of the database
daemon. This option is ignored if the driver is <b>sqlite</b>. The default is
to use appropriate default port according to the driver.
</dd>

<dt><b>--db-user</b> <em>USER</em></dt>
<dd>
For <b>mysql</b> and <b>pgsql</b> driver, this is the username use to connect to
the database. This option is ignored if the driver is <b>sqlite</b>. The default
is to use appropriate default port according to the driver.
</dd>

<dt><b>--db-password</b> <em>PASSWORD</em></dt>
<dd>
For <b>mysql</b> and <b>pgsql</b> driver, this is the password use to connect to
the database. This option is ignored if the driver is <b>sqlite</b>. The default
is to use appropriate default port according to the driver.
</dd>

<dt><b>--test-suite</b> <em>TEST_SUITE</em></dt>
<dd>
If specified, filter the test results to match the given test suite.
</dd>

<dt><b>--test-type</b> <em>TEST_TYPE</em></dt>
<dd>
If specified, filter the test results to match the given test type.
</dd>

<dt><b>--test-name</b> <em>TEST_NAME</em></dt>
<dd>
If specified, filter the test results to match the given test name.
</dd>

<dt><b>--test-user</b> <em>TEST_USER</em></dt>
<dd>
If specified, filter the test results to match the given user.
</dd>

<dt><b>--commit-id</b> <em>COMMIT_ID</em></dt>
<dd>
If specified, filter the test results to match the given commit ID.
</dd>

<dt><b>--latest</b></dt>
<dd>
Only show the latest results of the same kind of test run.
</dd>

<dt><b>--purge-old-tests</b></dt>
<dd>
Purge old runs of each test in the database.
</dd>
</dl>


EXAMPLES
========

```bash
# use default config file, get all test results by `bob` in LDMSD suite where
# the target commit-id being `abcdef`
$ tadaq --test-suite LDMSD --test-user bob --commit-id abcdef

# use custom config (e.g. different database), list all tests (all runs)
$ tadaq -c /home/bob/tada.cfg

# use CLI db options
$ tadaq --db-driver mysql --db-user bob --db-database bobdb

# List results from `MySuite` test suite, but only the latest runs
$ tadaq --test-suite MySuite --latest

# Purge old test runs (keep only the latest runs)
$ tadaq --purge-old-tests
```

SEE ALSO
========
[tadad](tadad.md)(1)
