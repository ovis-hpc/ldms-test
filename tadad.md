NAME
=====

tadad - TADA Daemon


SYNOPSIS
========

```
tadad [-c,--config CONFIG_FILE] [-p,--port TADAD_PORT]
      [--db-driver DRIVER] [--db-database DATABASE]
      [--db-path PATH] [--db-host HOST] [--db-port DB_PORT]
      [--db-user USER] [--db-password PASSWORD]
      [--db-purge]
```


DESCRIPTION
===========

TADA Daemon is the daemon in TADA suite that receives test results from the test
programs and stores them in the specified database. The result is also reported
in the STDOUT of the daemon. Please use `tadaq`(1) to query results from the
database.


OPTIONS AND CONFIGURATION
=========================

All options can be specified in the configuration file or in the command-line.
The command-line options override those in the configuration file. The config
file must contain `[tada]` section before specifying options. The options in the
config file is the same as command-line options, but all hyphens `-` in the
option name are replaced with underscores `_`. For example, `--db-driver`
command-line option becomes `db_driver` parameter in the configuration file.

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

<dt><b>--db-purge</b></dt>
<dd>
Purge the existing TADA tables in the database.
</dd>
</dl>


CONFIG EXAMPLES
===============

Config file example with `sqlite` database driver:
```ini
[tadad]
port = 1234
db_driver = sqlite
db_path = /var/lib/tadad.sqlite
```

Config file example with `mysql` database driver:
```ini
[tadad]
db_driver = mysql
db_user = tada
db_password = tada
# use default db_host: localhost
# use default db_database: tada_db
# use default mysql port
```

Config file example with `pgsql` database driver:
```ini
[tadad]
db_driver = pgsql
db_user = tada
db_password = tada
db_host = 10.10.10.10
# use default db_database: tada_db
# use default postgres port
```


NOTE
====

In the case of `mysql` or `postgres`, the database administrator must initially
create the empty database for `tadad`. Please consult `mysql` or `postgres`
manuals accordingly.


SEE ALSO
========
[tadaq](tadaq.md)(1)
