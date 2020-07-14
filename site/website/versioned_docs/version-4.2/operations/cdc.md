---
title: Change Data Capture
description: Guide for deploying Change Data Capture connectors and target databases.
id: version-4.2-cdc
original_id: cdc
---

## Database Setup

When using [CDC data
sources](../api/sources-sinks.md#change-data-capture-cdc) in Jet, one
must also take care that the source databases are set up correctly,
meaning they have all the features, required for change data capture,
enabled.

### MySQL

#### Database user

The MySQL CDC source needs a MySQL database user which will be used for
connecting to the database. For how to create one, see the "CREATE USER
Statement" in the MySQL Reference Manual
([5.7](https://dev.mysql.com/doc/refman/5.7/en/create-user.html),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/create-user.html) - note
that `mysql_native_password` is no longer the default authentication
plugin in MySQL 8; you will need to specify it using the
`IDENTIFIED WITH mysql_native_password BY '<password>'` construct).

This database user will also need to have certain permissions enabled.
Use the "GRANT Statement"
([5.7](https://dev.mysql.com/doc/refman/5.7/en/grant.html),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/grant.html)) to set them
if missing. The needed permissions are following:

* SELECT: enables selection of rows from tables
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_select),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_select))
* RELOAD: enables usage of the FLUSH statement to clear or reload
  internal caches, flush tables or acquire locks
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_reload),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_reload))
* SHOW DATABASES: enables usage of the SHOW DATABASES statement
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_show-databases),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_show-databases))
* REPLICATION SLAVE: enables reading the server's binlog (source of the
  CDC events)
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_replication-slave),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_replication-slave))
* REPLICATION CLIENT: enables usage of SHOW MASTER STATUS, SHOW SLAVE
  STATUS and SHOW BINARY LOGS statements
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_replication-client),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_replication-client))

#### Binlog

Binary logging can be enabled by adding a couple of options to the MySQL
config file. See MySQL Reference Manual on how to do that
([5.7](https://dev.mysql.com/doc/refman/5.7/en/option-files.html),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/option-files.html)). For
example:

```text
server-id         = 223344
log_bin           = mysql-bin
binlog_format     = ROW
binlog_row_image  = FULL
expire_logs_days  = 10
```

The semantics of these options are as follows:

* **server-id**: must be unique withing the MySQL cluster
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/replication-options.html#sysvar_server_id),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/replication-options.html#sysvar_server_id))
* **log_bin**: base name of the sequence of binlog files
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#option_mysqld_log-bin),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#option_mysqld_log-bin))
* **binlog_format**: must be set to "ROW" in order for the CDC source to
  work properly
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_binlog_format),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format))
* **binlog_row_image**: must be set to "FULL" in order for the CDC
  source to work properly
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_binlog_row_image),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_image))
* **expire_log_days**: number of days for automatic binlog file removal
  ([5.7](https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_expire_logs_days),
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_expire_logs_days))

While the database server is running, the active values of these
variables can be checked with the help of the "SHOW VARIABLES Statement"
([5.7](https://dev.mysql.com/doc/refman/5.7/en/show-variables.html),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/show-variables.html)).
It's worth pointing out that the names of the options sometimes differ
from the names of the MySQL system variables they set. For example:

```text
SHOW VARIABLES LIKE 'server_id';
```

#### Session timeouts

When an initial consistent snapshot is made for large databases, your
established connection could timeout while the tables are being read.
You can prevent this behavior by configuring **interactive_timeout**
([5.0](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_interactive_timeout),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout))
and **wait_timeout**
([5.0](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_wait_timeout),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout))
in your MySQL configuration file.

On how to work with the MySQL config file consult the Reference Manual
([5.7](https://dev.mysql.com/doc/refman/5.7/en/option-files.html),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/option-files.html)).

### PostgreSQL

#### Database version

The PostgreSQL change data capture connector works by exploiting the
[logical
decoding](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)
feature of the database, first introduced in version 9.4. This version
however is no longer supported. Debezium recommends running change data
capture on version [9.6](https://www.postgresql.org/docs/9.6/index.html)
or later.

#### Output plug-in

PostgreSQL's logical decoding feature is a mechanism which allows the
extraction of the changes which were committed to the transaction log
and the processing of these changes in a user-friendly manner via the
help of an [output
plug-in](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html).

The output plug-ins currently available are:

* [`decoderbufs`](https://github.com/debezium/postgres-decoderbufs),
  maintained by the Debezium community, based on ProtoBuf
* [`wal2json`](https://github.com/eulerto/wal2json), maintained by the
  wal2json community, based on JSON
* `pgoutput`, the standard logical decoding plug-in in PostgreSQL 10 and
  later, maintained by the Postgres community

The `pgoutput` plug-in is always present and requires no explicit
installation, for the other two follow the instructions provided by
their maintainers.

Note: for simplicity Debezium also provides a Docker image based on a
vanilla [PostgreSQL server
image](https://github.com/debezium/docker-images/tree/master/postgres/9.6)
on top of which it compiles and installs all above mentioned plugins.

#### Server config

Running change data capture on a PostgreSQL server requires certain
configuration options to be set accordingly. This can be done either by

* editing the `postgresql.conf` file, or by
* using the [ALTER
  SYSTEM](https://www.postgresql.org/docs/current/sql-altersystem.html)
  command

The important properties to set are:

```properties
# MODULES
shared_preload_libraries = 'decoderbufs,wal2json'

# REPLICATION
wal_level = logical
max_wal_senders = 1
max_replication_slots = 1
```

`shared_preload_libraries` contains a comma separated list of installed
output plug-ins. `wal_levels` is used to tell the server to use logical
decoding with the write-ahead log.

Logical decoding uses [replication
slots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS).
Replication slots retain WAL data even during connector outages. For
this reason it is important to monitor and limit replication slots to
avoid too much disk consumption and other conditions that can happen,
such as catalog bloat if a slot stays unused for too long. This is why
the `max_wal_sender` and `max_replication_slots` parameters are set with
the smallest possible values.

#### Replication permissions

Replication can only be performed by a database user (specifically the
one we set up our CDC connector with) only if the user has appropriate
permissions. The permissions needed are `REPLICATION` and `LOGIN`.

For setting up database users/roles see the [PostgreSQL
documentation](https://www.postgresql.org/docs/9.6/user-manag.html), but
basically the essential command is:

```text
CREATE ROLE name REPLICATION LOGIN;
```

Note: database super-users already have all the permissions needed by
replication too.

#### Client authentication

Replication can only be performed for a configured number of hosts. The
PostgreSQL server needs to allow access from the host the CDC connector
is running on. To specify such  [client authentication](https://www.postgresql.org/docs/9.6/auth-pg-hba-conf.html)
options add following lines to the end of the `pg_hba.conf` file:

```text
local   replication    user                    trust
host    replication    user    127.0.0.1/32    trust
host    replication    user    ::1/128         trust
```

This example tells the server to allow replication for the specified
user locally or on `localhost`, using IPv4 or IPv6.

### Other Databases

Streaming CDC data from other databases supported by Debezium is
possible in Jet by using the [generic Debezium
source](/javadoc/4.2/com/hazelcast/jet/cdc/DebeziumCdcSources.html).
This deployment guide however only covers the databases we have first
class support for. For the other ones pls. refer to the Debezium
documentation:

* [SQL
  Server](https://debezium.io/documentation/reference/1.1/connectors/sqlserver.html#setting-up-sqlserver)
* [MongoDB](https://debezium.io/documentation/reference/1.1/connectors/mongodb.html#setting-up-mongodb)
* [Oracle](https://debezium.io/documentation/reference/1.1/connectors/oracle.html#setting-up-oracle)
* [Db2](https://debezium.io/documentation/reference/1.1/connectors/db2.html#setting-up-Db2)
* [Cassandra](https://debezium.io/documentation/reference/1.1/connectors/cassandra.html#setting-up-cassandra)

## Connector Deployment

### MySQL

#### Using replicas

Enabling the features needed for the MySQL CDC connector (in particular
the binlog) has a performance impact on the database. From our
measurements we estimated it to around 15%, but this is very dependent
on your particular workload. What's certain is that it's not negligible.

Enabling CDC requires pretty much the same settings as replication, so
if you already have replication enabled, then there should be no further
performance penalty. However the CDC source is an extra replication
client, so if you already have replication you might consider connecting
the CDC source to your replica, instead of the main database.

Another reason why using a replica for CDC might be useful, is that for
large databases the snapshot taken by the CDC source when it first
starts can take a significant amount of time and will put heavy load on
the database during that period. This might affect the performance of
other transactions.

#### Using Global Transaction IDs

When using a replica for the CDC source and in general when the MySQL
server cluster has multiple member it's a good idea to give yourself the
option of connecting the CDC source to any one of the members. The
source does track the transaction ids in the binlog and can connect to
the binlog of another MySQL server on restart, but in order for the ids
to match between servers Global Transaction Identifiers (GTIDs) must be
enabled in them.

This can be achieved by setting both of the **gtid_mode**
([5.0](https://dev.mysql.com/doc/refman/5.7/en/replication-options-gtids.html#sysvar_gtid_mode),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_mode))
and **enforce_gtid_consistency**
([5.0](https://dev.mysql.com/doc/refman/5.7/en/replication-options-gtids.html#sysvar_enforce_gtid_consistency),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_enforce_gtid_consistency))
options to "ON".

On how to work with the MySQL config file consult the Reference Manual
([5.7](https://dev.mysql.com/doc/refman/5.7/en/option-files.html),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/option-files.html)).

### PostgreSQL

#### Primary-only replication

As we've mentioned in the [PostgreSQL database setup
section](#postgresql) the connector uses logical decoding replication
slots. All PostgreSQL versions (up to 12) only support logical
replication slots on *primary* servers.

This means that it's not possible to limit the performance impact of
running change data capture on replicas. This weakness is somewhat
offset by the fact that the logical replication process seems quite
limited as far as the amount of resources it can acquire. When measuring
its output it behaves quite like a single threaded process, which can't
saturate neither CPU nor the network.

In our tests we didn't manage to make it output much more than 20,000
records/second, so on a powerful server running the database it
shouldn't affect normal operations too severely.

#### Failure tolerance

PostgreSQL failure tolerance associated with replication slots is
somewhat lacking in certain aspects. The CDC connector can quite nicely
deal with its own restart or connection loss to the primary database,
but only as long as replication slots remain intact. Replication
slots are not themselves synced to physical replicas, so you can’t
continue to use a slot after a master failure results in promotion
of a standby.

There are discussions in the PostgreSQL community around a feature
called *failover slots* which would help mitigate this problem, but as
of version 12 they have not been implemented yet.
