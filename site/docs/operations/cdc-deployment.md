---
title: CDC Deployment
description: Guide for deploying CDC connectors and target databases.
---

## Database Setup

When using [CDC data sources](../api/sources-sinks#cdc) in Jet, one must
also take care that the source databases are set up correctly, meaning
they have all the features required for change data capture enabled.

### MySQL

#### Database user must be set up correctly

The MySQL CDC source needs a MySQL database user which will be used for
connecting to the database. For how to create one, see the "CREATE USER
Statement" in the MySQL Reference Manual
([5.7](https://dev.mysql.com/doc/refman/5.7/en/create-user.html),
[8.0](https://dev.mysql.com/doc/refman/8.0/en/create-user.html)).

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

#### Binlog must be enabled

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

#### Session timeouts should be large enough

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

### Other Databases

Streaming CDC data from other databases supported by Debezium is
possible in Jet by using the [generic Debezium
source](/javadoc/{jet-version}/com/hazelcast/jet/cdc/DebeziumCdcSources.html).
This deployment guide however only covers the databases we have first
class support for. For the other ones pls. refer to the Debezium
documentation:

* [PostgreSQL](https://debezium.io/documentation/reference/1.1/connectors/postgresql.html#setting-up-PostgreSQL)
* [SQL
  Server](https://debezium.io/documentation/reference/1.1/connectors/sqlserver.html#setting-up-sqlserver)
* [MongoDB](https://debezium.io/documentation/reference/1.1/connectors/mongodb.html#setting-up-mongodb)
* [Oracle](https://debezium.io/documentation/reference/1.1/connectors/oracle.html#setting-up-oracle)
* [Db2](https://debezium.io/documentation/reference/1.1/connectors/db2.html#setting-up-Db2)
* [Cassandra](https://debezium.io/documentation/reference/1.1/connectors/cassandra.html#setting-up-cassandra)

### Connector Deployment

#### CDC from replica

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

#### GTID in multi-member clusters

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
