---
title: Hazelcast Jet SQL
description: Introduction to Hazelcast Jet SQL
---

With Hazelcast Jet you can write SQL to process real-time event streams
as well as data at rest. The data can be stored inside a Hazelcast
cluster (including the Jet cluster itself) or in an external storage
system.

**Note:** _The service is in beta state and supports only a very limited
subset of the planned functionality. The behavior, API, and binary
formats will probably change in future releases._

## Quick Start

Prerequisite is Java.

Download and start Jet by pasting this into your terminal:

```text
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.4/hazelcast-jet-4.4.tar.gz
tar xvf hazelcast-jet-4.4.tar.gz
cd hazelcast-jet-4.4
bin/jet-start
```

Wait for this message in the output:

```text
2021-01-15 13:39:33,156 [ INFO] [main] [c.h.c.LifecycleService]:
    [10.212.134.152]:5701 is STARTED
```

The prompt doesn't return, which allows you to easily stop Jet. We'll
instruct you to restart Jet in a few places, so simply come to this
window, type `Ctrl+C` to stop Jet and type `jet-start` to start it
again.

Now start another terminal window and enter the SQL shell:

```text
$ bin/jet sql
Connected to Hazelcast Jet 4.4 at [192.168.5.13]:5701 (+0 more)
Type 'help' for instructions
sql〉
```

You are now ready to type some SQL. Try this:

```sql
sql〉SELECT * from TABLE(generate_series(1,5));
+------------+
|           v|
+------------+
|           1|
|           2|
|           3|
|           4|
|           5|
+------------+
5 row(s) selected
sql〉SELECT sum(v) FROM TABLE(generate_series(1,5));
+--------------------+
|              EXPR$0|
+--------------------+
|                  15|
+--------------------+
1 row(s) selected
sql〉
```

Here are two more examples with streaming SQL. Streaming queries never
complete, so use `Ctrl+C` to cancel them after a while:

```sql
sql〉SELECT * FROM TABLE(generate_stream(10));
+--------------------+
|                   v|
+--------------------+
|                   0|
|                   1|
|                   2|
|                   3|
|                   4|
|                   5|
^C
Query cancelled.
sql〉SELECT * FROM TABLE(generate_stream(100)) WHERE v / 10 * 10 = v;
+--------------------+
|                   v|
+--------------------+
|                   0|
|                  10|
|                  20|
|                  30|
|                  40|
|                  50|
^C
Query cancelled.
sql〉
```

`generate_stream()` creates a streaming data source that never
completes. It emits `bigint`'s starting from zero at the rate you
indicate with the argument (in events per second).

## Features in this Beta

In this beta release, you can use these:

- [SELECT and WHERE
expressions](https://docs.hazelcast.org/docs/{imdg-version}/manual/html-single/index.html#expressions)
- FROM [Apache Kafka topics](kafka-connector.md) and
[files (local and remote)](file-connector.md)
- JOIN with an IMap inside the Jet cluster (enrichment)
- [INSERT/SINK INTO](basic-commands#insertsink-statement) a Kafka topic
  or an IMap inside the cluster
- [aggregate functions](basic-commands#aggregate-functions) (doesn't
  yet support streaming sources like Kafka)

These are some of the features on our roadmap:

- Joins with arbitrary external data sources
- Windowed aggregation of streaming data
- JDBC

## CREATE EXTERNAL MAPPING

There is no native storage system in Jet SQL, instead it works with
_external mappings_ to access various resources as if they were tables.
This includes its own internal IMaps.

This is how you can create a mapping to a Kafka topic `trades` with
JSON messages:

```sql
sql〉CREATE EXTERNAL MAPPING trades (
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT)
TYPE Kafka
OPTIONS (
    'valueFormat' = 'json',
    'bootstrap.servers' = '127.0.0.1:9092'
);
OK
sql〉SHOW MAPPINGS;
+--------------------+
|name                |
+--------------------+
|tradeMap            |
+--------------------+
1 row(s) selected
sql〉
```

The [DDL](ddl) section has more details.

## Query a CSV File

To try out the CSV file format, first activate the CSV connector:

```bash
$ mv opt/hazelcast-jet-csv-4.4.jar lib/
$
```

Restart the Jet server for this to take effect.

Create a sample CSV file named `trades.csv`:

```bash
$ vi trades.csv

id,name
1,Jerry
2,Greg
3,Mary
```

Now you can write the SQL:

```sql
sql〉CREATE MAPPING csv_trades (id TINYINT, name VARCHAR)
TYPE File
OPTIONS ('format'='csv',
    'path'='/path/to/hazelcast-jet-4.4', 'glob'='trades.csv');
OK
sql〉select * from csv_trades;
+----+--------------------+
|  id|name                |
+----+--------------------+
|   1|Jerry               |
|   2|Greg                |
|   3|Mary                |
+----+--------------------+
2 row(s) selected
sql〉
```

See the [File Connector](file-connector) page for more details.

## Query a Kafka Topic

Let's start by installing Kafka. Paste these into your terminal:

```text
wget http://mirror.cc.columbia.edu/pub/software/apache/kafka/2.7.0/kafka_2.13-2.7.0.tgz
tar xvf kafka_2.13-2.7.0.tgz
cd kafka_2.13-2.7.0
```

Now start ZooKeeper, then Kafka:

```bash
$ bin/zookeeper-server-start.sh config/zookeeper.properties
...
[2021-01-20 12:44:04,863] INFO Created server with tickTime 3000 minSessionTimeout 6000 maxSessionTimeout 60000 datadir /tmp/zookeeper/version-2 snapdir /tmp/zookeeper/version-2 (org.apache.zookeeper.server.ZooKeeperServer)
...
<Type Ctrl-Z to get the prompt back>
[1]+  Stopped                 bin/zookeeper-server-start.sh config/zookeeper.properties
$ bg
[1]+ bin/zookeeper-server-start.sh config/zookeeper.properties &
$ bin/kafka-server-start.sh config/server.properties
```

From the Jet home directory, activate the Kafka connector:

```bash
$ mv opt/hazelcast-jet-kafka-4.4.jar lib/
$
```

Restart the Jet server for this to take effect.

You are now ready to query Kafka. We'll use JSON messages like this one:

```json
{
    "id": 1,
    "ticker": "ABCD",
    "price": 5.5,
    "amount": 10
}
```

Let's create a streaming query that filters and transforms the trade
events it gets from Kafka:

```sql
sql〉CREATE MAPPING trades (
    id BIGINT,
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT)
TYPE Kafka
OPTIONS (
    'valueFormat' = 'json',
    'bootstrap.servers' = '127.0.0.1:9092'
);
OK
sql〉SELECT ticker, ROUND(price * 100) AS price_cents, amount
  FROM trades
  WHERE price * amount > 100;
+------------+----------------------+-------------------+
|ticker      |           price_cents|             amount|
+------------+----------------------+-------------------+
```

The query is now running, ready to receive messages from Kafka. You can
interrupt it with `Ctrl+C`, but leave it running for now.

Now start another terminal window and push some messages to Kafka:

```sql
sql〉INSERT INTO trades VALUES
  (1, 'ABCD', 5.5, 10),
  (2, 'EFGH', 14, 20);
OK
```

When you go back to the window where the query is running, you'll see
a result row has appeared:

```text
+-----------------+----------------------+-------------------+
|ticker           |           price_cents|             amount|
+-----------------+----------------------+-------------------+
|EFGH             |                  1400|                 20|
```

See the [Kafka Connector](kafka-connector) page for more details.

## Store Query Results in an IMap

You can send the query results to an IMap using the `SINK INTO` clause.
`SINK INTO` is similar to the standard `INSERT INTO`, [see
here](basic-commands#insertsink-statement) for the full details.

This creates a map named `tradeMap` with a Java `Long` as the key
and the JSON trade event as the value, and then stores an entry in it:

```sql
sql〉CREATE MAPPING tradeMap (
    id BIGINT EXTERNAL NAME "__key",
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT)
TYPE IMap
OPTIONS (
    'keyFormat'='bigint',
    'valueFormat'='json');
OK
sql〉SINK INTO tradeMap VALUES (1, 'hazl', 10, 1);
OK
sql〉SELECT * FROM tradeMap;
+----+----------+--------+--------+
|  id|ticker    |   price|  amount|
+----+----------+--------+--------+
|   1|hazl      |10.0000…|       1|
+----+----------+--------+--------+
1 row(s) selected
sql〉
```

## Create a Standalone Streaming Query

Now we can connect the solutions above and tell Jet to syphon the data
from the Kafka topic into the IMap:

```sql
sql〉CREATE JOB ingest_trades AS
  SINK INTO tradeMap(id, ticker, price, amount)
  SELECT id, ticker, price, amount
  FROM trades;
OK
sql〉SHOW JOBS;
+--------------------+
|name                |
+--------------------+
|ingest_trades       |
+--------------------+
1 row(s) selected
```

As we already saw, a streaming query never completes on its own and its
lifecycle is coupled to the shell, but normally you want to create a
long-running query that lives on independently. We achieved this with
`CREATE JOB`.

Let's try it out by publishing some events to the Kafka topic and
checking if they landed in the IMap:

```sql
sql〉INSERT INTO trades VALUES
  (1, 'ABCD', 5.5, 10),
  (2, 'EFGH', 14, 20);
OK
sql〉SELECT * FROM tradeMap;
+---------------+--------------------+----------+--------------------+
|             id|ticker              |     price|              amount|
+---------------+--------------------+----------+--------------------+
|              2|EFGH                |14.000000…|                  20|
|              1|ABCD                |5.5000000…|                  10|
+---------------+--------------------+----------+--------------------+
2 row(s) selected
sql〉
```

To cancel the job, use:

```sql
sql〉DROP JOB ingest_trades;
```

## Use Jet SQL Embedded Inside Your App

If you use Jet as an embedded library, besides the `hazelcast-jet`
dependency add also the `hazelcast-jet-sql` dep:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-sql:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-sql</artifactId>
    <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

Submit a query:

```java
JetInstance jet = Jet.newJetInstance();
String query = "select * from TABLE(generate_series(1,5))";
try (SqlResult result = jet.getSql().execute(query)) {
    for (SqlRow row : result) {
        System.out.println("" + row.getObject(0));
    }
}
```

This code should print

```txt
1
2
3
4
5
```

## IMDG SQL and Jet SQL

Hazelcast IMDG can execute SQL statements using either the default SQL
backend or the Jet SQL backend.

The default SQL backend is designed for quick, ad-hoc queries over
IMaps. The Jet backend allows you to combine various data sources (IMap,
Kafka, files) using a single query. It is designed for long-running
queries (continuous queries, batch processing) and does not optimize for
very low query initialization overhead.

The Hazelcast SQL service selects the backend automatically with a
trial-and-error approach: first try the default IMDG backend, if it
can't execute the statement, try the Jet backend.

This documentation summarizes the additional SQL features of Hazelcast
Jet. For a summary of the default SQL engine features, supported data
types and the built-in functions and operators, please see the [chapter
on SQL](https://docs.hazelcast.org/docs/{imdg-version}/manual/html-single/index.html#sql)
in the Hazelcast IMDG reference manual.
