---
title: Hazelcast Jet SQL
description: Introduction to Hazelcast Jet SQL
---

With Hazelcast Jet you can write SQL to process real-time event streams
as well as data at rest. You can pull the data from a Hazelcast cluster
(currently only the Jet cluster itself) or from external systems like
Kafka or S3. You can see the results directly as well as send them to
one of those systems. Jet SQL also has an extension that allows you to
create a long-running background job that continuously takes the
real-time data, processes it, and pushes it to the target system.

**Note:** _The service is in beta state and supports only a very limited
subset of the planned functionality. The behavior, API, and binary
formats may change in future releases._

## Quick Start

You can use our Docker image, or download our tarball and start Jet from
it.

<!--DOCUSAURUS_CODE_TABS-->
<!--Docker-->
Update and start the Jet container by pasting this into your terminal:

```text
docker pull hazelcast/hazelcast-jet
docker network create jet-network
docker run --name jet --network jet-network -v "$(pwd)":/csv-dir --rm hazelcast/hazelcast-jet
```

We created a named Docker network so we can easily make several
containers talk to each other, using the container name as the hostname.

The `-v` option maps the current directory to `/csv-dir` inside the
container, stay in the same directory when you create the file in the
CSV example below. On Windows, due to limitations of Docker, switch to
some subdirectory of `c:\Users` before executing the above commands.

<!--Tarball-->
Prerequisite is Java.

Download and start Jet by pasting this into your terminal:

```text
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.4/hazelcast-jet-4.4.tar.gz
tar xvf hazelcast-jet-4.4.tar.gz
cd hazelcast-jet-4.4
bin/jet-start
```
<!--END_DOCUSAURUS_CODE_TABS-->

----

Wait for a message like this in the output:

```text
2021-01-15 17:50:18,645 [ INFO] [main] [c.h.c.LifecycleService]:
    [172.17.0.2]:5701 is STARTED
```

The prompt doesn't return, which allows you to easily stop or restart
Jet.

Now start another terminal window and enter the SQL shell:

<!--DOCUSAURUS_CODE_TABS-->
<!--Docker-->
```text
$ docker run --network jet-network -it --rm hazelcast/hazelcast-jet jet --targets jet sql
Connected to Hazelcast Jet 4.4 at [172.17.0.2]:5701 (+0 more)
Type 'help' for instructions
sql〉
```

Here, `--targets jet` is the SQL shell's command-line parameter that
tells it to connect to the host named `jet`.

<!--Tarball-->
```text
$ bin/jet sql
Connected to Hazelcast Jet 4.4 at [192.168.0.1]:5701 (+0 more)
Type 'help' for instructions
sql〉
```
<!--END_DOCUSAURUS_CODE_TABS-->

----

You are now ready to write some SQL. Try these:

```sql
sql〉 SELECT * FROM TABLE(generate_series(1,3));
+------------+
|           v|
+------------+
|           1|
|           2|
|           3|
+------------+
3 row(s) selected
sql〉 SELECT sum(v) as total FROM TABLE(generate_series(0, 9));
+--------------------+
|               total|
+--------------------+
|                  45|
+--------------------+
1 row(s) selected
sql〉 SELECT key, sum(key) as total FROM (
          SELECT v/2 as key FROM TABLE(generate_series(0, 9))
      ) GROUP BY key;
+--------------------+--------------------+
|                 key|               total|
+--------------------+--------------------+
|                   1|                   2|
|                   2|                   4|
|                   3|                   6|
|                   4|                   8|
|                   0|                   0|
+--------------------+--------------------+
5 row(s) selected
```

Here are two more examples with streaming SQL. Streaming queries never
complete, so use `Ctrl+C` to cancel them after a while:

```sql
sql〉 SELECT * FROM TABLE(generate_stream(10));
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
sql〉 SELECT * FROM TABLE(generate_stream(100)) WHERE v / 10 * 10 = v;
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

`generate_stream()` generates an infinite stream of values. It emits
`bigint`s starting from zero at the rate you indicate with the argument
(in events per second). For Jet SQL, a stream is like a table with
infinitely many rows which you can only access sequentially and thus
never reach the end. For example, you get an error if you try to
aggregate the whole stream:

```sql
sql〉 SELECT sum(v) FROM TABLE(generate_stream(10));
From line 1, column 1 to line 1, column 45: Grouping/aggregations not
supported for a streaming query
```

For aggregation to work in the setting of an unbounded stream, you have
to apply a windowing function. This is an upcoming feature of Jet SQL.

## Features in this Beta

In this beta release, you can use these:

- [SELECT and WHERE
expressions](https://docs.hazelcast.org/docs/{imdg-minor-version}/manual/html-single/index.html#expressions)
- FROM [Apache Kafka topics](kafka-connector.md) and
[files (local and remote)](file-connector.md)
- JOIN with an IMap inside the Jet cluster (enrichment)
- [INSERT/SINK INTO](basic-commands#insertsink-statement) a Kafka topic
  or an IMap inside the cluster
- [GROUP BY and aggregate functions](basic-commands#aggregate-functions)
  on bounded data (non-streaming)

These are some of the features on our roadmap:

- GROUP BY for IMap
- Windowed aggregation of streaming data
- JOIN with any data source
- JDBC driver

## CREATE EXTERNAL MAPPING

There is no native storage system in Jet SQL, instead it works with
_external mappings_ to access various resources as if they were tables.
This includes its own internal IMaps.

This is how you can create a mapping for a Hazelcast IMap `myMap` with
JSON values:

```sql
sql〉 CREATE EXTERNAL MAPPING myMap (
    id BIGINT EXTERNAL NAME "__key",
    name VARCHAR,
    age INT)
TYPE IMap
OPTIONS (
    'keyFormat'='bigint',
    'valueFormat'='json');
OK
sql〉 SHOW MAPPINGS;
+--------------------+
|name                |
+--------------------+
|myMap               |
+--------------------+
1 row(s) selected
sql〉
```

The [DDL](ddl) section has more details.

## Query a CSV File

Make sure you are in the same directory from which you started Jet.
Create a sample CSV file named `likes.csv`:

```bash
$ vi likes.csv

id,name,likes
1,Jerry,13
2,Greg,108
3,Mary,73
4,Jerry,88
```

Now you can write the SQL:

<!--DOCUSAURUS_CODE_TABS-->
<!--Docker-->
```sql
sql〉 CREATE MAPPING csv_likes (id INT, name VARCHAR, likes INT)
TYPE File
OPTIONS ('format'='csv',
    'path'='/csv-dir', 'glob'='likes.csv');
OK
```

<!--Tarball-->
```sql
sql〉 CREATE MAPPING csv_likes (id INT, name VARCHAR, likes INT)
TYPE File
OPTIONS ('format'='csv',
    'path'='/path/to/curr-dir', 'glob'='likes.csv');
OK
```

<!--END_DOCUSAURUS_CODE_TABS-->

```sql
sql〉 SELECT * FROM csv_likes;
+------------+--------------------+------------+
|          id|name                |       likes|
+------------+--------------------+------------+
|           1|Jerry               |          13|
|           2|Greg                |         108|
|           3|Mary                |          73|
|           4|Jerry               |          88|
+------------+--------------------+------------+
4 row(s) selected
sql〉 SELECT name, sum(likes) as total_likes FROM csv_likes GROUP BY name;
+--------------------+--------------------+
|name                |         total_likes|
+--------------------+--------------------+
|Greg                |                 108|
|Jerry               |                 101|
|Mary                |                  73|
+--------------------+--------------------+
3 row(s) selected
```

See the [File Connector](file-connector) page for more details.

## Query a Kafka Topic

Let's start by running a Kafka server.

<!--DOCUSAURUS_CODE_TABS-->
<!--Docker-->
```text
docker run --name kafka --network jet-network --rm hazelcast/hazelcast-quickstart-kafka
```

<!--Tarball-->
Paste these into your terminal:

```text
wget http://mirror.cc.columbia.edu/pub/software/apache/kafka/2.7.0/kafka_2.13-2.7.0.tgz
tar xvf kafka_2.13-2.7.0.tgz
cd kafka_2.13-2.7.0
```

Now start ZooKeeper, then Kafka:

```text
$ bin/zookeeper-server-start.sh config/zookeeper.properties
...
[2021-01-20 12:44:04,863] INFO Created server with tickTime 3000
  minSessionTimeout 6000 maxSessionTimeout 60000 datadir
  /tmp/zookeeper/version-2 snapdir /tmp/zookeeper/version-2
  (org.apache.zookeeper.server.ZooKeeperServer)
...
<Type Ctrl-Z to get the prompt back>
[1]+  Stopped  bin/zookeeper-server-start.sh config/zookeeper.properties
$ bg
[1]+ bin/zookeeper-server-start.sh config/zookeeper.properties &
$ bin/kafka-server-start.sh config/server.properties
```
<!--END_DOCUSAURUS_CODE_TABS-->

----

You are now ready to query Kafka. We'll use JSON messages like this one:

```json
{
    "id": 1,
    "ticker": "ABCD",
    "price": 5.5,
    "amount": 10
}
```

First write a streaming query that filters and transforms the trade
events it gets from Kafka:

<!--DOCUSAURUS_CODE_TABS-->
<!--Docker-->
```sql
sql〉 CREATE MAPPING trades (
    id BIGINT,
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT)
TYPE Kafka
OPTIONS (
    'valueFormat' = 'json',
    'bootstrap.servers' = 'kafka:9092'
);
OK
```

<!--Tarball-->
```sql
sql〉 CREATE MAPPING trades (
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
```

<!--END_DOCUSAURUS_CODE_TABS-->

```sql
sql〉 SELECT ticker, ROUND(price * 100) AS price_cents, amount
  FROM trades
  WHERE price * amount > 100;
+------------+----------------------+-------------------+
|ticker      |           price_cents|             amount|
+------------+----------------------+-------------------+
```

The Kafka topic is infinite, so this query is a _streaming query_. It is
now running, ready to receive messages from Kafka. You can interrupt it
with `Ctrl+C`, but leave it running for now.

Now start another terminal window and push some messages to Kafka:

```sql
sql〉 INSERT INTO trades VALUES
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

You see only one of the two rows you inserted, the other one was
eliminated by the `WHERE` clause. See the [Kafka
Connector](kafka-connector) page for more details.

## Store Query Results in an IMap

You can send the query results to an IMap using the `SINK INTO` clause.
`SINK INTO` is similar to the standard `INSERT INTO`, [see
here](basic-commands#insertsink-statement) for the full details.

This creates a map named `tradeMap` with a `Long` key and the JSON trade
event as the value, and then stores an entry in it:

```sql
sql〉 CREATE MAPPING tradeMap (
    __key BIGINT,
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT)
TYPE IMap
OPTIONS (
    'keyFormat'='bigint',
    'valueFormat'='json');
OK
sql〉 SINK INTO tradeMap VALUES (1, 'hazl', 10, 1);
OK
sql〉 SELECT * FROM tradeMap;
+-----+----------+--------+--------+
|__key|ticker    |   price|  amount|
+-----+----------+--------+--------+
|    1|hazl      |10.0000…|       1|
+-----+----------+--------+--------+
1 row(s) selected
sql〉
```

## Create a Standalone Streaming Query

Now we can connect the solutions above and tell Jet to syphon the data
from the Kafka topic into the IMap:

```sql
sql〉 CREATE JOB ingest_trades AS
  SINK INTO tradeMap
  SELECT id, ticker, price, amount
  FROM trades;
OK
sql〉 SHOW JOBS;
+--------------------+
|name                |
+--------------------+
|ingest_trades       |
+--------------------+
1 row(s) selected
```

As we already saw, a streaming query never completes on its own and its
lifecycle is coupled to the shell, but normally you want to create a
long-running query that lives independently. We achieved this with
`CREATE JOB`.

Let's try it out by publishing some events to the Kafka topic and
checking if they landed into the IMap:

```sql
sql〉 INSERT INTO trades VALUES
  (1, 'ABCD', 5.5, 10),
  (2, 'EFGH', 14, 20);
OK
sql〉 SELECT * FROM tradeMap;
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
sql〉 DROP JOB ingest_trades;
```

## Use Jet SQL Embedded Inside Your Java App

If you use Jet as an embedded Java library, besides the `hazelcast-jet`
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
String query = "SELECT * FROM TABLE(generate_series(1,5))";
try (SqlResult result = jet.getSql().execute(query)) {
    for (SqlRow row : result) {
        System.out.println("" + row.getObject(0));
    }
}
```

This code will print:

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
on SQL](https://docs.hazelcast.org/docs/{imdg-minor-version}/manual/html-single/index.html#sql)
in the Hazelcast IMDG reference manual.
