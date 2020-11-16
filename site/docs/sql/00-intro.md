---
title: Hazelcast Jet SQL
description: Introduction to Hazelcast Jet SQL features.
---

Hazelcast Jet allows you to create a Jet processing job using the
familiar SQL language. It can execute distributed SQL statements over
Hazelcast IMaps and external data sets.

**Note:** _The service is in beta state. Behavior and API might change
in future releases. Binary compatibility is not guaranteed between minor
or patch releases._

## Overview

In the first release, Jet SQL supports the following features:

- SQL Queries over [Apache Kafka topics](05-kafka-connector.md) and
[files (local and remote)](04-files-connector.md)
- Joining Kafka or file data with local IMaps (enrichment)
- Filtering and projection using [SQL
expressions](https://docs.hazelcast.org/docs/{imdg-version}/manual/html-single/index.html#expressions)
- Aggregating data from files using predefined
[aggregate functions](00a-basic-commands#aggregation-functions)
- Receiving query results via Jet client (Java) or writing the results
to an [IMap](03-imap-connector.md) in the Jet cluster
- Running continuous (streaming) and batch queries, see
[Job Management](02-job-management.md)

These are some of the features on our roadmap:

- Joins with arbitrary external data sources
- Windowed aggregation
- JDBC

## Example: How to query Apache Kafka using SQL

We have a topic called `trades` with stock trade events. Trades are
encoded as JSON messages:

```json
{ "ticker": "ABCD", "price": 5.5 }
```

To use a remote topic as a table in Jet, first create an `EXTERNAL
MAPPING`. It maps the JSON messages to a fixed list of columns with
data types:

```sql
CREATE MAPPING trades (
    ticker VARCHAR,
    value DECIMAL)
TYPE Kafka
OPTIONS (
    valueFormat 'json',
    "bootstrap.servers" '1.2.3.4',
    "value.deserializer" 'org.apache.kafka.connect.json.JsonSerializer')
```

To submit the above query, use the Java API (we plan JDBC support and
support in non-Java clients in the future):

```java
JetInstance inst = ...;
inst.getSql().execute( /* query text */ );
```

A SQL query can now be used to read from the `trades` topic, as if it
was a table:

```java
JetInstance inst = ...;
try (SqlResult result = inst.getSql().execute("SELECT * FROM trades")) {
    for (SqlRow row : result) {
        // Process the row.
    }
}
```

The query now runs in the Jet cluster and streams the results to the Jet
client that started it. The iteration will never complete, a Kafka topic
is a _streaming source_, that is it has no end of data. The backing Jet
job will terminate when the client crashes, or add a `result.close()`
call to close it earlier. By default the reading starts at the tip of
the topic, but you can modify it by adding `auto.offset.reset` Kafka
property to toe mapping options.

Jet can also update an IMap using the `SINK INTO` command, which means
you can use Jet SQL as a simple API to ingest data and store it in the
IMDG. To be able to write to an IMap, Jet has to know what objects to
create for the map key and value. It can derive that automatically by
sampling an existing entry in the map, but if the map may be empty, you
have to create a mapping for it, too.

```sql
CREATE MAPPING latest_trades
TYPE IMap
OPTIONS (
    keyFormat 'java',
    keyJavaClass 'java.lang.String',
    valueFormat 'java',
    valueJavaClass 'java.math.BigDecimal'
)
```

Note that we omitted the column list in this query. It will be
determined automatically according to the options. Since we use `String`
and `BigDecimal` as the key and java class, the default name for key
(`__key`) and for the value (`this`) will be used. So the mapping will
behave as if the following columns were specified in the previous
statement:

```sql
(
    __key VARCHAR,
    this DECIMAL
)
```

The SINK INTO command will look like this:

```sql
SINK INTO latest_trades(__key, this)
SELECT ticker, value
FROM trades
```

It will put the `ticker` and `value` fields from the messages in the
topic into the `latest_trades` IMap. The `ticker` field will be used as
the map key and the `value` field will be used as the map value. If
multiple messages have the same ticker, the entry in the target map will
be overwritten, as they arrive.

However, you cannot directly execute the above command because it would
never complete. Remember, it's reading from a Kafka topic. Since it
doesn't return any rows to the client, you must create a job for it:

```sql
CREATE JOB trades_ingestion AS
SINK INTO latest_trades(__key, this)
SELECT ticker, value
FROM trades
```

The above command will create a job named `trades_ingestion`. Now, even
if the client disconnects, the cluster will continue running the job.

To cancel the job, use:

```sql
DROP JOB trades_ingestion
```

## Installation

You need the `hazelcast-jet-sql` module on your classpath. If you're
using Gradle or Maven, make sure to add this module to the dependencies:

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

If you're using the distribution package, make sure to move the
`hazelcast-jet-sql-{jet-version}.jar` file from the `opt/` to the `lib/`
directory.

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
