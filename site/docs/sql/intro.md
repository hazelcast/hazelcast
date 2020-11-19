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

- SQL queries over [Apache Kafka topics](kafka-connector.md) and files
(local and remote) (TODO)
- Joining Kafka or file data with local IMaps (enrichment)
- Filtering and projection using [SQL
expressions](https://docs.hazelcast.org/docs/{imdg-version}/manual/html-single/index.html#expressions)
- Aggregating data from files using predefined [aggregate
functions](basic-commands#aggregation-functions)
- Receiving query results via Jet client (Java) or writing the results
to an [IMap](imap-connector.md) in the Jet cluster
- Running continuous (streaming) and batch queries, see
[Job Management](job-management.md)

These are some of the features on our roadmap:

- Joins with arbitrary external data sources
- Windowed aggregation of streaming data
- JDBC

## Installation

When you use the distribution package, the `hazelcast-jet-sql` is not on
the class path by default. Make sure to move the
`hazelcast-jet-sql-{jet-version}.jar` file from the `opt/` to the `lib/`
directory.

If you use Jet in embedded mode, besides the `hazelcast-jet` dependency
add also the `hazelcast-jet-sql` dep:

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

If you use other features from SQL such as the Kafka or File connectors,
also add the `hazelcast-jet-kafka` or `hazelcast-jet-hadoop` modules in
the same way.

## Example

In this example we'll show how to query Apache Kafka using SQL and
stream the messages to an IMap.

First, make sure you have Apache Kafka up and running. You can follow
the [instructions here](https://kafka.apache.org/quickstart). Start the
broker and create a topic named `trades`.

In our topic the trades will be encoded as JSON messages:

```plain
key: ABCD
value: {
    "ticker": "ABCD",
    "price": 5.5,
    "amount": 10
}
```

The `ticker` is repeated in both the key and the value. We'll ignore the
key in the subsequent examples, it's only used by Kafka for
partitioning.

### Creating the Mapping for Kafka Topic

To use a remote topic as a table in Jet, first create an `EXTERNAL
MAPPING` for the topic. It maps the messages to a fixed list of columns
with data types:

```sql
CREATE EXTERNAL MAPPING trades (
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT)
TYPE Kafka
OPTIONS (
    valueFormat 'json',
    "bootstrap.servers" '127.0.0.1:9092'
    /* ... more configuration options for the Kafka consumer */
)
```

The `valueFormat` option specifies the serialization format for the
value. For other possible values see the [Kafka
Connector](kafka-connector) page. The options not handled by Jet, such
as the `bootstrap.servers` above, are all passed directly to the Kafka
consumer or producer.

// TODO document the CLI

To submit the above query, use the Java API (we plan to support JDBC and
non-Java clients in the future):

```java
JetInstance inst = ...;
inst.getSql().execute( /* query text */ );
```

### Querying the Kafka Topic

A SQL query can now be used to read from the `trades` topic, as if it
was a table:

```java
JetInstance inst = ...;
try (SqlResult result = inst.getSql().execute("SELECT * FROM trades")) {
    for (SqlRow row : result) {
        // Process the row
        System.out.println(row);
    }
}
```

The query now runs in the Jet cluster and streams the results to the Jet
client that started it. The iteration will never complete, a Kafka topic
is a _streaming source_, that is it has no end of data. The backing Jet
job will terminate when the client crashes, or you can add a
`result.close()` call to close it earlier. By default, the reading
starts at the tip of the topic, but you can modify it by adding
`auto.offset.reset` Kafka property to the mapping options.

You can use the power of the SQL language and use expressions and the
`WHERE` clause, e.g.:

```sql
SELECT ticker, ROUND(price * 100) AS price_cents, amount
FROM trades
WHERE price * amount > 100
```

### Creating the Mapping for the IMap

Jet can update an IMap using the `SINK INTO` command, which means you
can use Jet SQL as a simple API to ingest data and store it in IMDG. For
the sake of this tutorial it's enough to think about the `SINK INTO`
command as of a standard `INSERT INTO` command. If you're interested to
know why don't we use the standard command, [see
here](basic-commands#insertsink-statement).

To be able to write to an IMap, Jet has to know what type of objects to
create for the map key and value. It can derive that automatically by
sampling an existing entry in the map, but if the map is empty<sup><a
name='footnote-1-backref'></a>[*](#footnote-1)</sup>, you have to create
a mapping for it first. We want to replicate the topic structure, so we
need to create a Java class for the value:

```java
public class Trade implements Serializable {
    public String ticker;
    public BigDecimal price;
    public long amount;
}
```

We used public fields, but of course you can use private fields and use
setters/getters. This class must be available to the cluster. You can
either add it to the members class paths by creating a JAR file and
adding to the `lib` folder, or you can use User Code Deployment. The
user code deployment has to be enabled on the members; add the following
section to the `config/hazelcast.yaml` file:

```yaml
hazelcast:
  user-code-deployment:
    enabled: true
```

Then use a client to upload the class:

```java
ClientConfig clientConfig = new JetClientConfig();
clientConfig.getUserCodeDeploymentConfig()
            .setEnabled(true)
            .addClass(Trade.class);
JetInstance jet = Jet.newJetClient(clientConfig);
```

After this, you can create the mapping for the IMap. The name of the
IMap is `latest_trades`:

```sql
CREATE MAPPING latest_trades
TYPE IMap
OPTIONS (
    keyFormat 'java',
    keyJavaClass 'java.lang.String',
    valueFormat 'java',
    valueJavaClass 'com.example.Trade'
)
```

Note that we omitted the column list in this query. It will be
determined automatically according to the OPTIONS. Since we use `String`
as the key class, the default column name for the key will be used:
`__key`. The properties of the `Trade` class will be used. So the
mapping will behave as if the following columns were specified in the
previous statement:

```sql
(
    __key VARCHAR,
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT
)
```

### Streaming Messages from the Kafka Topic to the IMap

After creating the mapping for the `latest_trades` IMap, we can submit
the following statement:

```sql
SINK INTO latest_trades(__key, ticker, price, amount)
SELECT ticker, ticker, price, amount
FROM trades
```

It will put the fields from the Kafka topic into the IMap, replicating
the `ticker` twice: once into the value and once into the key. Since we
use the `ticker` as the key, every new trade for the same ticker will
overwrite the previous trade for that ticker, hence the name
`latest_trades`.

### Creating a Long-Running Job

However, you cannot directly execute the above command because it would
never complete. Remember, it's reading from a Kafka topic, which is a
stream of messages. Since it doesn't return any rows to the client, you
must create a job for it:

```sql
CREATE JOB trades_ingestion AS
SINK INTO latest_trades(__key, ticker, price, amount)
SELECT ticker, ticker, price, amount
FROM trades
```

The part after the `AS` keyword is the same as the previous statement.
The command will create a job named `trades_ingestion`. Now, even if the
client disconnects, the cluster will continue running the job.

### Checking the Target IMap Contents

To add some messages to the Kafka topic you can use this command:

```sql
INSERT INTO trades
VALUES ('ABCD', 5.5, 10), ('EFGH', 14, 20) /* ,  ... */
```

While this works, we intend to use this way only for prototyping or
ad-hoc work. Jet is optimized for larger or long-running queries. Under
the hood, it will spin up a distributed job for each INSERT statement.
If you benchmarked the number of records per second, the performance
would be embarrassing.

To check out the contents of the `latest_trades` IMap, use:

```sql
SELECT * FROM latest_trades
```

### Cancelling the Job

To cancel the job, use:

```sql
DROP JOB trades_ingestion
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

----

<span style='font-size:90%'><a
name='footnote-1'>[*)](#footnote-1-backref) This statement is not
entirely correct, it might not be enough if the map has one entry. The
map has to have an entry on every cluster member. Practically, if the
IMap doesn't have plenty of entries, you should always create the
mapping and not rely on the auto-sampling.</span>
