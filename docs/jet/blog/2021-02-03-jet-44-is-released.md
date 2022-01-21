---
title: Jet 4.4 Released
author: Marko Topolnik
authorURL: https://twitter.com/mtopolnik
authorImageURL: https://pbs.twimg.com/profile_images/922726943614783488/Pb5DDGWF_400x400.jpg
---

Today we're releasing Hazelcast Jet 4.4 and we have some exciting new
features!

## Jet SQL

Hazelcast Jet 4.4 brings you the first beta version of our SQL
interface. You can now log into Jet from the command line and issue
queries against the data sources you specify. They can be both data at
rest (_batch_ sources) and live feeds (_streaming_ sources).

If you have Docker at hand, here's something you can try out right now!
(For examples that don't require Docker, go to the
[docs](/docs/sql/intro).)

```bash
docker pull hazelcast/hazelcast-jet
docker network create jet-network
docker run --name jet --network jet-network -v "$(pwd)":/csv-dir --rm hazelcast/hazelcast-jet
```

Wait for a message like this in the output:

```text
2021-01-15 17:50:18,645 [ INFO] [main] [c.h.c.LifecycleService]:
    [172.17.0.2]:5701 is STARTED
```

Now start another terminal window and enter the SQL shell:

```text
$ docker run --network jet-network -it --rm hazelcast/hazelcast-jet jet --targets jet sql
Connected to Hazelcast Jet 4.4 at [172.17.0.2]:5701 (+0 more)
Type 'help' for instructions
sql〉
```

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
sql〉 SELECT key, sum(key) as total FROM (
          SELECT v/2 as key FROM TABLE(generate_series(0, 7))
      ) GROUP BY key;
+--------------------+--------------------+
|                 key|               total|
+--------------------+--------------------+
|                   0|                   0|
|                   1|                   2|
|                   2|                   4|
|                   3|                   6|
+--------------------+--------------------+
4 row(s) selected
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
^C
Query cancelled.
sql〉 SELECT * FROM TABLE(generate_stream(100)) WHERE v / 10 * 10 = v;
+--------------------+
|                   v|
+--------------------+
|                   0|
|                  10|
|                  20|
^C
Query cancelled.
sql〉
```

For more examples with CSV files, Kafka and IMap, go to the
[docs](/docs/sql/intro).

We're currently very focused on bringing more features to our SQL, so
stay tuned!

## File Connector

The [Unified File Connector API](/docs/api/sources-sinks) gives you a
simple way to read files, unified across different storage systems.
Using the same API you can read files from the local filesystem, Hadoop
FS, Amazon S3, Google Cloud Storage, and Azure Blob Storage. At the same
time, the connector supports a variety of encoding formats: text files,
CSV, JSON, Avro, etc., equally for all storage systems.

Here's how the Java syntax looks:

```java
BatchSource<String> source = FileSources
    .files("/path/to/my/directory")
    .build();
```

You specify the storage system type with the URI schema, for example to
access S3:

```java
BatchSource<String> source = FileSources
    .files("s3a://bucket-id/path/to/my/directory")
    .build();
```

And this is how you tell it to use the Avro encoding:

```java
BatchSource<User> source = FileSources
    .files("s3a://bucket-id/path/to/my/directory")
    .format(FileFormat.<User>avro())
    .build();
```

Read more in the [Programming Guide](/docs/api/sources-sinks).

## Kinesis Connector

[Amazon Kinesis Data
Streams](https://aws.amazon.com/kinesis/data-streams/) (KDS) is a
durable, scalable real-time data streaming service native to the AWS
environment, and fully managed by it. You can use it as both a source
and a sink in a Hazelcast Jet pipeline:

```java
StreamSource<Map.Entry<String, byte[]>> source = KinesisSources
        .kinesis("Tweets")
        .withInitialShardIteratorRule(".*", "LATEST", null)
        .build();
Sink<Entry<String, byte[]>> sink = KinesisSinks
        .kinesis("Tweets")
        .build();
Pipeline p = Pipeline.create();
p.readFrom(source)
 .withoutTimestamps()
 .map(e -> entry(e.getKey(), (new String(e.getValue()) + "-processed")
         .getBytes(UTF_8)))
 .writeTo(sink);
```

Check out our [tutorial](/docs/tutorials/kinesis) for a full example.

## Enforce Strict Event Order

Hazelcast Jet's primary focus is to leverage all opportunities to
improve the throughput and latency of its computation. One example is
using logic that isn't sensitive to the exact event order. Jet can use
this freedom to optimally load-balance the data across parallel tasks.
This works great for stateless transforms like `map` and `filter` as
well as aggregate operations specifically written in terms of
commutative and associative functions. However, Jet also supports
transforms such as `mapStateful`, where reordering any two events is
likely to result in different output.

In version 4.4 we provide a new option,
`pipeline.setPreserveOrder(true)`, which tells Jet to disable the
dataflow optimizations that result in reordered events. One consequence
of enabling it is that the level of parallelism in the source stage
determines the parallelism of all the subsequent stages because the data
flows in parallel lanes through the pipeline. So if you have a source
that isn't paralellized, your whole pipeline won't be parallelized
either (at least until a stage that explicitly changes the order, such
as `rebalance` or `groupingKey`). This feature works best when you have
a partitioned source and you only require strict order among events with
the same key. Then you get both the ordering you need and decent
parallelization.

## Improved Packaging

We used to offer Jet packaged with some hand-picked extensions while you
could add others by downloading them separately. As of 4.4 we offer two
kinds: a full package with all the extensions, and a slim one with none.
Normally you want to use the full package, but if you want to optimize
the download size or disk usage, use the slim package.

Along the same lines, we now provide a slim Docker image,
`hazelcast/hazelcast-jet:4.4-slim`, to serve as the base image in your
Dockerfile that combines it with the extensions, like this:

```Dockerfile
FROM hazelcast-jet:4.4-slim
ARG JET_HOME=/opt/hazelcast-jet
ARG REPO_URL=https://repo1.maven.org/maven2/com/hazelcast/jet
ADD $REPO_URL/hazelcast-jet-kafka/4.4/hazelcast-jet-kafka-4.4-jar-with-dependencies.jar $JET_HOME/lib/
# ... more ADD statements ...
```

See the
[instructions](/docs/operations/docker#build-a-custom-image-from-the-slim-image)
in our docs for more details.

## Full Release Notes

Hazelcast Jet 4.4 is based on IMDG version 4.1.1. Check out its Release
Notes [here](https://docs.hazelcast.org/docs/rn/index.html#4-1-1) and,
for the Enterprise Edition,
[here](https://docs.hazelcast.org/docs/ern/index.html#4-1-1).

Members of the open source community that appear in these release notes:

- @TomaszGaweda
- @hhromic

Thank you for your valuable contributions!

### New Features

- [sql] SQL Beta: submit jobs to Jet from the command-line SQL shell.
  (#2595, #2636, #2648, #2654, #2665, #2729, #2763, #2788)

- [file-api] [017] Unified API to create sources and sinks from
  file-like resources: local filesystem, Amazon S3, Azure Blob Storage
  and Data Lake Storage, Google Cloud Storage (#2518)

- [kinesis] [018] Amazon Kinesis connector (#2656)

- [pipeline-api] [016] Prevent event reordering: by default Jet reorders
  data for performance, now you can disable this to get strict event
  order where you need it.

### Enhancements

- [connectors] @hhromic improved the naming of source and sink stages
  across different connectors, bringing them all in line with the same
  convention `xSource` / `xSink` (#2685)

- [pipeline-api] @TomaszGaweda added the `pipeline.isEmpty()` method
  that tells whether it contains any stage (#2659)

- [core] @TomaszGaweda added the
  `jet.imdg.version.mismatch.check.disabled` config property that
  disables the enforcement of the exact IMDG dependency version. This
  allows adding IMDG quick fixes to the existing Jet release. (#2610)

- [core] New packaging: download either the full package with all the
  extensions enabled, or the minimal package and separately download the
  extensions you want. (#2796)

- [cli] Improved the behavior of `jet submit`: now it waits for the job
  to start and prints a message about it. (#2699)

- [python] Improved the error message when using a Python function but
  Python is not installed. (#2672)

- [kafka] Improved the performance of the Kafka source by fine-tuning
  some timeouts. (#2732)

### Fixes

- [core] Fixed a problem where Jet would close `System.out` during JVM
  shutdown, preventing shutdown hooks from printing to stdout. (#2649)

- [file-connector] Fixed the blocking File connector declaring its
  processors as cooperative, resulting in performance loss. (#2628)

- [file-connector] Several bug fixes in the File connector. (#2772)

- [core] Fixed a leak caused by Jet's ephemeral loggers created for each
  job. They didn't get released from internal maps in the logging
  framework. (#2737)

- [core] Fixed two problems with the `peek` transform. (#2740, #2765)

- [hadoop] Fixed a problem when using Hadoop for local files, it behaved
  as if the files were shared. (#2764)

### Breaking Changes

None.

_If you enjoyed reading this post, check out Jet at
[GitHub](https://github.com/hazelcast/hazelcast-jet) and give us a
star!_
