---
title: Jet 4.0 is Released
author: Can Gencer
authorURL: http://twitter.com/cgencer
authorImageURL: https://pbs.twimg.com/profile_images/1187734846749196288/elqWdrPj_400x400.jpg
---

We're happy to introduce the release of Jet 4.0 which brings several new
features. This release was a big effort and a total of [230
PRs](https://github.com/hazelcast/hazelcast-jet/pulls?q=is%3Apr+milestone%3A4.0)
were merged, making it one of our biggest in terms of new features.

## Distributed Transactions

Jet previously had first-class support for fault tolerance through an
implementation of the [Chandy-Lamport distributed snapshotting](https://lamport.azurewebsites.net/pubs/chandy.pdf)
algorithm which requires participation from the whole pipeline,
including sources and sinks. Previously, the at-least-once and
exactly-once processing guarantees were only limited to replayable
sources such as Kafka. Jet 4.0 comes with a full two-phase commit (2PC)
implementation which makes it possible to have end-to-end exactly-once
processing with acknowledgement-based sources such as JMS. Jet is now
also able to work with transactional sinks to avoid duplicate writes, and
this version adds transactional file and Kafka sinks, with transactional
JMS and JDBC sinks utilizing XA transactions coming in the next release.

We will have additional posts about this topic in the future detailing
the mechanism and also results of our tests done with 2PC for various
message brokers and databases.

## Python User-Defined Functions

Python is a popular language with a very large ecosystem of libraries,
and has especially become popular in the domain of data processing and
machine learning. Jet itself is a data processing framework for both
streams and batches of data, but the API for defining the pipeline
itself has been previously limited to Java and Java functions only.

In this version we have added a native way to execute Python code within
a Jet pipeline. Jet can now spawn separate Python processes on
each node which communicate back using
[gRPC](https://github.com/hazelcast/hazelcast-jet-demos/tree/master/debezium-cdc-without-kafka).
The processes are fully managed by Jet and can make use of techniques
such as smart batching of events.

The user defines a mapping stage which takes an input item, and
transforms it using a supplied Python function. The function can make
use of libraries such as scikit, numpy and many others. This makes it
possible to use Jet for deploying ML models into production. For
example, given this pipeline:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(10, (ts, seq) -> bigRandomNumberAsString()))
    .withoutTimestamps()
    .apply(mapUsingPython(new PythonServiceConfig()
            .setBaseDir(baseDir)
            .setHandlerModule("take_sqrt")))
    .writeTo(Sinks.observable(RESULTS));
```

The user only has to supply the following Python function:

```python
import numpy as np

def transform_list(input_list):
    """
    Uses NumPy to transform a list of numbers into a list of their square
    roots.

    :param input_list: the list with input items
    :return: the list with input items' square roots
    """
    num_list = [float(it) for it in input_list]
    sqrt_list = np.sqrt(num_list)
    return [str(it) for it in sqrt_list]
```

For a more in-depth discussion on this topic, I recommend Jet Core
Engineer Marko Topolnik's presentation,
[Deploying ML models at scale](https://www.youtube.com/watch?v=q1vBbqxnJIQ).

## Observables

When you submit a Jet pipeline, typically it reads the data from a
source and writes to a sink (such as a `IMap`). When the submitter of
the pipeline wants to read the results, the sink must be read outside of
the pipeline, which is not always very convenient.

In Jet 4.0, a new sink type called `Observable` is added which can be
used to publish messages directly to the caller. It utilizes a Hazelcast
Ringbuffer as the underlying data store which allows the decoupling of
the producer and consumer.

```java
Observable<SimpleEvent> o = jet.newObservable();
o.addObserver(event -> System.out.println(event));
p.readFrom(TestSources.itemStream(10))
 .withoutTimestamps()
 .writeTo(Sinks.observable(o));
jet.newJob(p).join();
```

The `Observable` can also be used to be notified of a job's completion
and any errors that may occur during processing.

## Custom Metrics

Over the last few releases we've been improving the metrics support in
Jet, such as being able to get metrics directly from running or
completed jobs through the use of `Job.getMetrics()`. In this release,
we've made it possible to also add your own custom metrics into a
pipeline through the use of a simple API:

```java
p.readFrom(TestSources.itemStream(10))
 .withoutTimestamps()
 .map(event -> {
     if (event.sequence() % 2 == 0) {
         Metrics.metric("numEvens").increment();
     }
     return event;
 }).writeTo(Sinks.logger());
```

These custom metrics will then be available as part of
`Job.getMetrics()` or through JMX along with the rest of the metrics.

## Debezium, Kafka Connect and Twitter Connectors

As part of Jet 4.0, we're releasing three new connectors:

### Debezium

Debezium is a Change Data Capture (CDC) platform and the new
[Debezium](https://debezium.io/) connector for Jet allows you to stream
changes directly from databases such as MySQL and PostgreSQL without
requiring any other dependencies.

Although Debezium typically requires use of Kafka and Kafka Connect, the
native Jet integration means you can directly stream changes without
having to use Kafka. The integration also supports fault-tolerance so
that when a Jet job is scaled up or down, old changes do not need to
replayed.

This makes it suitable to build an end-to-end solution where for example
an in-memory cache supported by `IMap` is always kept up to date with the
latest changes in the database.

```java
Configuration configuration = Configuration
        .create()
        .with("name", "mysql-inventory-connector")
        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
        /* begin connector properties */
        .with("database.hostname", mysql.getContainerIpAddress())
        .with("database.port", mysql.getMappedPort(MYSQL_PORT))
        .with("database.user", "debezium")
        .with("database.password", "dbz")
        .with("database.server.id", "184054")
        .with("database.server.name", "dbserver1")
        .with("database.whitelist", "inventory")
        .with("database.history.hazelcast.list.name", "test")
        .build();

Pipeline p = Pipeline.create();
p.readFrom(DebeziumSources.cdc(configuration))
 .withoutTimestamps()
 .map(record -> Values.convertToString(record.valueSchema(), record.value()))
 .writeTo(Sinks.logger());
```

The Debezium connector is currently available in the
[hazelcast-jet-contrib repository](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/debezium),
along with a [demo application](https://github.com/hazelcast/hazelcast-jet-demos/tree/master/debezium-cdc-without-kafka).

### Kafka Connect

The [Kafka Connect source](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/kafka-connect)
allows you to use any existing Kafka Connect source and use it natively
with Jet, without requiring presence of a Kafka Cluster. The records
will be streamed as Jet events instead, which can be processed further
and it has full support for fault-tolerance and replaying. A full list
of connectors can be viewed through [Confluent Hub](https://www.confluent.io/hub/).

### Twitter

We've also released a simple [Twitter source](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/twitter)
that uses the Twitter client, which can be used to process a stream of
Tweets.

```java
Properties credentials = new Properties();
properties.setProperty("consumerKey", "???"); // OAuth1 Consumer Key
properties.setProperty("consumerSecret", "???"); // OAuth1 Consumer Secret
properties.setProperty("token", "???"); // OAuth1 Token
properties.setProperty("tokenSecret", "???"); // OAuth1 Token Secret
List<String> terms = Arrays.asList("term1", "term2");
StreamSource<String> streamSource =
             TwitterSources.stream(credentials,
                     () -> new StatusesFilterEndpoint().trackTerms(terms)
             );
Pipeline p = Pipeline.create();
p.readFrom(streamSource)
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

These connectors are currently under incubation, and will be part of a
main release in the future.

## Improved Jet Installation

We've also made many improvements to the Jet installation package. It
has been cleaned up to reduce the size, and now supports the following:

* Default config format is now YAML and many of the common options are
  in the default configuration.
* A rolling file logger which writes to the log folder is now the
  default logger
* Support for daemon mode through `jet-start -d` switch.
* Improved readme and a new "hello world" application which can be
  submitted right after installation.
* Improved JDK9+ support, to avoid illegal import warnings.

## Hazelcast 4.0

Another major change that's worth noting is that Jet is now based on
Hazelcast 4.0 - which in itself was a major release and brought many new
features and technical improvements such as improved performance and
Intel Optane DC Support and encryption at rest.

## Breaking Changes and Migration Guide

As part of 4.0, we've also done some house cleaning and as a result some
things have been moved around. All the changes are listed as part of the
[migration guide blog post](/blog/2020/04/01/upgrading-to-jet-40).

We are committed to backwards compatibility going forward and any
interfaces or classes which are subject to change will be marked as
`@Beta` or `@EvolvingApi` going forwards.

## Wrapping Up

This is a big release for Hazelcast Jet, and we have many more exciting
features in the pipeline (pun intended), including SQL support, extended
support for 2PC, improved Serialization support, even more connectors,
Kubernetes Operators and many more. We will also be aiming to make
shorter, more frequent releases to bring new features to users quicker.
