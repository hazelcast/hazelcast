# Hazelcast Jet

![GitHub release](https://img.shields.io/github/release/hazelcast/hazelcast-jet.svg)
[![Join the community on Slack](https://hz-community-slack.herokuapp.com/badge.svg)](https://slack.hazelcast.com)
[![Code Quality: Java](https://img.shields.io/lgtm/grade/java/g/hazelcast/hazelcast.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/hazelcast/hazelcast-jet/context:java)
[![Docker pulls](https://img.shields.io/docker/pulls/hazelcast/hazelcast-jet)](https://img.shields.io/docker/pulls/hazelcast/hazelcast-jet)

<img src="https://github.com/hazelcast/hazelcast-jet/raw/master/logo/hazelcast-jet.png" width="100">

----

[Hazelcast Jet](https://jet-start.sh/) is an open-source, cloud-native, distributed stream
and batch processing engine.

Jet is simple to set up. The nodes you start discover each other and
form a cluster automatically. You can do the same locally, even on the
same machine (your laptop, for example). This is great for quick testing.

With Jet it's easy to build fault-tolerant and elastic data processing
pipelines. Jet keeps processing data without loss even if a node fails,
and you can add more nodes that immediately start sharing the
computation load.

You can embed Jet as a part of your application, it's just a single JAR
without dependencies. You can also deploy it standalone, as a
stream-processing cluster.

Jet also provides a highly available, distributed in-memory data store.
You can cache your reference data and enrich the event stream with it,
store the results of a computation, or even store the input data you're
about to process with Jet.

----

## Start using Jet

Add this to your `pom.xml` to get the latest Jet as your project
dependency:

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>4.1</version>
</dependency>
```

Since Jet is embeddable, this is all you need to start your first Jet
instance! Read on for a quick example of your first Jet program.

### Batch Processing with Jet

Use this code to start an instance of Jet and tell it to perform some
computation:

```java
String path = "books";

JetInstance jet = Jet.bootstrappedInstance();

Pipeline p = Pipeline.create();

p.readFrom(Sources.files(path))
 .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
 .filter(word -> !word.isEmpty())
 .groupingKey(word -> word)
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());

jet.newJob(p).join();
```

When you run this, point the `path` variable to some directory with text
files in it. Jet will analyze all the files and give you the word
frequency distribution in the log output (for each word it will say how
many times it appears in the files).

The above was an example of processing data at rest (i.e., _batch
processing_). It's conceptually simpler than stream processing so we
used it as our first example.

### Stream Processing with Jet

For stream processing you need a streaming data source. A simple example
is watching a folder of text files for changes and processing each new
appended line. Here's the code you can try out:

```java
String path = "books";

JetInstance jet = Jet.bootstrappedInstance();

Pipeline p = Pipeline.create();

p.readFrom(Sources.fileWatcher(path))
 .withIngestionTimestamps()
 .setLocalParallelism(1)
 .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
 .filter(word -> !word.isEmpty())
 .groupingKey(word -> word)
 .window(WindowDefinition.tumbling(1000))
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());

jet.newJob(p).join();
```

Before running this make an empty directory and point the `path`
variable to it. While the job is running copy some text files into it
and Jet will process them right away.

## Features:

* Constant low latency - predictable latency is a design goal
* Zero dependencies - single JAR which is embeddable (minimum JDK 8)
* Cloud Native - with [Docker images](https://hub.docker.com/r/hazelcast/hazelcast-jet/)
and [Kubernetes support](https://jet-start.sh/docs/operations/kubernetes)
including Helm Charts.
* Elastic - Jet can scale jobs up and down while running
* Fault Tolerant - At-least-once and exactly-once processing guarantees
* In-memory storage - Jet provides robust distributed in-memory storage
for caching, enrichment or storing job results
* Sources and sinks for Apache Kafka, Hadoop, Hazelcast IMDG, sockets, files
* Dynamic node discovery for both on-premise and cloud deployments.

## Distribution

You can download the distribution package which includes command-line
tools from [https://jet-start.sh](https://jet-start.sh/download).

## Getting Started and Documentation

See the [Hazelcast Jet Getting Started Guide](https://jet-start.sh/docs/get-started/intro).

## Code Samples

See [examples folder](examples) for some examples.

## Architecture

See the following architecture pages for more insight into the internals of Jet:

* [Cooperative Multithreading](https://jet-start.sh/docs/architecture/execution-engine)
* [Fault Tolerance](https://jet-start.sh/docs/architecture/fault-tolerance)
* [In-memory storage](https://jet-start.sh/docs/architecture/in-memory-storage)
* [Event-time processing](https://jet-start.sh/docs/architecture/event-time-processing)

## Connectors

You can see a full list of connectors at the [Sources and Sink](https://jet-start.sh/docs/api/sources-sinks) section of the docs. A summary is below:

| Name                                                         | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| [Amazon S3](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/s3/src/main/java/com/hazelcast/jet/s3) | A connector that allows AWS S3 read/write support for Hazelcast Jet. |
| [Apache Avro](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/avro)   | Source and sink connector for Avro files.                                                     |
| [Apache Hadoop](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/hadoop/src/main/java/com/hazelcast/jet/hadoop) | A connector that allows Apache Hadoop read/write support for Hazelcast Jet. |
| [Apache Kafka](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/kafka) | A connector that allows consuming/producing events from/to Apache Kafka. |
| [CDC Debezium](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-debezium) | A Hazelcast Jet connector for Debezium which enables Hazelcast Jet pipelines to consume CDC events from various databases. |
| [CDC MySQL](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-mysql) | A Hazelcast Jet connector for CDC data coming from MySQL databases (based on Debizium wrapped with a proprietary API). |
| [Elasticsearch](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/elasticsearch) | A Hazelcast Jet connector for Elasticsearch for querying/indexing objects from/to Elasticsearch. |
| [Files](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)            | Connector for local filesystem.                                               |
| [Hazelcast Cache Journal](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java) | Connector for change events on caches in local and remote Hazelcast clusters. |
| [Hazelcast Cache](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)         | Connector for caches in local and remote Hazelcast clusters.                  |
| [Hazelcast List](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)          | Connector for lists in local and remote Hazelcast clusters.                   |
| [Hazelcast Map Journal](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)   | Connector for change events on maps in local and remote Hazelcast clusters.   |
| [Hazelcast Map](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)           | Connector for maps in local and remote Hazelcast clusters.                    |
| [InfluxDb](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/influxdb) | A Hazelcast Jet Connector for InfluxDb which enables pipelines to read/write data points from/to InfluxDb. |
| [JDBC](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)          | Connector for relational databases via JDBC.                                  |
| [JMS](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)           | Connector for JMS topics and queues.|
| [Kafka Connect](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/kafka-connect) | A generic Kafka Connect source provides ability to plug any Kafka Connect source for data ingestion to Jet pipelines.|
| [MongoDB](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/mongodb) | A Hazelcast Jet connector for MongoDB for querying/inserting objects from/to MongoDB. |
| [Redis](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/redis) | Hazelcast Jet connectors for various Redis data structures.  |
| [Socket](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)        | Connector for TCP sockets.                                                    |
| [Twitter](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/twitter)  | A Hazelcast Jet connector for consuming data from Twitter stream sources in Jet pipelines. |

See [hazelcast-jet-contrib](https://github.com/hazelcast/hazelcast-jet-contrib) repository for more detailed information on community supported connectors and tools. 

## Start Developing Hazelcast Jet

### Use Latest Snapshot Release

You can always use the latest snapshot release if you want to try the features
currently under development.

Maven snippet:

```xml
<repositories>
    <repository>
        <id>snapshot-repository</id>
        <name>Maven2 Snapshot Repository</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>daily</updatePolicy>
        </snapshots>
    </repository>
</repositories>
<dependencies>
    <dependency>
        <groupId>com.hazelcast.jet</groupId>
        <artifactId>hazelcast-jet</artifactId>
        <version>4.2-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Build From Source

#### Requirements

* JDK 8 or later

To build on Linux/MacOS X use:
```
./mvnw clean package -DskipTests
```
for Windows use:
```
mvnw clean package -DskipTests
```

### Contributions

We encourage pull requests and process them promptly.

To contribute:

* Complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)
* If you're not familiar with Git, see the [Hazelcast Guide for Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process

### Community

Hazelcast Jet team actively answers questions on [Stack Overflow](https://stackoverflow.com/tags/hazelcast-jet)
and [Gitter](https://gitter.im/hazelcast/hazelcast-jet).

You are also encouraged to join the [hazelcast-jet mailing list](http://groups.google.com/group/hazelcast-jet)
if you are interested in community discussions

## License
          
Source code in this repository is covered by one of two licenses:   

 1. [Apache License 2.0](licenses/apache-v2-license.txt)   
 2. [Hazelcast Community License](licenses/hazelcast-community-license.txt).   

The default license throughout the repository is Apache License 2.0 unless the  
header specifies another license. Please see the [Licensing section](https://jet-start.sh/license) for more information.

## Copyright

Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
