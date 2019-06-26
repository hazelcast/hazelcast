# Hazelcast Jet

![GitHub release](https://img.shields.io/github/release/hazelcast/hazelcast-jet.svg)
[![Join the chat at https://gitter.im/hazelcast/hazelcast-jet](https://badges.gitter.im/hazelcast/hazelcast-jet.svg)](https://gitter.im/hazelcast/hazelcast-jet?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<img src="https://github.com/hazelcast/hazelcast-jet/raw/master/logo/hazelcast-jet.png" width="100">

----

[Hazelcast Jet] is an open-source, cloud-native, distributed stream
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
    <version>3.1</version>
</dependency>
```

Since Jet is embeddable, this is all you need to start your first Jet
instance! Read on for a quick example of your first Jet program.

### Batch Processing with Jet

Use this code to start an instance of Jet and tell it to perform some
computation:

```java
String path = "books";

JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();

p.drawFrom(Sources.files(path))
        .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
        .filter(word -> !word.isEmpty())
        .groupingKey(word -> word)
        .aggregate(AggregateOperations.counting())
        .drainTo(Sinks.logger());

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

JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();

p.drawFrom(Sources.fileWatcher(path))
        .withIngestionTimestamps()
        .setLocalParallelism(1)
        .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
        .filter(word -> !word.isEmpty())
        .groupingKey(word -> word)
        .window(WindowDefinition.tumbling(1000))
        .aggregate(AggregateOperations.counting())
        .drainTo(Sinks.logger());

jet.newJob(p).join();
```

Before running this make an empty directory and point the `path`
variable to it. While the job is running copy some text files into it
and Jet will process them right away.

## Features:

* Constant low-latency - predictable latency is a design goal
* Zero dependencies - single JAR which is embeddable (minimum JDK 8)
* Cloud Native - with [Docker images](https://hub.docker.com/r/hazelcast/hazelcast-jet/)
and [Kubernetes support](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/master/integration/kubernetes)
including Helm Charts.
* Elastic - Jet can scale jobs up and down while running
* Fault Tolerant - At-least-once and exactly-once processing guarantees
* In memory storage - Jet provides robust distributed in memory storage
for caching, enrichment or storing job results
* Sources and sinks for Apache Kafka, Hadoop, Hazelcast IMDG, sockets, files
* Dynamic node discovery for both on-premise and cloud deployments.

## Distribution

You can download the distribution package which includes command-line
tools from [jet.hazelcast.org](http://jet.hazelcast.org/download/).

## Documentation

See the [Hazelcast Jet Reference Manual].

## Code Samples

See [Hazelcast Jet Code Samples] for some examples.

## Additional Connectors

See [hazelcast-jet-contrib](github.com/hazelcast/hazelcast-jet-contrib) repository for community supported
connectors and tools.

## Architecture

See the [architecture](https://jet.hazelcast.org/architecture/) and
[performance](https://jet.hazelcast.org/performance/) pages for
more details about Jet's internals and design.

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
        <version>3.2-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Build From Source

#### Requirements

* JDK 8 or later
* [Apache Maven](https://maven.apache.org/) version 3.5.2 or later

To build, use the command:

```
mvn clean package -DskipTests
```

### Contributions

We encourage pull requests and process them promptly.

To contribute:

* Complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)
* If you're not familiar with Git, see the [Hazelcast Guide for Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process

### Community

Hazelcast Jet team actively answers questions on [Stack Overflow](https://stackoverflow.com/tags/hazelcast-jet).

You are also encouraged to join the [hazelcast-jet mailing list](http://groups.google.com/group/hazelcast-jet)
if you are interested in community discussions

## License

Hazelcast Jet is available under the Apache 2 License. Please see the
[Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing) for more information.

## Copyright

Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.


[Hazelcast Jet]: http://jet.hazelcast.org
[Hazelcast Jet Reference Manual]: https://docs.hazelcast.org/docs/jet/latest/manual/
[Hazelcast Jet Code Samples]: https://github.com/hazelcast/hazelcast-jet-code-samples
