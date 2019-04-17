## Hazelcast Jet

[Hazelcast Jet](http://jet.hazelcast.org) is a distributed computing
platform built for high-performance stream processing and fast batch
processing. It embeds Hazelcast In-Memory Data Grid (IMDG) to provide
a lightweight, simple-to-deploy package that includes scalable
in-memory storage.

Visit [jet.hazelcast.org](http://jet.hazelcast.org) to learn more
about the architecture and use cases.

### Features:

* Low-latency, high-throughput distributed data processing framework.
* Highly parallel and distributed stream and batch processing of data.
* Connectors allowing high-velocity ingestion of data from Apache
Kafka, HDFS, Hazelcast IMDG, sockets and local data files (such as
logs or CSVs).
* API for custom connectors.
* Dynamic node discovery for both on-premise and cloud deployments.
* Cloud Native - with available [Docker images](https://hub.docker.com/r/hazelcast/hazelcast-jet/) 
and [Kubernetes support](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/master/integration/kubernetes)
including Helm Charts.

### Hello World

#### Batch Example

```java
// create a Jet node
JetInstance jet = Jet.newJetInstance();

// create a distributed map and put some data
IMap<Integer, String> lines = jet.getMap("bookLines");
lines.put(0, "It was the best of times");
lines.put(1, "it was the worst of times");
lines.put(2, "it was the age of wisdom");
lines.put(3, "it was the age of foolishness");
lines.put(4, "it was the epoch of belief");

Pipeline p = Pipeline.create();

Pattern delimiter = Pattern.compile("\\W+");

// minimal word count
p.drawFrom(Sources.map(lines))
        .flatMap(e -> traverseArray(delimiter.split(e.getValue().toLowerCase())))
        .filter(word -> !word.isEmpty())
        .groupingKey(wholeItem())
        .aggregate(counting())
        .drainTo(Sinks.map("counts"));

jet.newJob(p).join();

System.out.println(jet.getMap("counts").entrySet());
```

#### Streaming Example

```java
// enable event journal
JetConfig config = new JetConfig();
config.getHazelcastConfig()
        .addEventJournalConfig(new EventJournalConfig().setMapName("bookLines"));
// create a Jet node
JetInstance jet = Jet.newJetInstance(config);

// create a distributed map and put some data
IMap<Integer, String> lines = jet.getMap("bookLines");

Pipeline p = Pipeline.create();

Pattern delimiter = Pattern.compile("\\W+");

// minimal streaming word count
p.drawFrom(Sources.mapJournal(lines, JournalInitialPosition.START_FROM_OLDEST))
        .withIngestionTimestamps()
        .setLocalParallelism(1)
        .flatMap(e -> traverseArray(delimiter.split(e.getValue().toLowerCase())))
        .filter(word -> !word.isEmpty())
        .groupingKey(wholeItem())
        .window(WindowDefinition.tumbling(TimeUnit.SECONDS.toMillis(1)))
        .aggregate(counting())
        .drainTo(Sinks.logger());

jet.newJob(p);

lines.put(0, "It was the best of times");
Thread.sleep(500);
lines.put(1, "it was the worst of times");
Thread.sleep(500);
lines.put(2, "it was the age of wisdom");
Thread.sleep(500);
lines.put(3, "it was the age of foolishness");
Thread.sleep(500);
lines.put(4, "it was the epoch of belief");
Thread.sleep(500);
lines.put(5, "it was the epoch of incredulity");
Thread.sleep(500);
```

### Getting Started

See the
[Getting Started Guide](http://jet.hazelcast.org/getting-started/).


### Code Samples

See
[Hazelcast Jet Code Samples](https://github.com/hazelcast/hazelcast-jet-code-samples)
for some examples.

### Documentation

See the [Jet Reference Manual](https://jet.hazelcast.org/documentation/).

### Architecture

See [Jet Architecture](https://jet.hazelcast.org/architecture/).

### High Performance Design

See the [write up](https://jet.hazelcast.org/performance/) on our high
performance secret sauce.

### Releases

Download from [jet.hazelcast.org](http://jet.hazelcast.org/download/).

Use Maven snippet:
```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>${hazelcast.jet.version}</version>
</dependency>
```

### Snapshot Releases

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
        <version>${hazelcast.jet.version}</version>
    </dependency>
</dependencies>
```

### Building From Source

Pull the latest commits from GitHub (`git pull`) and use Maven to
build (`mvn clean install`). This will also run all the checks and
tests: Checkstyle, FindBugs, and JUnit tests.

Java 8 and Java 9 are supported. 

Maven version 3.5.2 is the minimum version required. 


### Contributing to Hazelcast Jet

We encourage pull requests and process them promptly.

To contribute:

* see [Developing with Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process
* complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)

For an enhancement or larger feature, create a GitHub issue first to
discuss.

Submit your contribution as a pull request on GitHub. Each pull
request is subject to automatic verification, so make sure your
contribution passes the `mvn clean install` build locally before
submitting it.

### Mail Group

Please join the mail group if you are interested in using or
developing Hazelcast Jet.

[http://groups.google.com/group/hazelcast-jet](http://groups.google.com/group/hazelcast-jet)

#### License

Hazelcast Jet is available under the Apache 2 License. Please see the
[Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing) for more information.

#### Copyright

Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
