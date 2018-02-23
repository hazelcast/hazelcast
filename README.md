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
* Distributed java.util.stream API support for Hazelcast data
structures such as IMap and IList.
* Connectors allowing high-velocity ingestion of data from Apache
Kafka, HDFS, Hazelcast IMDG, sockets and local data files (such as
logs or CSVs).
* API for custom connectors.
* Dynamic node discovery for both on-premise and cloud deployments.
* Virtualization support and resource management via Docker, Apache
jclouds, Amazon Web Services, Microsoft Azure, Consul, Heroku,
Kubernetes, Pivotal Cloud Foundry and Apache ZooKeeper.

### Getting Started

See the
[Getting Started Guide](http://jet.hazelcast.org/getting-started/).


### Code Samples

See
[Hazelcast Jet Code Samples](https://github.com/hazelcast/hazelcast-jet-code-samples)
for some examples.

### Documentation

See the [Jet Reference Manual](https://docs.hazelcast.org/docs/jet/0.6/).

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

Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
