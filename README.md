## Hazelcast Jet

[Hazelcast Jet](http://jet.hazelcast.org) is a distributed computing platform built for high-performance stream processing and fast batch processing. It embeds Hazelcast In Memory Data Grid (IMDG) to provide a lightweight package of a processor and a scalable in-memory storage.

Visit [jet.hazelcast.org](http://jet.hazelcast.org) to learn more about the architecture and use-cases.

### Features:

* Low latency and distributed general data processing framework with high throughput.
* Highly parallel and distributed stream and batch processing of data.
* Distributed java.util.stream API support for Hazelcast data structures such as IMap and IList.
* Connectors allowing high-velocity ingestion of data from Apache Kafka, HDFS, Hazelcast IMDG, sockets and local data files (such as logs or CSVs)
* API for custom connectors
* Distributed implementations of java.util.{Queue, Set, List, Map} data structures highly optimized to be used for the processing
* Dynamic node discovery for both on-premise and cloud deployments.
* Virtualization support and resource management via Docker, YARN and Mesos


### Getting Started

See [Getting Started Guide](http://jet.hazelcast.org/getting-started/)


### Code Samples

See [Hazelcast Jet Code Samples](https://github.com/hazelcast/hazelcast-jet-code-samples) for some examples.

### Documentation

See documentation at [jet.hazelcast.org](http://jet.hazelcast.org/)

### Releases

Download from [jet.hazelcast.org](http://jet.hazelcast.org/download/)

Use Maven snippet:
```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>${hazelcast.jet.version}</version>
</dependency>
<repository>
    <id>cloudbees-release-repository</id>
    <url>https://repository-hazelcast-l337.forge.cloudbees.com/release/</url>
</repository>
```

### Snapshot Releases

Maven snippet:
```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>${hazelcast.jet.version}</version>
</dependency>
<repository>
    <id>cloudbees-snapshot-repository</id>
    <url>https://repository-hazelcast-l337.forge.cloudbees.com/snapshot/</url>
    <releases>
        <enabled>false</enabled>
    </releases>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

### Building From Source

Pull latest from repo `git pull origin master` and use Maven install (or package) to build `mvn clean install`.

### Testing

Type `mvn test` to run unit and integration tests.

### Checkstyle and Findbugs

In each Pull Request, we do static analyzing on the changes.
Run the following commands locally to check if your contribution is checkstyle and findbugs compatible.

```
mvn clean compile -P findbugs
```

```
mvn clean validate -P checkstyle
```

### Contributing to Hazelcast Jet

We encourage pull requests and process them promptly.

To contribute:

* see [Developing with Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process
* complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)

For an enhancement or larger feature, create a GitHub issue first to discuss.

### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast-jet](http://groups.google.com/group/hazelcast-jet)

#### License

Hazelcast is available under the Apache 2 License. Please see the [Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing) for more information.

#### Copyright

Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
