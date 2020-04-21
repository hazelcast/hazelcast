# Hazelcast

[![Gitter](https://img.shields.io/gitter/room/gitterHQ/gitter.svg)](https://gitter.im/hazelcast/hazelcast?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) 
[![GitHub](https://img.shields.io/github/license/hazelcast/Hazelcast.svg)](https://github.com/hazelcast/Hazelcast/blob/master/LICENSE)
[![javadoc](https://javadoc.io/badge2/com.hazelcast/hazelcast/4.0/javadoc.svg)](https://javadoc.io/doc/com.hazelcast/hazelcast/4.0)
[![Docker pulls](https://img.shields.io/docker/pulls/hazelcast/hazelcast)](https://img.shields.io/docker/pulls/hazelcast/hazelcast)
[![Total Alerts](https://img.shields.io/lgtm/alerts/g/hazelcast/hazelcast.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/hazelcast/hazelcast/alerts)
[![Code Quality: Java](https://img.shields.io/lgtm/grade/java/g/hazelcast/hazelcast.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/hazelcast/hazelcast/context:java)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=hz-os-master&metric=alert_status)](https://sonarcloud.io/dashboard?id=hz-os-master)

----

Hazelcast is an open-source distributed in-memory data store and
computation platform. It provides a wide variety of distributed data
structures and concurrency primitives, including:

* a distributed, partitioned and queryable in-memory key-value store
  implementation, called `IMap`
* additional data structures and simple messaging constructs such as
  `Set`, `MultiMap`, `Queue`, `Topic`
* cluster-wide unique ID generator, called `FlakeIdGenerator`
* a distributed, [CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
  based counter, called `PNCounter`
* a cardinality estimator based on [`HyperLogLog`](https://en.wikipedia.org/wiki/HyperLogLog).

Additionally, Hazelcast includes a production-ready
[Raft](https://en.wikipedia.org/wiki/Raft_(computer_science))
implementation which allows implementation of _linearizable_ constructs
such as:

* a distributed and reentrant lock implementation, called `FencedLock`
* primitives for distributed computing such as `AtomicLong`,
`AtomicReference` and `CountDownLatch`.

Hazelcast data structures are in-memory, highly optimized and offer very
low latencies. For a single `get` or `put` operation on an `IMap`, you
can typically expect a round-trip-time of under _100 microseconds_.

It's very simple to form a cluster with Hazelcast, you can easily do it
on your computer by just starting several instances. The instances will
discover each other and form a cluster. There aren't any dependencies on
any external systems.

Hazelcast automatically replicates data across the cluster and you are
able to seamlessly tolerate failures and add additional capacity to
the cluster when needed.

Hazelcast comes with clients in the following programming languages:

* [Java](https://github.com/hazelcast/hazelcast)
* [.NET](https://github.com/hazelcast/hazelcast-csharp-client)
* [Python](https://github.com/hazelcast/hazelcast-python-client)
* [C++](https://github.com/hazelcast/hazelcast-cpp-client)
* [Node.js](https://github.com/hazelcast/hazelcast-nodejs-client)
* [Go](https://github.com/hazelcast/hazelcast-go-client)

Hazelcast also has first-class support for running on different
cloud providers such as [AWS](https://github.com/hazelcast/hazelcast-aws),
[GCP](https://github.com/hazelcast/hazelcast-gcp) 
and [Azure](https://github.com/hazelcast/hazelcast-azure)
as well as on [Kubernetes](https://github.com/hazelcast/hazelcast-kubernetes).

## Download

You can download Hazelcast from
[hazelcast.org](http://hazelcast.org/download/). Once you have
downloaded, you can start the Hazelcast instance using the script
`bin/start.sh`.

## Get Started

Hazelcast allows you to interact with a cluster using a simple API, for
example you can use the Hazelcast Java Client to connect to a running
cluster and perform operations on it:

```java
HazelcastInstance hz = HazelcastClient.newHazelcastClient();
IMap<String, String> map = hz.getMap("my-distributed-map");
map.put("key", "value");
String current = map.get("key");
map.putIfAbsent("somekey", "somevalue");
map.replace("key", "value", "newvalue");
```

You only need to add a single JAR as a dependency:

````xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>${hazelcast.version}</version>
</dependency>
````

For more information, see the [Getting Started Guide](https://hazelcast.org/imdg/get-started/)

## Documentation

See the [reference
manual](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html)
for in-depth documentation about Hazelcast features.

## Code Samples

See [Hazelcast Code Samples](https://github.com/hazelcast/hazelcast-code-samples)

## Hazelcast Jet

[Hazelcast Jet](https://jet-start.sh) is a distributed batch and stream
processing framework based on Hazelcast. It can be used to import/export
data from/to Hazelcast using a very wide variety of data sources
including Hadoop, S3, Apache Kafka, Elasticsearch, JDBC and JMS.

## Get Help

You can use the following channels for getting help with Hazelcast:

* [Hazelcast mailing list](http://groups.google.com/group/hazelcast)
* [Gitter](https://gitter.im/hazelcast/hazelcast) for chatting with the
  development team and other Hazelcast users.
* [Stack Overflow](https://stackoverflow.com/tags/hazelcast)

## Contributing

We encourage Pull Requests and process them promptly.

To contribute:

* see [Developing with Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process
* complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)

For an enhancement or larger feature, create a GitHub issue first to
discuss.

### Using Snapshot Releases

Maven snippet:

```xml
<repository>
    <id>sonatype-snapshots</id>
    <name>Sonatype Snapshot Repository</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <releases>
        <enabled>false</enabled>
    </releases>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>${hazelcast.version}</version>
</dependency>
```

### Building From Source

Pull latest from repo `git pull origin master` and use Maven install (or
package) to build `mvn clean install`.

### Testing

Hazelcast has 3 testing profiles:

* **Default**: Type `mvn test` to run quick/integration tests (those can
  be run in parallel without using network).
* **Slow Tests**: Type `mvn test -P slow-test` to run tests that are
  either slow or cannot be run in parallel.
* **All Tests**: Type `mvn test -P all-tests` to run all tests serially
  using network.

### Checkstyle and SpotBugs

Hazelcast uses static code analysis tools to check if a Pull Request is
ready for merge. Run the following commands locally to check if your
contribution is Checkstyle and SpotBugs compatible.

```bash
mvn clean validate -P checkstyle
```

```bash
mvn clean compile -P spotbugs
```

## License

Hazelcast is available under the Apache 2 License. Please see the
[Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing)
for more information.

## Acknowledgments
[![](https://www.yourkit.com/images/yklogo.png)](http://www.yourkit.com/)

Thanks to [YourKit](http://www.yourkit.com/) for supporting open source software by providing us a free license 
for their Java profiler  

## Copyright

Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
