## Hazelcast is a clustering and highly scalable data distribution platform for Java.

With its various distributed data structures, distributed caching capabilities, elastic nature, memcache support,
integration with Spring and Hibernate and more importantly with so many happy users, Hazelcast is feature-rich,
enterprise-ready and developer-friendly in-memory data grid solution.

### Features:

* Distributed implementations of `java.util.{Queue, Set, List, Map}`
* Distributed implementation of `java.util.concurrent.locks.Lock`
* Distributed implementation of `java.util.concurrent.ExecutorService`
* Distributed `MultiMap` for one-to-many relationships
* Distributed `Topic` for publish/subscribe messaging
* Synchronous (write-through) and asynchronous (write-behind) persistence
* Transaction support
* Socket level encryption support for secure clusters
* Second level cache provider for Hibernate
* Monitoring and management of the cluster via JMX
* Dynamic HTTP session clustering
* Support for cluster info and membership events
* Dynamic discovery, scaling, partitioning with backups and fail-over

### Getting Started

See [Getting Started Guide](http://hazelcast.org/docs/latest/manual/html/gettingstarted.html)

### Documentation

See documentation at [www.hazelcast.org](http://hazelcast.org/documentation/)

### Code Samples

See [Hazelcast Code Samples](https://github.com/hazelcast/hazelcast-code-samples)

### Releases

Download from [www.hazelcast.org](http://hazelcast.org/download/)

Or use Maven snippet:
````xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>${hazelcast.version}</version>
</dependency>
````

### Snapshot Releases

Maven snippet:
````xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>${hazelcast.version}</version>
</dependency>
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
````


### Building From Source

Pull latest from repo `git pull origin master` and use Maven install (or package) to build `mvn clean install`.

### Testing

Hazelcast has 3 testing profiles:

* **Default**: Type `mvn test` to run quick/integration tests (those can be run in-parallel without using network).
* **Slow Tests**: Type `mvn test -P slow-test` to run tests those are either slow or cannot be run in-parallel.
* **All Tests**: Type `mvn test -P all-tests` to run all test serially using network.

### Contributing to Hazelcast

We encourage pull requests and process them promptly.

To contribute:

* see [Developing with Git] (https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process
* complete the [Hazelcast Contributor Agreement] (https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)

For an enhancement or larger feature, create a GitHub issue first to discuss.


### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

#### License

Hazelcast is available under the Apache 2 License.

#### Copyright

Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
