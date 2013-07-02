## Hazelcast is a clustering and highly scalable data distribution platform for Java.

With its various distributed data structures, distributed caching capabilities, elastic nature, memcache support,
integration with Spring and Hibernate and more importantly with so many happy users, Hazelcast is feature-rich,
enterprise-ready and developer-friendly in-memory data grid solution.

### Features:

* Distributed implementations of `java.util.{Queue, Set, List, Map}`
* Distributed implementation of `java.util.concurrency.locks.Lock`
* Distributed implementation of `java.util.concurrent.ExecutorService`
* Distributed `MultiMap` for one-to-many relationships
* Distributed `Topic` for publish/subscribe messaging
* Transaction support and J2EE container integration via JCA
* Socket level encryption support for secure clusters
* Synchronous (write-through) and asynchronous (write-behind) persistence
* Second level cache provider for Hibernate
* Monitoring and management of the cluster via JMX
* Dynamic HTTP session clustering
* Support for cluster info and membership events
* Dynamic discovery, scaling, partitioning with backups and fail-over

#### Hazelcast 3.0
Switch to 3.0 branch to see upcoming [Hazelcast 3.0](https://github.com/hazelcast/hazelcast/tree/3.0)

### Getting Started

See [Getting Started Guide](http://hazelcast.com/docs/latest/manual/single_html/#GettingStarted)

### Documentation

See documentation at [www.hazelcast.com](http://hazelcast.com/docs.jsp)

### Releases

Download from [www.hazelcast.com](http://hazelcast.com/downloads.jsp)

Or use Maven snippet:
````xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>${hazelcast.version}</version>
</dependency>
````

### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

#### License

Hazelcast is available under the Apache 2 License.

#### Copyright

Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
