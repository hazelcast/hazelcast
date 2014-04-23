# Introduction

## Hazelcast Overview


Hazelcast is a clustering and highly scalable data distribution platform for Java. Hazelcast helps architects and developers to easily design and develop faster, highly scalable and reliable applications for their businesses.

-   Distributed implementations of `java.util.{Queue, Set, List, Map}`

-   Distributed implementation of `java.util.concurrent.ExecutorService`

-   Distributed implementation of `java.util.concurrency.locks.Lock`

-   Distributed Topic for publish/subscribe messaging

-   Transaction support and J2EE container integration via JCA

-   Distributed listeners and events

-   Support for cluster info and membership events

-   Dynamic HTTP session clustering

-   Dynamic clustering

-   Dynamic scaling to hundreds of servers

-   Dynamic partitioning with backups

-   Dynamic fail-over

-	A very small JAR file

-	Super simple to use; include a single jar

-   Super fast; thousands of operations per sec.

-   Super efficient; very nice to CPU and RAM




Hazelcast is pure Java. JVMs that are running Hazelcast will dynamically cluster. Although by default Hazelcast will use multicast for discovery, it can also be configured to only use TCP/IP for environments where multicast is not available or preferred ([Click here for more info](#network-configuration)). Communication among cluster members is always TCP/IP with Java NIO beauty. Default configuration comes with 1 backup so if one node fails, no data will be lost. It is as simple as using `java.util.{Queue, Set, List, Map}`. Just add the `hazelcast.jar` into your classpath and start coding.


