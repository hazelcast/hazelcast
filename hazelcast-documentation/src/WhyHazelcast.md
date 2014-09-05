
## Why Hazelcast?



**A Glance at Traditional Data Persistence**

Data is at the core of software systems and in conventional architectures, relational database persists and provides access to data. Basically, applications are talking directly with a database which has its backup as another machine. To increase the performance, tuning or a faster machine is required. This leads to a large amount of money or effort.

Then, there is the idea of keeping copies of data next to the database. This is performed using technologies like external key-value stores or second level caching. This helps to offload the database. However, when the database is saturated or if the applications perform mostly "put" operations (writes), this approach is of no use, since it insulates the database only from the "get" loads (reads). Even if the applications is read intensive, then there can be consistency problems: when data changes, what happens to the cache, and how are the changes handled? This is when concepts like time-to-live (TTL) or write-through come in.

However, in the case of TTL, if the access is less frequent then the TTL, the result will always be a cache miss. On the other hand, in the case of write-through caches; if there are more than one of these caches in a cluster, then we again have consistency issues. This can be avoided by having the nodes communicating with each other so that entry invalidations can be propagated.

We can conclude that an ideal cache would combine TTL and write-through features. And, there are several cache servers and in-memory database solutions in this field. However, those are stand-alone single instances with a distribution mechanism to an extent provided by other technologies. This brings us back to square one: we would experience saturation or capacity issues if the product is a single instance or if consistency is not provided by the distribution.

**And, there is Hazelcast**

Hazelcast, a brand new approach to data, is designed around the concept of distribution. Data is shared around the cluster for flexibility and performance. It is an in-memory data grid for clustering and highly scalable data distribution.

One of the main features of Hazelcast is not having a master node. Each node in the cluster is configured to be the same in terms of functionality. The oldest node manages the cluster members, i.e. automatically performs the data assignment to nodes.

Another main feature is the data being held entirely in-memory. This is fast. In the case of a failure, such as a node crash, no data will be lost since Hazelcast distributes copies of data across all the nodes of cluster.

As it can be seen in the feature list given in [Hazelcast Overview](#hazelcast-overview) section, Hazelcast supports a number of distributed data structures and distributed computing utilities. This provides powerful ways of accessing distributed clustered memory, but also CPUs for true distributed computing. 

**Hazelcast's Distinctive Strengths**


* It is open source.
* It is a small JAR file. You do not need to install software.
* It is a library, it does not impose an architecture on Hazelcast users.
* It provides out of the box distributed data structures (i.e. Map, Queue, MultiMap, Topic, Lock, Executor, etc.).
* There is no "master", so no single point of failure in Hazelcast cluster; each node in the cluster is configured to be functionally the same.
* When the size of your memory and compute requirement increases, new nodes can be dynamically joined to the cluster to scale elastically.
* Data is resilient to node failure. Data backups are also distributed across the cluster. As can be noticed, this is a big benefit when a node in the cluster is gone (e.g. crashes). Data will not be lost.
* Nodes are always aware of each other (and they communicate) unlike traditional key-value caching solutions.
* You can build your own custom distributed data structures using the Service Programming Interface (SPI), if you are not happy with the ones provided.

Finally, Hazelcast has a vibrant open source community enabling it to be continuously developed.

Hazelcast is a fit when you need:

-	Analytic applications requiring big data processing by partitioning the data,
-	Retaining frequently accessed data in the grid,
-	A cache, particularly an open source JCache provider with elastic distributed scalability,
-	A primary data store for applications with utmost performance, scalability and low-latency requirements,
-	An In-Memory NoSQL Key Value Store,
-	Publish/subscribe communication at highest speed and scalability between applications,
-	Applications that need to scale elastically in distributed and cloud environments,
-	A highly available distributed cache for applications,
-	As an alternative to Coherence, Gemfire and Terracotta.

