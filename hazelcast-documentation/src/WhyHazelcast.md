
## Why Hazelcast?



**A Glance at Traditional Data Persistence**

Data is the essence in software systems and in conventional architectures, relational database persists and provides access to data. Basically, applications are talking directly with a database which has its backup as another machine. To increase the performance capabilities in a conventional architecture, a faster machine is required or utilization of the current resources should be tuned. This leads to a large amount of money or manpower.

Then, there is the idea of keeping copies of data next to the database. This is performed using technologies like external key-value storages or second level caching. Purpose is to protect the database from excessive loads. However, when the database is saturated or if the applications perform mostly "put" operations, this approach is of no use, since it insulates the database only from the "get" loads. Even if the applications heavily perform "get"s, then there appears a consistency issue: when data is changed within the database, what is the reaction of local data cache, how these changes are handled? This is the point where concepts like time-to-live (TTL) or write-through come as solutions. 

However, for example in the case of caches having entries with TTL; if the frequency of access to an entry is less than TTL, again there is no use. On the other hand, in the case of write through caches; if there are more than one of these caches in a cluster, then we have again consistency issues between those. This can be avoided by having the nodes communicating with each other so that entry invalidations can be propagated.

We can conclude that an ideal cache would combine TTL and write through features. And, there are several cache servers and in-memory database solutions in this field. However, those are stand-alone single instances with a distribution mechanism to an extent provided by other technologies. This brings us back to square one: we would experience saturation or capacity issues if the product is a single instance or if consistency is not provided by the distribution. 

**And, there is Hazelcast**

Hazelcast, a brand new approach to data, is designed around the concept of distribution. Data is shared around the cluster for flexibility and performance. It is an in-memory data grid for clustering and highly scalable data distribution.

One of the main features of Hazelcast is not having a master node. Each node in the cluster is configured to be the same in terms of functionality. The oldest node manages the cluster members, i.e. automatically performs the data assignment to nodes. When a new node joins to the cluster or a node goes down, this data assigment is repeated across the nodes and the data distribution comes to a balance again. Therefore, getting Hazelcast up and running is  simple as the nodes are discovered and clustered automatically at no time.

Another main feature is the data being persisted entirely in-memory. This is  fast. In the case of a failure, such as a node crash, no data will be lost since Hazelcast keeps copies of data across all the nodes of cluster. Data is kept in partition slices and each partition slice is owned by a node and backed up on another node. Please see the illustration below.

![](images/WhyHazelcast.jpg)



As it can be seen in the feature list given in [Hazelcast Overview](#hazelcast-overview) section, Hazelcast supports a number of distributed collections and features. Data can be loaded from various sources into diversity of structures, messages can be sent across a cluster, locks can be put to take measures against concurrent operations and events happening in a cluster can be listened. 

**Hazelcast's Distinctive Strengths**


* It is open source.
* It is a small JAR file. You do not need to install a software.
* It is a library, it does not impose an architecture on Hazelcast users.
* It provides out of the box distributed data structures (i.e. Map, Queue, MultiMap, Topic, Lock, Executor, etc.).
* There is no "master" in Hazelcast cluster; each node in the cluster is configured to be functionally the same.
* When the size of your data to be stored and processed in memory increases, just add nodes to the cluster to increase the memory and processing power.
* Data is not the only thing which is distributed, backups are distributed, too. As can be noticed, this is a big benefit when a node in the cluster is gone (e.g. crashes). Data will not be lost.
* Nodes are always aware of each other (and they communicate) unlike the traditional key-value caching solutions.
* And, it can be used as a platform to build your own distributed data structures using the Service Programming Interface (SPI), if you are not happy with the ones provided.

And still evolving. Hazelcast has a dynamic open source community enabling it to be continuously developed. Since it has a very clean API that implements Java interfaces, its usage is simple especially for Java developers. These along with the above features make Hazelcast easy to use and simple to manage.

As an in-memory data grid provider, Hazelcast is a perfect fit:

-	For data analysis applications requiring big data processings by partitioning the data,
-	For retaining frequently accessed data in the grid,
-	To be a primary data store for applications with utmost performance, scalability and low-latency requirements,
-	For enabling publish/subscribe communication between applications,
-	For applications to be run in distributed and scalable cloud environments,
-	To be a highly available distributed cache for applications,
-	As an alternative to Coherence, Gemfire and Terracotta.

