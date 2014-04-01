
## Why Hazelcast?


As it can be seen in the above list, Hazelcast supports a number of distributed collections and features. Data can be loaded from various sources into diversity of structures, messages can be sent across a cluster, locks can be put to take measures against concurrent operations and events happening in a cluster can be listened. 


Below, Hazelcast's key properties are listed:


* It is a small JAR file. You do not need to install a software.
* It is a library, it does not impose an architecture on Hazelcast users.
* It provides out of the box distributed data structures (i.e. Map, Queue, MultiMap, Topic, Lock, Executor, etc.)
* When the size of your data to be stored and processed in memory increases, just add machines to the cluster to increase the memory and processing power.
* Data is not the only thing which is distributed, backups are distributed, too. As can be noticed, this is a big benefit when a node in the cluster is gone (e.g. crashes). Data will not be lost.
* Nodes are always aware of each other (and they communicate) unlike the traditional key-value caching solutions.
* And, it can be used as a platform to build your own distributed data structures using the Service Programming Interface (SPI), if you are not happy with the ones provided

And still evolving. Hazelcast has a dynamic open source community enabling it to be continuously developed. Since it has a very clean API that implements Java interfaces, its usage is ultra-simple especially for Java developers. These along with the above features make Hazelcast easy to use and simple to manage.

As an In-Memory Data Grid provider, Hazelcast is a perfect fit:

-	For data analysis applications requiring big data processings by partitioning the data,
-	For retaining frequently accessed data in the grid,
-	To be a primary data store for applications with utmost performance, scalability and low-latency requirements,
-	For enabling publish/subscribe communication between applications,
-	For applications to be run in distributed and scalable cloud environments,
-	To be a highly available distributed cache for applications,
-	As an alternative to Coherence, Gemfire and Terracotta.

