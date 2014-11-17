

# Glossary

Term|Definition
:-|:-
**2-phase Commit**|2-phase commit protocol is an atomic commitment protocol for distributed systems. It consists of two phases: commit-request and commit. In commit-request phase, transaction manager coordinates all of the transaction resources to commit or abort. In commit-phase, transaction manager decides to finalize operation by committing or aborting according to the votes of the each transaction resource.
**ACID**|A set of properties (Atomicity, Consistency, Isolation, Durability) guaranteeing that transactions are processed reliably. Atomicity requires that each transaction be all or nothing (i.e. if one part of the transaction fails, the entire transaction will fail). Consistency ensures that only valid data following all rules and constraints is written. Isolation ensures that transactions are securely and independently processed at the same time without interference (and without transaction ordering). Durability means that once a transaction has been committed, it will remain so, no matter if there is a power loss, crash, or error.
**Cache**|A high-speed access area that can be either a reserved section of main memory or storage device. 
**Garbage Collection**|Garbage collection is the recovery of storage that is being used by an application when that application no longer needs the storage. This frees the storage for use by other applications (or processes within an application). It also ensures that an application using increasing amounts of storage does not reach its quota. Programming languages that use garbage collection are often interpreted within virtual machines like the JVM. The environment that runs the code is also responsible for garbage collection.
**Hazelcast Cluster**|It is a virtual environment formed by Hazelcast nodes communicating with each other.
**Hazelcast Node**|A node in Hazelcast terms, is a Hazelcast instance. Depending on your Hazelcast usage, it can refer to a server or a Java virtual machine (JVM). For example, if there is only one Hazelcast instance in a JVM, that JVM can be called as a node. Nodes are also referred as `cluster members` that form a cluster. 
**Hazelcast Partitions**|Memory segments containing the data. Hazelcast is built-on the partition concept, it uses partitions to store and process data. Each partition can have hundreds or thousands of data entries depending on your memory capacity. You can think of a partition as a block of data. In general and optimally, a partition should have a maximum size of 50-100 Megabytes.
**IMDG**|An in-memory data grid (IMDG) is a data structure that resides entirely in memory, and is distributed among many nodes in a single location or across multiple locations. IMDGs can support thousands of in-memory data updates per second, and they can be clustered and scaled in ways that support large quantities of data.
**Invalidation**|The process of marking an object as being invalid across the distributed cache.
**Java heap**|Java heap is the space that Java can reserve and use in memory for dynamic memory allocation. All runtime objects created by a Java application are stored in heap. By default, the heap size is 128 MB, but this limit is reached easily for business applications. Once the heap is full, new objects cannot be created and the Java application shows errors.
**LRU, LFU**|LRU and LFU are two of eviction algorithms. LRU is the abbreviation for Least Recently Used. It refers to entries eligible for eviction due to lack of interest by applications. LFU is the abbreviation for Least Frequently Used. It refers to the entries eligible for eviction due to having the lowest usage frequency.
**Multicast**|It is a type of communication where data is addressed to a group of destination nodes simultaneously.
**Near Cache**|It is a caching model. When it is enabled, an object retrieved from a remote node is put into the local cache and the future requests made to this object will be handled by this local node. For example, if you have a map with data that is mostly read, then using near cache is a good idea.
**NoSQL**|"Not Only SQL". It is a database model that provides a mechanism for storage and retrieval of data that is tailored in means other than the tabular relations used in relational databases. It is a type of database which does not adhering to the traditional relational database management system (RDMS) structure. It is not built on tables and does not employ SQL to manipulate data. It also may not provide full ACID guarantees, but still has a distributed and fault tolerant architecture.
**Race Condition**|This condition occurs when two or more threads can access shared data and they try to change it at the same time.
**Serialization**|Process of converting an object into a stream of bytes in order to store the object or transmit it to memory, a database, or a file. Its main purpose is to save the state of an object in order to be able to recreate it when needed. The reverse process is called deserialization.
**Split Brain**|Split brain syndrome, in a clustering context, is a state in which a cluster of nodes gets divided (or partitioned) into smaller clusters of nodes, each of which believes it is the only active cluster.
**Transaction**|Means a sequence of information exchange and related work (such as data store updating) that is treated as a unit for the purposes of satisfying a request and for ensuring data store integrity.




