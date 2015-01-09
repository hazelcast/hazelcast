
## Data Partitioning

As you read in the [Sharding in Hazelcast section](#sharding-in-hazelcast), Hazelcast shards are called Partitions. Partitions are memory segments each of which can contain hundreds or thousands of data entries depending on your memory capacity. 

By default, Hazelcast offers 271 partitions. When you start a node, these 271 partitions will be owned by that node. The following is an illustration of the partitions in a 1 node Hazelcast cluster.

![](images/NodePartition.jpg)

When you start a second node, i.e. there will be 2 node Hazelcast cluster, the partitions will be distributed as shown in the following illlustration. 

![](images/BackupPartitions.jpg)

The blacks are primary partitions and reds are replicas (backups). In the above illustration, the first node has 135 primary partitions (black) and each of these partitions are backed up in the second node (red). At the same time, the first node has the replica partitions of the second node's primary partitions.

As you add more nodes, Hazelcast moves one by one some of the primary and replica partitions to the new nodes, making all nodes equal and redundant. Only the minimum amount of partitions will be moved to scale out Hazelcast. The following is an illustration of the partition distributions in a 4 node Hazelcast cluster.

![](images/4NodeCluster.jpg)

As you see, the partitions themselves are distributed equally among the members of the cluster. Hazelcast creates the backups of partitions and distributes them among nodes for redundancy, too.

Hazelcast distributes data entries into the partitions using a hashing algorithm. Given an object key (e.g. for map) or an object name (e.g. for topic or list):

- the key or name is serialized, i.e. converted into byte array,
- this byte array is hashed, and
- the result of the hash is mod by the number of partitions.

The result of this modulo - *MOD(hash result, partition count)* -  gives the partition in which the data will be stored.

Each node in a cluster will know where the key is. 