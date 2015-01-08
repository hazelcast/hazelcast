
## Data Partitioning

As you read in the [Sharding in Hazelcast section](#sharding-in-hazelcast), Hazelcast shards are called Partitions. Partitions are memory segments each of which can have hundreds or thousands of data entries depending on your memory capacity. 

By default, Hazelcast offers 271 partitions. Given a key, we serialize, hash and mode it with the number of partitions to find the partition the key belongs to. The partitions themselves are distributed equally among the members of the cluster. Hazelcast also creates the backups of partitions and distributes them among nodes for redundancy.

Hazelcast distributes data entries into the partitions using a hashing algorithm. The key of data is put through this algorithm and the resulted value is mod by 271. After the mod operation, the value we have addresses the place of the data in a partition. Each node in a cluster will know where the key is. 

Partitions in a 1 node Hazelcast cluster.

![](images/NodePartition.jpg)

Partitions in a 2 node cluster. 

![](images/BackupPartitions.jpg)

The blacks are primary partitions and reds are backups. In the above illustration, the first node has 135 primary partitions (black) and each of these partitions are backed up in the second node (red). At the same time, the first node has the backup partitions of the second node's primary partitions.

As you add more nodes, Hazelcast will move one by one some of the primary and backup partitions to new nodes, making all nodes equal and redundant. Only the minimum amount of partitions will be moved to scale out Hazelcast.

![](images/4NodeCluster.jpg)


