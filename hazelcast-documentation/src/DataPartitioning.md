
## Data Partitioning

As you read in the [Sharding in Hazelcast section](#sharding-in-hazelcast), Hazelcast shards are called Partitions. Partitions are memory segments each of which can contain hundreds or thousands of data entries depending on your memory capacity. 

By default, Hazelcast offers 271 partitions. When you start a node, these 271 partitions will be owned by that node. The following is an illustration of the partitions in a 1 node Hazelcast cluster.

![](images/NodePartition.jpg)

When you start a second node, i.e. there will be a 2 node Hazelcast cluster, the partitions are distributed as shown in the following illustration. 

![](images/BackupPartitions.jpg)

The blacks are primary partitions and blue ones are replicas (backups). In the above illustration, the first node has 135 primary partitions (black) and each of these partitions are backed up in the second node (blue). At the same time, the first node has the replica partitions of the second node's primary partitions.

As you add more nodes, Hazelcast moves one by one some of the primary and replica partitions to the new nodes, making all nodes equal and redundant. Only the minimum amount of partitions will be moved to scale out Hazelcast. The following is an illustration of the partition distributions in a 4 node Hazelcast cluster.

![](images/4NodeCluster.jpg)

As you see, the partitions themselves are distributed equally among the members of the cluster. Hazelcast creates the backups of partitions and distributes them among nodes for redundancy.

### How the Data is Partitioned

Hazelcast distributes data entries into the partitions using a hashing algorithm. Given an object key (e.g. for map) or an object name (e.g. for topic or list):

- the key or name is serialized, i.e. converted into byte array,
- this byte array is hashed, and
- the result of the hash is mod by the number of partitions.

The result of this modulo - *MOD(hash result, partition count)* -  gives the partition in which the data will be stored. 

### Partition Table

When you start a node, a partition table is created within it. This table stores the information of which partitions belong to which nodes. Purpose of this table is to make all nodes in the cluster aware of this information. As a result, each node knows where the data is.

The oldest node in the cluster (the one which was started first) sends the partition table to all nodes periodically. By this way, each node in the cluster is informed about the partition ownership changes, if any. The ownerships may be changed when, for example, a new node joins to the cluster, or when a node leaves.

![image](images/NoteSmall.jpg) ***NOTE:*** *If the oldest node goes down, the next oldest node starts to send the partition table information to the other nodes.*

You can configure the partition table sending frequency using the `hazelcast.partition.table.send.interval` system property. It is set to 15 seconds by default. 

### Repartitioning

Repartitioning is the process of redistribution of partition ownerships. Hazelcast performs the repartitioning in the following cases:

- When a node joins to the cluster.
- When a node leaves the cluster.

In these cases, the partition table in the oldest node is updated with the new partition ownerships. 


