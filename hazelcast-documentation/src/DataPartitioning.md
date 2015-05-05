
## Data Partitioning

As you read in the [Sharding in Hazelcast section](#sharding-in-hazelcast), Hazelcast shards are called Partitions. Partitions are memory segments, where each of those segments can contain hundreds or thousands of data entries, depending on the memory capacity of your system. 

By default, Hazelcast offers 271 partitions. When you start a node, that nose owns those 271 partitions. The following illustration shows the partitions in a single node Hazelcast cluster.

![](images/NodePartition.jpg)

When you start a second node on that cluster (creating a 2-node Hazelcast cluster), the partitions are distributed as shown in the following illustration. 

![](images/BackupPartitions.jpg)

In the illustration, the partitions with black text are primary partitions, and the partitions with blue text are replica partitions (backups). The first node has 135 primary partitions (black), and each of these partitions are backed up in the second node (blue). At the same time, the first node also has the replica partitions of the second node's primary partitions.

As you add more nodes, Hazelcast one-by-one moves some of the primary and replica partitions to the new nodes, making all nodes equal and redundant. Only the minimum amount of partitions will be moved to scale out Hazelcast. The following is an illustration of the partition distributions in a 4-node Hazelcast cluster.

![](images/4NodeCluster.jpg)

Hazelcast distributes the partitions equally among the members of the cluster. Hazelcast creates the backups of partitions and distributes them among nodes for redundancy.

### How the Data is Partitioned

Hazelcast distributes data entries into the partitions using a hashing algorithm. Given an object key (for example, for a map) or an object name (for example, for a topic or list):

- the key or name is serialized (converted into a byte array),
- this byte array is hashed, and
- the result of the hash is mod by the number of partitions.

The result of this modulo - *MOD(hash result, partition count)* -  gives the partition in which the data will be stored. 

### Partition Table

When you start a node, a partition table is created within it. This table stores the information for which partitions belong to which nodes. The purpose of this table is to make all nodes in the cluster aware of this information, making sure that each node knows where the data is.

The oldest node in the cluster (the one that started first) periodically sends the partition table to all nodes. In this way, each node in the cluster is informed about any changes to the partition ownership. The ownerships may be changed when, for example, a new node joins the cluster, or when a node leaves the cluster.

![image](images/NoteSmall.jpg) ***NOTE:*** *If the oldest node goes down, the next oldest node sends the partition table information to the other nodes.*

You can configure the frequency (how often) that the node sends the partition table the information by using the `hazelcast.partition.table.send.interval` system property. The property is set to every 15 seconds by default. 

### Repartitioning

Repartitioning is the process of redistribution of partition ownerships. Hazelcast performs the repartitioning in the following cases:

- When a node joins to the cluster.
- When a node leaves the cluster.

In these cases, the partition table in the oldest node is updated with the new partition ownerships. 


