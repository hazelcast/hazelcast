---
title: In-Memory Storage
description: How Jet makes use of Hazelcast's in-memory storage
id: version-4.0-in-memory-storage
original_id: in-memory-storage
---

A distinctive feature of Hazelcast Jet is that it has no dependency on
disk storage, it keeps all of its operational state in the RAM of the
cluster. Here are some details on how that storage works.

## Data is Partitioned and Replicated

Hazelcast Jet divides the key space into partitions (aka. shards) and
maps each key to a single partition. By default, there are 271
partitions. For each partition, Hazelcast creates multiple replicas, and
assigns one replica as primary and other replicas as backups.

We'll walk through a scenario with 3 Jet nodes and 12 partitions, as
shown in Figure 1. Primaries are blue, backups red. For instance, the
1st node keeps primaries for partitions 1, 4, 7, 10 and backups for
partitions 2, 3, 5, 6. On the top, in yellow, there is a single Jet
processor (local parallelism = 1) that does a group-and-aggregate
operation.

![Figure 1](assets/ram-storage-1.png)

Figure 1. Processors, primaries and backups

The DAG edge that delivers the data to this processor is of the
*partitioned-distributed* kind. For each item Jet first extracts its
key, computes the key's partition, looks up the processor in charge of
that partition, and sends it there. Every processor maintains its own
internal state.

## Snapshots are Stored in IMap

When a processor receives a snapshot barrier from its input streams, it
saves its internal state to the Hazelcast IMap which is created for the
current snapshot. Figure 2 shows how state objects of the first
processor instance end up in the local node's primaries, thanks to the
same partitioning scheme used for both processing and storage.
Additionally, backup copies of these state objects are sent to the 2nd
and the 3rd node.

![Figure 2](assets/ram-storage-2.png)

Figure 2. Processor state replicated to remote members

## Data Spreads to a Newly Added Node

Let’s see how we make use of the partition replicas to recover processor
state after a change in the cluster topology. In Figure 3 we added a new
node to the Jet cluster. Hazelcast Jet rebalances the partitions and
assigns some partition replicas to the new node. It uses the consistent
hashing algorithm to move a minimum amount of data between the nodes
while rebalancing. In our scenario, the new node receives one primary
and one backup from each existing Hazelcast Jet node. For instance, from
the 1st node it receives the primary of partition 1 and backup of
partition 2. After rebalancing the job is restarted and processor states
are initialized from local replicas of the snapshot.

![Figure 3](assets/ram-storage-3.png)

Figure 3. Fourth node added: partitions spread out

## Data Recovered from Backups when a Node Fails

Finally, Figure 4 shows how Hazelcast Jet recovers from the failure of
1st node. Before the failure, 2nd and 3rd nodes were keeping the backup
replicas for the partitions assigned to the 1st node. After the failure,
the 2nd node promotes partitions 1 and 4 from backup to primary.
Similarly, the 3rd node promotes partitions 7 and 10. After promotion
these partitions lack a backup so the nodes create new backup replicas
for each other, as shown with the bold-dashed green boxes. After this
point, processor states are restored from the local primary partition
replicas.

![Figure 4](assets/ram-storage-4.png)

Figure 4. One node lost: backups promoted to primary, new backups made
