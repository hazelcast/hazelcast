# Graceful Shutdown Improvements

|ℹ️ Since: 3.7| 
|-------------|

## Background

### Description

Current shutdown design lacks of data safety guarantee if there are
multiple nodes shutting down concurrently in the cluster. When a node
wants to shut down, it only checks if 1st backups of its partitions are
safe or not. If there are 2 nodes that contain backups of each other, we
might lose data if both of them decides to shutdown at the same time. 

We have another inherent outcome of the current design: graceful
shutdown loses data configured with 0 backup. 

We can build a race-free graceful shutdown process on top of the new
migration system developed in [Avoid Data Loss on Migration Design](01-avoid-data-loss-on-migration.md) work.
Basic principle of the solution is as follows:

-   A graceful shutdown can be done safely when the node which wants to
    shutdown does not keep any data. 

So, if a node wants to shutdown, we can safely migrate all of its
replica ownerships (both partition ownerships and backup ownerships) to
any other progressing node before shutting down the node completely.
 This solution enables us to:

-   shutdown any number of in the cluster concurrently, without losing
    any data,

-   continue to keep user data configured with 0 backup after graceful
    shutdowns.

## User Interaction

### API design and/or Prototypes

We have the following methods in `PartitionService` interface in the
public API:

```java
/**
 * Checks whether the cluster is in a safe state. When in a safe state,
 * it is permissible to shut down a server instance.
 *
 * @return true if there are no partitions being migrated, and there are sufficient backups
 * for each partition per the configuration; false otherwise.
 * @since 3.3
 */
boolean isClusterSafe();

/**
 * Check if the given member is safe to shutdown, meaning check if at least one backup of the partitions
 * owned by the given member are in sync with primary.
 *
 * @param member the cluster member to query.
 * @return true if the member is in a safe state, false otherwise.
 * @since 3.3
 */
boolean isMemberSafe(Member member);

/**
 * Check if the local member is safe to shutdown, meaning check if at least one backup of the partitions
 * owned by the local member are in sync with primary.
 *
 * @since 3.3
 */
boolean isLocalMemberSafe();
```

Originally, these methods checks a member is able to perform a safe
shutdown or not. With the proposed solution, these methods are not
necessary anymore since shutdown calls can be made anytime and master
will be able to shutdown the nodes after taking necessary steps.
Therefore, we changed the behaviour of these methods as follows:

```java
/**
 * Checks whether the cluster is in a safe state.
 * Safe state means; there are no partitions being migrated and all backups are in sync
 * when this method is called.
 *
 * @return true if there are no partitions being migrated and all backups are in sync.
 *
 * @since 3.3
 */
boolean isClusterSafe();

/**
 * Checks whether the given member is in safe state.
 * Safe state means; all backups of partitions currently owned by the member are in sync when this method is called.
 *
 * @param member the cluster member to query.
 * @return true if the member is in a safe state, false otherwise.
 * @since 3.3
 */
boolean isMemberSafe(Member member);

/**
 * Checks whether local member is in safe state.
 * Safe state means; all backups of partitions currently owned by local member are in sync when this method is called.
 *
 * @since 3.3
 */
boolean isLocalMemberSafe();
```

Now, they check if all replicas of the partitions owned by a particular
node are sync or not. Please be aware that these methods are still racy.
State of the cluster may be already changed just before the return value
is given back to the user.  

## Technical Design

Here is the proposed solution for the new graceful shutdown process:

-   Once a node initiates the shutdown process, it notifies the master
    by sending a fire & forget operation to master and waits for master
    to notify it back. While waiting, it retries the same operation on
    master since the shutdown mechanism is idempotent on master and the
    retry speeds up the process if master crashes when there are some
    nodes trying to shut down. 

-   Master node maintains a state, a set, for the nodes that has
    requested shutdown. Once it receives a shutdown request, it adds
    address of the node into this set. Address of this node will be
    removed from the set when the node actually leaves the cluster. Once
    an address is added into this set, master node triggers the
    migration system. 

-   Migration system has the following modifications to handle the
    shutdown requests:

    -   `RedoPartitioningTask` removes shutdown-requested nodes from the
        member groups that is provided to the repartitioning algorithm
        and a new partition table is produced with the given limited
        member groups. Then, migration decision algorithm schedules a
        few migrations that will move the initial partition table to the
        targeted partition table. These migrations have the following
        property: shutdown-requested nodes can be only source of these
        migrations. So once all of these migrations are completed,
        partition table will not contain any node that requested to shut
        down. To verify this, RedoPartitioningTask schedules a new task,
        `ProcessShutdownRequestsTask`,  at the end of the queue after
        the migrations.

    -   `ProcessShutdownRequestsTask` checks if a shutdown-requested
        node is still present in the partition table. If not, it means
        that all of the replicas owned by the corresponding node have
        been transferred to other nodes so it can shutdown safely. So
        master notifies the node to continue the shutdown process. 

  

This approach is a truly graceful shutdown solution with the following
benefits:

-   It makes the shutdown process race-free. So any number of nodes can
    shutdown concurrently without loosing any data, including the data
    configured with 0 backup. Relatedly, it is a resume-able process,
    just like any other mechanism in the migration system. So if the
    master crashes while some nodes are shutting down, new node will
    continue the process safely. 

-   It doesn't decrease available partition replica counts more than
    necessarily. For instance, if an initial cluster has 4 nodes
    initially and 2 of them decide to shutdown, all of the partitions
    will have 2 replicas after the shutdown is completed.

-   It orders the ongoing partition operations and the shutdown process.
    Since partition migrations are performed within partition threads,
    there will be no partition operation interrupted in the middle of
    its execution. Any migration will start after the ongoing operation
    is completed on the partition and the next operation will be
    executed on the new owner of the partition. 

## Testing Criteria

We are testing the solution for the following cases:

-   Shutting down the whole cluster

-   Shutting down some nodes

-   Shutting down some nodes and restarting them back  

-   Shutting down some nodes and starting new nodes 

-   Shutting down some nodes and terminating some others

-   Shutting down master 

-   Terminating master when there are other nodes to shutdown 

-   Verifying no partition lost events are fired when multiple members
    shutdown gracefully

For all of these scenarios, partition versions, partition data and
partition assignments are verified.  

  

  

  

  
