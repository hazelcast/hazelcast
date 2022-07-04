# Gigantic Cache Migration Enhancements

|ℹ️ Since: 3.9| 
|-------------|

## Background

### Description

The migration and replication mechanisms, currently, choose safety over
availability when cluster membership changes (new member joins or an
existing member leaves) to prevent data loss. But in cases where high
availability and/or performance are more important than safety, that
safety guarantee becomes a show stopper. 

To be able to increase availability when cluster membership changes,
we'll introduce three features:

1.  Manual Control of Partition Migration: A new cluster state to
    manually postpone rebalancing and replications until cluster becomes
    ACTIVE

2.  Ability to promote a lite member to a data (full) member

3.  A new opt-in migration mechanism to allow migrating partition data
    in many fragments instead of single large block  

## Functional Design

### Summary of Functionality

Two new user facing features are introduced. 

1.  A new cluster state, `NO_MIGRATION`, is introduced, which stands
    between `ACTIVE` and `FROZEN` cluster states. Cluster can be taken
    to `NO_MIGRATION` state via `Cluster` API, `cluster.sh` script or
    Management Center.

2.  Ability to promote a lite member to a data member is introduced. A
    lite member can be promoted via `Cluster.promoteLocalMember()` api
    or Management Center. 

## User Interaction

### API design and/or Prototypes

-   A new enum `NO_MIGRATION` is added to `ClusterState`.
    Existing `Cluster.changeState(newState)`
    and `Cluster.getClusterState()` methods are operational
    for `NO_MIGRATION` too. 

-   `promoteLocalMember()` method is added to `Cluster` interface, which
    promotes local member to a data member when invoked on a lite
    member.   

-   `hazelcast.partition.migration.fragments.enabled` property is
    introduced to enable/disable fragmented migrations.

## Technical Design

### Manual Control of Partition Migration: `NO_MIGRATION` Cluster State

Existing cluster states tends to increase or at least keep the safety of
partition data. They trade availability off against losing data. In
general functionality, there are two permissions/restrictions provided
by cluster states;

-   Allowing/forbidding new member joins

-   Allowing/forbidding migrations (rebalancing) and/or replications
    (replicating missing backups)

From this aspect, 

-   `ACTIVE` state always allows both new member joins and migrations.

-   `FROZEN` and `PASSIVE` states forbids both new member joins and
    migrations. Their only difference is, `PASSIVE` state restricts
    types of operations allowed to run.

To provide more availability when a new member joins or an existing
member leaves the cluster, we need a new cluster state that allows new
members to join but forbids migrations (rebalancing)  and
replications (replicating missing backups). 

Two new attributes, `joinAllowed`  and `migrationAllowed,` are
added to `ClusterState` enum. In join and migration mechanisms, instead
of checking cluster state names directly, we use `joinAllowed` 
and `migrationAllowed` attributes of cluster state to decide whether
process join requests or perform migrations/replications. That way, new
cluster states with attributes become;

-   `ACTIVE       → joinAllowed: true,  migrationAllowed: true`

-   `NO_MIGRATION → joinAllowed: true,  migrationAllowed: false`

-   `FROZEN       → joinAllowed: false, migrationAllowed: false`

-   `PASSIVE      → joinAllowed: false, migrationAllowed: false`

### Lite Member to Data Member Promotion 

Although changing cluster state to `NO_MIGRATION` to provide more
availability when new member joins is an option, one downside is it
affects all of the cluster, it's an all or nothing option. With
`NO_MIGRATION`, both rebalancing and replication of missing backups
become disabled. A finer grained option for new member joins is to
prevent migrations/rebalancing only to newly joining members but to keep
existing cluster members intact. That way, missing backups can still be
replicated but rebalancing will be disabled, which is a middle ground
between safety and availability. 

In this setup, new members are configured as lite members, they don't
own any partitions when they join the cluster. So, there'll be no data
movement to these members. When later conditions become suitable, when
for example cluster load/traffic becomes low, user can manually promote
these lite members to data members via the API or Management Center.
When promoted, they will be assigned some portion of partitions and data
migrations will be triggered to those new data members. 

#### Lite Member Promotion Algorithm

`promoteLocalMember()` is added to `Cluster` interface, which can be
invoked to promote only local member. When this method is called;

-   If cluster version is less than 3.9 then method will fail with
    an `UnsupportedOperationException.`

-   If local member is already a data member then method will fail with
    an `IllegalStateException.`

-   Otherwise, a promotion request will be sent to known master member.

-   If target member itself is not master or mastership claim process is
    still in progress then request will fail with
    an `IllegalStateException`.

-   If target member doesn't identify requester as a cluster member then
    request will fail with an `IllegalArgumentException`.

-   Otherwise, master will; 

    -   Mark requester as data member.

    -   Update its member list with an incremented version number.

    -   Publish new member list to the cluster. (Also see [Cluster Service Consistency Improvements Design](../cluster/01-clusterservice-consistency-improvements.md)
        for member list publish mechanism.)

    -   Notify partition service to trigger rebalancing.

    -   Return new member list to the caller.

-   When requester receives member list response from master, it will
    update its member list and mark its local member as data member.

-   If local member list update fails, which means master has changed
    during promotion, then method will fail with
    an `IllegalStateException`.  
      

The promotion algorithm guarantees that the promotion process is
executed in a safe way. Failures are explicit so that the action can be
retried safely and once the promotion has succeeded, it will be
eventually dispatched to all nodes in the cluster. 

### Fragmented Migration

Currently, when a partition migration is requested, all partition data
is copied/transferred in a single shot, in a big replication chunk,
which increases pressure on both memory and network. With this new
Fragmented Migration mechanism, a big replication chunk is migrated with
fragments to reduce memory consumption and increase availability of the
system. As fragments are portions of the big replication chunk, the
migration process causes less memory and network bandwidth consumption.
Additionally, these fragments are migrated with interleaving, which
means that partition threads will be able to process requests of other
partitions during migrations. 

The Fragmented Migration mechanism will increase availability of the
system during migrations trading off increased migration time against
cluster stability. It is built on the mechanics introduced
by [Fine-Grained Anti Entropy Mechanism Design](03-fine-grained-anti-entropy-mechanism.md). 

There is no change in the master node's migration orchestration tasks.
The whole migration fragmentation logic is encapsulated between
migration source and destination.

Once the migration source receives the migration request, it first sets
the migration flag so that there will be no further update in the
partition data. Once migration is initiated and migration flag is set,
until migration of the partition is completed, no updates are allowed on
that partition. Reads are allowed unless it's explicitly disabled by
system property `hazelcast.partition.migration.stale.read.disabled`.
There is no change in this part, existing behaviour is kept as is. In
the future, we can allow updates to pending namespaces, fragments of the
partitions those are not migrated yet. 

Once the partition is marked, the migration source collects all
namespaces by calling
`FragmentedMigrationAwareService#getAllServiceNamespaces()`. Then, it
performs the following steps until all fragments are migrated to the
destination:

-   By iterating over collected namespaces, select a set of namespaces
    to migrate in one shot

-   Collect partition replica version vectors of namespaces

-   Collect migration operations of the current namespaces
    via `FragmentedMigrationAwareService#prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces)`

-   Put the collected namespace replica version vectors and migration
    operations into an `ReplicaFragmentMigrationState` object and send
    it to the migration destination.  
      

##### **ReplicaFragmentMigrationState**

```java
class ReplicaFragmentMigrationState implements IdentifiedDataSerializable {

    private Map<ServiceNamespace, long[]> namespaces;

    private Collection<Operation> migrationOperations;

}
```

  
Once a group of fragments are sent, a callback is set to send new
fragments when the response for the current batch is received from the
migration destination. 

Currently, we do the following fragment grouping:

-   All non-fragmented migration aware services are put into a single
    fragment and sent in one go. 

-   Namespaces of fragmented migration aware services are sent
    separately. 

Lets say we have `MapService` and `CacheService` as fragmented migration
services (i.e., other services are non-fragmented) and we have `"map1"`,
`"map2"`, `"cache1"`, `"list1", "queue1"` as data structures. Then, we
apply the following grouping:

-   \[ "list1", "queue1" \]

-   \[ "map1" \]

-   \[ "map2" \]

-   \[ "cache1" \]

The migration destination applies the received fragments with a similar
approach. Once the first fragment is received, it sets the migration
status and validates it for the further fragments received from the
migration source. 

When the migration source receives the response from the migration
destination for the last fragment, it returns the migration completion
result to the master. After this point, commit / rollback phase is
applied the same way. On migration finalization, all migrated namespace
replica versions are committed / rolled back. 

Fragmented migration can be disabled by setting configuration
property `hazelcast.partition.migration.fragments.enabled `to` false. `When
fragmented migration is disabled, partitions are migrated in a single
chunk as it's done before.
