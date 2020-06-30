# Fine-Grained Anti Entropy Mechanism

|ℹ️ Since: 3.9| 
|-------------|

## Background

### Description

Current anti-entropy mechanism (which detects inconsistent backup
replicas and syncs them with primary) replicates whole partition data
when an inconsistency is detected. When partition data is large
(especially in case of HD), anti entropy can cause large bubbles in
processing & network pipeline.

Instead, we will develop finer-grained mechanism to detect
inconsistencies per data structure (IMap, ICache, IQueue etc) and only
replicate the specific data structure of partition. 

### Terminology

| Term     | Definition                                                                                                               |
|----------|--------------------------------------------------------------------------------------------------------------------------|
| Fragment |  A portion of the partition data. Currently, a fragment corresponds to data of a specific data structure in a partition. |

## Functional Design

No functional changes are planned. Migration/replication and anti
entropy will continue to work as it is.

## User Interaction

### API design and/or Prototypes

There are no user level API for migrations and anti entropy. No further
API changes are planned. 

## Technical Design

### Anti Entropy Mechanism Before 3.9

Before Hazelcast 3.9, each primary partition manages its own backup
replica version vector. Since there are at most 6 backup replicas,
length of vector is also 6. Each element of the vector shows version of
the specific replica with the same index. For a replica version vector
`[A, B, C, D, E, F] `of partition `P`; `A` is the version of the
`1st` replica of `P`, `B` is the version of the `2nd` replica of P ...
so on. 

When a backup-aware operation is executed on primary replica of
partition `P`, versions in replica version vector are incremented
according to desired backup count of operation. For example, if
operation requests 3 backups to be replicated then only initial 3
replica versions are incremented, hence new versions
become `[A+1, B+1, C+1, D, E, F].`

Each replica keeps only versions for replicas whose indexes are
greater-than-or-equal to its own index. Versions for lesser indexes are
kept as zero (0). Because, each replica is super-set of replicas whose
indexes are greater than its own index, it contains all data greater
index replicas have. For example,

-   1st replica of partition `P`  knows versions
    as `[A, B, C, D, E, F]` 

-   2nd replica knows as `[0, B, C, D, E, F]`

-   3rd replica knows as `[0, 0, C, D, E, F]`

-   so on.

With each backup operation, replica version vector maintained by primary
replica is copied to the backup replica. For 3 backup case,
versions `[A+1, B+1, C+1, D, E, F]` are carried via backup operations.
After backup operations are executed, versions on backup replicas will
be;

-   1st replica:  `[A+1, B+1, C+1, D, E, F]`

-   2nd replica: `[0, B+1, C+1, D, E, F]`

-   3rd replica:  `[0, 0, C+1, D, E, F]`  
      

According to specific version in backup operation and version known by
backup replica, anti entropy mechanism decides whether to apply backup
operation and/or to set versions;  

-   If version in backup operation is equal to plus one of version in
    backup replica, then backup operation is applied and versions are
    set.  
    For example when version in backup operation is A+1 and version
    known by backup replica is A then version in backup replica is set
    to A+1.  
      

-   If version in backup operation is less than version in backup
    replica, then backup operation is ignored and versions are kept
    intact. By this way, applying a stale backup operation is
    prevented.  
    For example when version in backup operation is A and version known
    by backup replica is A+2.  
      

-   If version in backup operation is greater than plus one of version
    in backup replica, then backup operation is applied and versions are
    set. But also replica is marked as dirty and a replica sync request
    is sent to primary replica. Since, not incremental version means,
    there's at least one backup that's not received/applied yet.  
    For example when version in backup operation is A+2 and version
    known by backup replica is A, then version in backup replica is set
    to A+2 and dirty flag is set.   
      

-   If replica is already marked as dirty, when a backup operation is
    received, regardless of version in operation, a new replica sync
    request is sent to primary replica. 

  

Apart from backup operation execution, there's also periodic task
running on each primary replica (with the interval of
`hazelcast.partition.backup.sync.interval`, default 30 seconds) which
sends replica versions to each backup replica. When this task detects a
missing backup, then anti entropy again triggers replica sync from
primary.

There are also guarding mechanisms to prevent flooding system with
multiple replica sync requests. For each partition, only a single sync
request can be sent within an interval. If no sync response is received
in that interval, then a new sync request is allowed. Also globally,
there's a limit to restrict concurrent replica sync requests and
response on a member. This is to avoid OOM with multiple replica sync
data.

### Fine-Grained Anti Entropy Mechanism

By fine-grained anti-entropy mechanism, per partition version vectors
are divided into fragments. Each fragment is represented with a
namespace which is defined by migration-aware-service itself. Since this
is an opt-in feature for services, there will be a internal namespace
for all non-fragmented services to keep existing functionality. 

-   Without fragmented replica mechanism, each partition had a single
    version vector:  
    `Partition P` → `[A, B, C, D, E, F]`  
      

-   With fragmented replicas, version handling becomes:  
    `Partition P` → `{Namespace-X: [Ax, Bx, Cx, Dx, Ex, Fx],`  
    `               Namespace-Y: [Ay, By, Cy, Dy, Ey, Fy],`  
    `               Namespace-Z: [Az, Bz, Cz, Dz, Ez, Fz],...}`

  

Instead of transferring/keeping a global-per-partition version vector,
each backup operation will carry its own namespace version vector. If a
service or a backup operation is not fragment aware then internal
namespace will be used. Other than this difference, all remaining
anti-entropy mechanics will be the same. All version comparisons and
replica sync requests will be based on per-partition namespace version
vectors, instead of global-per-partition version vectors. 

Periodic anti-entropy task, which runs on primary replica, sends
namespace versions to backup replicas for each partition. Then on backup
replicas, it gathers all inconsistent namespaces and sends replica
sync requests to primary replica for all inconsistent namespaces in one
go. Then, it's up to the partition owner to how to respond sync
requests, either in one shot or in fragments.

  

`ServiceNamespace` is abstraction of this namespace, whose instances
will be created by `FragmentedMigrationAwareService`s;

```java
public interface ServiceNamespace extends DataSerializable {
    /**
     * Name of the service which fragments belongs to
     * @return name of the service
     */
    String getServiceName();
}
```

  

`FragmentedMigrationAwareService` is an extension to
existing `MigrationAwareService` abstraction. It adds two new namespace
aware replication methods.

```java
public interface MigrationAwareService {

    Operation prepareReplicationOperation(PartitionReplicationEvent event);
    
    [...]
}
```

  

```java
public interface FragmentedMigrationAwareService extends MigrationAwareService {

    Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event);


    boolean isKnownServiceNamespace(ServiceNamespace namespace);

    Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces);
}
```

  
`FragmentedMigrationAwareService`'s `prepareReplicationOperation(event, namespaces)` method
creates a replication operation for only given namespaces instead of
whole service data.

  

`BackupAwareOperation`s and `BackupOperation`s created by
`FragmentedMigrationAwareService` should know their own namespaces and
they should explicitly implement ServiceNamespaceAware interface.

```java
public interface ServiceNamespaceAware {

    ServiceNamespace getServiceNamespace();
}
```

  

Backup handling mechanism invokes
`ServiceNamespaceAware.getServiceNamespace()` method of
`BackupAwareOperation`s and `BackupOperation`s to find out specific
namespace. 

  

Additionally, anti-entropy retry mechanism is removed. Replica sync
requests were being retried when primary owner was lacking permits (due
to max allowed parallel replications limit). But this retry mechanism
was too complex and mostly redundant. Backup replica can already re-send
another sync request when initial sync request timeouts.

### IMap Fragmented Replication Implementation

IMap partitions are fragmented by map data structures. Each IMap segment
in a partition forms a single replica fragment. Theoretically dividing
into more smaller fragments is also possible but that requires a tree
like structure to keep map data instead of currently used hash based
maps.

As a first step, `MapMigrationAwareService` is converted
to `FragmentedMigrationAwareService` and abstract `MapOperation,` which
is the base class for all IMap operations,
implements `ServiceNamespaceAware` interface.
 Then `MapReplicationOperation` is slightly modified to only replicate a
set of namespaces.

```java
public abstract class MapOperation extends AbstractNamedOperation implements IdentifiedDataSerializable, ReplicaFragmentAware {
   [...]

    @Override
    public ObjectNamespace getServiceNamespace() {
        MapContainer container = mapContainer;
        if (container == null) {
            MapService service = getService();
            container = service.getMapServiceContext().getMapContainer(name);
        }
        return container.getObjectNamespace();
    }
}
```

###  ICache Fragmented Replication Implementation

Similar to IMap, ICache partitions are fragmented per ICache partition
segment. `ICacheService` implements `FragmentedMigrationAwareService`
and some base/abstract cache operations (such as
`AbstractCacheOperation`,  `AbstractHiDensityCacheOperation` ...)
implement `ServiceNamespaceAware` interface.

```java
abstract class AbstractCacheOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, ReplicaFragmentAware, IdentifiedDataSerializable {

    [...]

    @Override
    public ObjectNamespace getServiceNamespace() {
        ICacheRecordStore recordStore = cache;
        if (recordStore == null) {
            ICacheService service = getService();
            recordStore = service.getOrCreateRecordStore(name, getPartitionId());
        }
        return recordStore.getObjectNamespace();
    }
}
```

  

`CacheReplicationOperation` is modified to only replicate a set of
namespaces and `HiDensityCacheReplicationOperation` (which is EE version
of cache replication operation and renamed from
`CacheReplicationOperation`) is adapted to these changes.

  

Similar to IMap and ICache, MultiMap and ILock services are converted
to `FragmentedMigrationAwareService`. ILock service already
uses `ObjectNamespace` internally to distinguish its own locks and locks
managed by IMap and MultiMap. Even though ILock and IMap (also MultiMap)
partition data are replicated separately by their services, ILock data
belonging to IMap must be replicated together with IMap partition. To
accomplish that, `ObjectNamespace` now extends `ServiceNamespace`
and `ObjectNamespace`s known by ILock service are used as replica
fragment namespaces too.

  

```java
public interface ObjectNamespace extends ServiceNamespace {

    /**
     * Gets the object name within the service.
     *
     * @return the object name within the service
     */
    String getObjectName();
}
```

  
  

  
