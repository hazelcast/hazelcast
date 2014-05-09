


## JMX API per Node

Hazelcast members expose various management beans which includes statistics about distributed data structures and states of Hazelcast node internals.

The metrics are local to the nodes which means they do not reflect cluster wide values.

You can find the JMX API definition below with descriptions and the API methods in parenthesis.

**Atomic Long (`IAtomicLong`)**

*  Name ( `name` )
*  Current Value ( `currentValue` )
*  Set Value ( `set(v)` )
*  Add value and Get ( `addAndGet(v)` )
*  Compare and Set ( `compareAndSet(e,v)` )
*  Decrement and Get ( `decrementAndGet()` )
*  Get and Add ( `getAndAdd(v)` )
*  Get and Increment ( `getAndIncrement()` )
*  Get and Set ( `getAndSet(v)` )
*  Increment and Get ( `incrementAndGet()` )
*  Partition key ( `partitionKey` )

**Atomic Reference ( `IAtomicReference` )**

*  Name ( `name` )
*  Partition key  ( `partitionKey`)

**Countdown Latch ( `ICountDownLatch` )**

*  Name ( `name` )
*  Current count ( `count`)
*  Countdown ( `countDown()` )
*  Partition key  ( `partitionKey`)

**Executor Service ( `IExecutorService` )**

*  Local pending operation count ( `localPendingTaskCount` )
*  Local started operation count ( `localStartedTaskCount` )
*  Local completed operation count ( `localCompletedTaskCount` )
*  Local cancelled operation count ( `localCancelledTaskCount` )
*  Local total start latency ( `localTotalStartLatency` )
*  Local total execution latency ( `localTotalExecutionLatency` )

**List ( `IList` )**

*  Name ( `name` )
*  Clear list ( `clear` )
*  Total added item count ( `totalAddedItemCount` )
*  Total removed item count ( `totalRemovedItemCount` )

**Lock ( `ILock` )**

*  Name ( `name` )
*  Lock Object ( `lockObject` )
*  Partition key ( `partitionKey` )

**Map ( `IMap` )**

*  Name ( `name` )
*  Size ( `size` )
*  Config ( `config` )
*  Owned entry count ( `localOwnedEntryCount` )
*  Owned entry memory cost ( `localOwnedEntryMemoryCost` )
*  Backup entry count ( `localBackupEntryCount` )
*  Backup entry cost ( `localBackupEntryMemoryCost` )
*  Backup count ( `localBackupCount` )
*  Creation time ( `localCreationTime` )
*  Last access time ( `localLastAccessTime` )
*  Last update time ( `localLastUpdateTime` )
*  Hits ( `localHits` )
*  Locked entry count ( `localLockedEntryCount` )
*  Dirty entry count ( `localDirtyEntryCount` )
*  Put operation count ( `localPutOperationCount` )
*  Get operation count ( `localGetOperationCount` )
*  Remove operation count ( `localRemoveOperationCount` )
*  Total put latency ( `localTotalPutLatency` )
*  Total get latency ( `localTotalGetLatency` )
*  Total remove latency ( `localTotalRemoveLatency` )
*  Max put latency ( `localMaxPutLatency` )
*  Max get latency ( `localMaxGetLatency` )
*  Max remove latency ( `localMaxRemoveLatency` )
*  Event count ( `localEventOperationCount` )
*  Other (keySet,entrySet etc..) operation count ( `localOtherOperationCount` )
*  Total operation count ( `localTotal` )
*  Heap Cost ( `localHeapCost` )
*  Total added entry count ( `totalAddedEntryCount` )
*  Total removed entry count ( `totalRemovedEntryCount` )
*  Total updated entry count ( `totalUpdatedEntryCount` )
*  Total evicted entry count ( `totalEvictedEntryCount` )
*  Clear ( `clear()` )
*  Values ( `values(p)`)
*  Entry Set ( `entrySet(p)` )

**MultiMap ( `MultiMap` )**

*  Name ( `name` )
*  Size ( `size` )
*  Owned entry count ( `localOwnedEntryCount` )
*  Owned entry memory cost ( `localOwnedEntryMemoryCost` )
*  Backup entry count ( `localBackupEntryCount` )
*  Backup entry cost ( `localBackupEntryMemoryCost` )
*  Backup count ( `localBackupCount` )
*  Creation time ( `localCreationTime` )
*  Last access time ( `localLastAccessTime` )
*  Last update time ( `localLastUpdateTime` )
*  Hits ( `localHits` )
*  Locked entry count ( `localLockedEntryCount` )
*  Put operation count ( `localPutOperationCount` )
*  Get operation count ( `localGetOperationCount` )
*  Remove operation count ( `localRemoveOperationCount` )
*  Total put latency ( `localTotalPutLatency` )
*  Total get latency ( `localTotalGetLatency` )
*  Total remove latency ( `localTotalRemoveLatency` )
*  Max put latency ( `localMaxPutLatency` )
*  Max get latency ( `localMaxGetLatency` )
*  Max remove latency ( `localMaxRemoveLatency` )
*  Event count ( `localEventOperationCount` )
*  Other (keySet,entrySet etc..) operation count ( `localOtherOperationCount` )
*  Total operation count ( `localTotal` )
*  Clear ( `clear()` )

**Replicated Map ( `ReplicatedMap` )**

*  Name ( `name` )
*  Size ( `size` )
*  Config ( `config` )
*  Owned entry count ( `localOwnedEntryCount` )
*  Creation time ( `localCreationTime` )
*  Last access time ( `localLastAccessTime` )
*  Last update time ( `localLastUpdateTime` )
*  Hits ( `localHits` )
*  Put operation count ( `localPutOperationCount` )
*  Get operation count ( `localGetOperationCount` )
*  Remove operation count ( `localRemoveOperationCount` )
*  Total put latency ( `localTotalPutLatency` )
*  Total get latency ( `localTotalGetLatency` )
*  Total remove latency ( `localTotalRemoveLatency` )
*  Max put latency ( `localMaxPutLatency` )
*  Max get latency ( `localMaxGetLatency` )
*  Max remove latency ( `localMaxRemoveLatency` )
*  Event count ( `localEventOperationCount` )
*  Replication event count ( `localReplicationEventCount` )
*  Other (keySet,entrySet etc..) operation count ( `localOtherOperationCount` )
*  Total operation count ( `localTotal` )
*  Total added entry count ( `totalAddedEntryCount` )
*  Total removed entry count ( `totalRemovedEntryCount` )
*  Total updated entry count ( `totalUpdatedEntryCount` )
*  Clear ( `clear()` )
*  Values ( `values()`)
*  Entry Set ( `entrySet()` )

**Queue ( `IQueue` )**

*  Name ( `name` )
*  Config ( `QueueConfig` )
*  Partition key ( `partitionKey` )
*  Owned item count ( `localOwnedItemCount` )
*  Backup item count ( `localBackupItemCount` )
*  Minimum age ( `localMinAge` )
*  Maximum age ( `localMaxAge` )
*  Average age ( `localAveAge` )
*  Offer operation count ( `localOfferOperationCount` )
*  Rejected offer operation count ( `localRejectedOfferOperationCount` )
*  Poll operation count ( `localPollOperationCount` )
*  Empty poll operation count ( `localEmptyPollOperationCount` )
*  Other operation count ( `localOtherOperationsCount` )
*  Event operation count ( `localEventOperationCount` )
*  Total added item count ( `totalAddedItemCount` )
*  Total removed item count ( `totalRemovedItemCount` )
*  Clear ( `clear()` )

**Semaphore ( `ISemaphore` )**

*  Name ( `name` )
*  Available permits ( `available` )
*  Partition key ( `partitionKey` )
*  Drain ( `drain()`)
*  Shrink available permits by given number ( `reduce(v)` )
*  Release given number of permits ( `release(v)` )

**Set ( `ISet` )**

*  Name ( `name` )
*  Partition key ( `partitionKey` )
*  Total added item count ( `totalAddedItemCount` )
*  Total removed item count ( `totalRemovedItemCount` )
*  Clear ( `clear()` )

**Topic ( `ITopic` )**

*  Name ( `name` )
*  Config ( `config` )
*  Creation time ( `localCreationTime` )
*  Publish operation count ( `localPublishOperationCount` )
*  Receive operation count ( `localReceiveOperationCount` )
*  Total message count ( `totalMessageCount` )

**Hazelcast Instance ( `HazelcastInstance` )**

*  Name ( `name` )
*  Version ( `version` )
*  Build ( `build` )
*  Configuration ( `config` )
*  Configuration source ( `configSource` )
*  Group name ( `groupName` )
*  Network Port ( `port` )
*  Cluster-wide Time ( `clusterTime` )
*  Size of the cluster ( `memberCount` )
*  List of members ( `Members` )
*  Running state ( `running` )
*  Shutdown the member ( `shutdown()` )
*  **Node ( `HazelcastInstance.Node` )**
 *  Address ( `address` )
 *  Master address ( `masterAddress` )
* **Event Service ( `HazelcastInstance.EventService` )**
 *  Event thread count  ( `eventThreadCount` )
 *  Event queue size ( `eventQueueSize` )
 *  Event queue capacity ( `eventQueueCapacity` )
* **Operation Service ( `HazelcastInstance.OperationService` )**
 *  Response queue size  ( `responseQueueSize` )
 *  Operation executor queue size ( `operationExecutorQueueSize` )
 *  Running operation count ( `runningOperationsCount` )
 *  Remote operation count ( `remoteOperationCount` )
 *  Executed operation count ( `executedOperationCount` )
 *  Operation thread count ( `operationThreadCount` )
* **Proxy Service ( `HazelcastInstance.ProxyService` )**
 *  Proxy count ( `proxyCount` )
* **Partition Service ( `HazelcastInstance.PartitionService` )**
 *  Partition count ( `partitionCount` )
 *  Active partition count ( `activePartitionCount` )
* **Connection Manager ( `HazelcastInstance.ConnectionManager` )**
 *  Client connection count ( `clientConnectionCount` )
 *  Active connection count ( `activeConnectionCount` )
 *  Connection count ( `connectionCount` )
* **Client Engine ( `HazelcastInstance.ClientEngine` )**
 *  Client endpoint count ( `clientEndpointCount` )
* **System Executor ( `HazelcastInstance.ManagedExecutorService` )**
 *  Name ( `name` )
 *  Work queue size ( `queueSize` )
 *  Thread count of the pool ( `poolSize` )
 *  Maximum thread count of the pool ( `maximumPoolSize` )
 *  Remaining capacity of the work queue ( `remainingQueueCapacity` )
 *  Is shutdown ( `isShutdowm` )
 *  Is terminated ( `isTerminated` )
 *  Completed task count ( `completedTaskCount` )
* **Operation Executor ( `HazelcastInstance.ManagedExecutorService` )**
 *  Name ( `name` )
 *  Work queue size ( `queueSize` )
 *  Thread count of the pool ( `poolSize` )
 *  Maximum thread count of the pool ( `maximumPoolSize` )
 *  Remaining capacity of the work queue ( `remainingQueueCapacity` )
 *  Is shutdown ( `isShutdowm` )
 *  Is terminated ( `isTerminated` )
 *  Completed task count ( `completedTaskCount` )
* **Async Executor (`HazelcastInstance.ManagedExecutorService`)**
 *  Name ( `name` )
 *  Work queue size ( `queueSize` )
 *  Thread count of the pool ( `poolSize` )
 *  Maximum thread count of the pool ( `maximumPoolSize` )
 *  Remaining capacity of the work queue ( `remainingQueueCapacity` )
 *  Is shutdown ( `isShutdowm` )
 *  Is terminated ( `isTerminated` )
 *  Completed task count ( `completedTaskCount` )
* **Scheduled Executor ( `HazelcastInstance.ManagedExecutorService` )**
 *  Name ( `name` )
 *  Work queue size ( `queueSize` )
 *  Thread count of the pool ( `poolSize` )
 *  Maximum thread count of the pool ( `maximumPoolSize` )
 *  Remaining capacity of the work queue ( `remainingQueueCapacity` )
 *  Is shutdown ( `isShutdowm` )
 *  Is terminated ( `isTerminated` )
 *  Completed task count ( `completedTaskCount` )
* **Client Executor ( `HazelcastInstance.ManagedExecutorService` )**
 *  Name ( `name` )
 *  Work queue size ( `queueSize` )
 *  Thread count of the pool ( `poolSize` )
 *  Maximum thread count of the pool ( `maximumPoolSize` )
 *  Remaining capacity of the work queue ( `remainingQueueCapacity` )
 *  Is shutdown ( `isShutdowm` )
 *  Is terminated ( `isTerminated` )
 *  Completed task count ( `completedTaskCount` )
* **Query Executor ( `HazelcastInstance.ManagedExecutorService` )**
 *  Name ( `name` )
 *  Work queue size ( `queueSize` )
 *  Thread count of the pool ( `poolSize` )
 *  Maximum thread count of the pool ( `maximumPoolSize` )
 *  Remaining capacity of the work queue ( `remainingQueueCapacity` )
 *  Is shutdown ( `isShutdowm` )
 *  Is terminated ( `isTerminated` )
 *  Completed task count ( `completedTaskCount` )
* **IO Executor ( `HazelcastInstance.ManagedExecutorService` )**
 *  Name ( `name` )
 *  Work queue size ( `queueSize` )
 *  Thread count of the pool ( `poolSize` )
 *  Maximum thread count of the pool ( `maximumPoolSize` )
 *  Remaining capacity of the work queue ( `remainingQueueCapacity` )
 *  Is shutdown ( `isShutdowm` )
 *  Is terminated ( `isTerminated` )
 *  Completed task count ( `completedTaskCount` )
