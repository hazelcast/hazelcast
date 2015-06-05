
### Fixes

**3.4.3 Fixes**

This section lists issues solved for Hazelcast 3.4.3 release.

- Write-behind may cause reading of stale value upon migration [[#5339]](https://github.com/hazelcast/hazelcast/issues/5339).
- Last update time of an entry should not be changed after `getAll()` is invoked [[#5333]](https://github.com/hazelcast/hazelcast/issues/5333).
- `AtomicReference.alterAndGet()` throws `HazelcastSerializationException` [[#5265]](https://github.com/hazelcast/hazelcast/issues/5265).
- `ICompletableFuture` callback from the method `getAsync` is not always invoked [[#5133]](https://github.com/hazelcast/hazelcast/issues/5133).
- Warnings and exceptions are logged when closing the client connection [[#4966]](https://github.com/hazelcast/hazelcast/issues/4966).
- `CacheConfig` is not created on the cluster if the executer of `CacheCreateConfigOperation` has already a `CacheConfig` [[#4960]](https://github.com/hazelcast/hazelcast/issues/4960).
- The schema does not allow for an explicit `hz:replicatedMap` element to be created. One can be created inside `hz:config` but not as a definition for a concrete Replicated Map. Therefore, at present it is impossible to define a Replicated Map using Spring. [[#4958]](https://github.com/hazelcast/hazelcast/issues/4958).
- `ResponseThread` and `InvocationRegistry.InspectionThread` reset and retry operations. Since these threads did not implement `NIOTHread`, the `OperationExecutor` is free to execute tasks on these threads and that is not  desirable [[#4929]](https://github.com/hazelcast/hazelcast/issues/4929).
- The method `CacheManager.getCache()` does not re-open the closed cache. It should let access to the closed cache and re-open it. Cache can be accessed by `getCache` but it is still closed [[#4631]](https://github.com/hazelcast/hazelcast/issues/4631).
- The method `close()` of a Closeable `CacheLoader` is called without explicitly calling the method `Cache.close()` [[#4617]](https://github.com/hazelcast/hazelcast/issues/4617).
- The method `Cache.close()` does not call the method `close()` of registered Closeable `CacheEntryListener` [[#4616]](https://github.com/hazelcast/hazelcast/issues/4616).
- The method `NotEqualPredicate` should return false if entry is null (without index) and also if index is present, it should not throw an exception with null values [[#4525]](https://github.com/hazelcast/hazelcast/issues/4525).
- When running Hazelcast with Spring and Hibernate 4 and when an application is started, the error related to `org/hibernate/cache/QueryResultsRegion` is produced [[#4519]](https://github.com/hazelcast/hazelcast/issues/4519).
- Predicates with null values throws exception for unordered indexes [[#4373]](https://github.com/hazelcast/hazelcast/issues/4373).



**3.4.2 Fixes**

This section lists issues solved for Hazelcast 3.4.2 release.

- While executing unit tests, `SlowOperationDetectorThread` and `CleanupThread` may not be terminated before the next test is started [[#4757]](https://github.com/hazelcast/hazelcast/issues/4757).
- When multiple nodes join sequentially after partitions are assigned/distributed, old nodes fail to clean backup replicas larger than the configured backup count. This causes a memory leak. Also, when multiple nodes leave the cluster at the same time (or in a short period), the new partition owner looses some partition replica versions and this causes backup nodes for those specific replica indexes to fail synchronizing data from the owner node, although the owner node holds the whole partition data [[#4687]](https://github.com/hazelcast/hazelcast/issues/4687).
- After cluster merges due to a network-split, Hazelcast infinitely logs `WaitNotifyServiceImpl$WaitingOp::WrongTargetException` warnings [[#4676]](https://github.com/hazelcast/hazelcast/issues/4676).
- A strange `mapName` parameter occurred when using wildcard configuration for a custom `MapStoreFacory` [[#4667]](https://github.com/hazelcast/hazelcast/issues/4667).
- The method `IExecutorService.submitToKeyOwner` encountered two errors: the `onResponse` method is invoked with null and a cast exception is thrown in a Hazelcast thread [[#4627]](https://github.com/hazelcast/hazelcast/issues/4627).
- The method `init` in an implementation of the `MapLoaderLifecyleSupport` interface is not invoked [[#4623]](https://github.com/hazelcast/hazelcast/issues/4623).
- The method `readData` in `NearCacheConfig` reads the `maxSize` twice [[#4609]](https://github.com/hazelcast/hazelcast/issues/4609).
- The system property `hazelcast.client.request.retry.count` is not handled properly [[#4592]](https://github.com/hazelcast/hazelcast/issues/4592).


**3.4.1 Fixes**

This section lists issues solved for Hazelcast 3.4.1 release.

- IMap.getAll does not put data to RecordStore upon loading from map store [[#4458]](https://github.com/hazelcast/hazelcast/issues/4458).
- In the ClientNearCache class, there is a comparator which is used in a TreeSet to find the entries that should be evicted. If there are CacheRecords with the same hit count or lastAccessTime (depending on the policy, i.e. LFU or LRU), all of them should be evicted [[#4451]](https://github.com/hazelcast/hazelcast/issues/4451).
- When using write-behind and the entries, which have not been stored yet, are evicted, duplicate calls to the map store is made [[#4448]](https://github.com/hazelcast/hazelcast/issues/4448).
- There is a memory leak caused by the empty await queues in WaitNotifyService. When more then one thread try to lock on an IMap key at the same time, a memory leak occurs [[#4432]](https://github.com/hazelcast/hazelcast/issues/4432).
- ClientListener is not configurable via ListenerConfig. HazelcastInstanceImpl.initializeListeners(Config config) does not honor ClientListener instances [[#4429]](https://github.com/hazelcast/hazelcast/issues/4429).
- The CacheConfig(CacheSimpleConfig simpleConfig) constructor is broken. Variable assignments should be fixed [[#4423]](https://github.com/hazelcast/hazelcast/issues/4423).
- In ReplicatedMap, the containsKey method should return false on the removed keys [[#4420]](https://github.com/hazelcast/hazelcast/issues/4420).
- During the Hazelcast.shutdownAll() process, LockService is shut down before the MapService and this may cause null pointer exception if there is something like isLocked check in some internal IMap operations [[#4382]](https://github.com/hazelcast/hazelcast/issues/4382).
- Hazelcast clients shut down in the case of an IP change of one or more of the configured node (DNS) addresses [[#4349]](https://github.com/hazelcast/hazelcast/issues/4349).
- Write-behind system coalesces all operations on a specific key in a configured write-delay-seconds window and it should also store only the latest change on that key in that window. Problem with the current behavior is; a continuously updated key may not be persisted ever due to the shifted store time during the updates [[#4341]](https://github.com/hazelcast/hazelcast/issues/4341).
- Issue with contains pattern in Config.getXXXConfig(). Since the actual wildcard search always does a contains matching, you cannot set a configuration for startsWith, for instance [[#4315]](https://github.com/hazelcast/hazelcast/issues/4315).
- ReplicatedMapMBean is not present in JMX [[#4173]](https://github.com/hazelcast/hazelcast/issues/4173).

**3.4 Fixes**

This section lists issues solved for **Hazelcast 3.4** release.

- Deadlock happens in MapReduce implementation when there is a high load on the system. The issue has been solved by offloading Distributed MapReduce result collection to the async executor [[#4238]](https://github.com/hazelcast/hazelcast/issues/4238).
- When the class `ClientExecutorServiceSubmitTest.java` is compiled using the Eclipse compiler, it gives a compile error: "*The method submit(Runnable, ExecutionCallback) is ambiguous for the type IExecutorService*". The reason is that the `IExecutorService.java` class does not have some generics. The issue has been solved by adding these missing generics to the `IExecutorService.java` class [[#4234]](https://github.com/hazelcast/hazelcast/issues/4234).
- JCache declarative listener registration does not work [[#4215]](https://github.com/hazelcast/hazelcast/issues/4215).
- JCache evicts the records which are not expired yet. To solve this issue, the `clear` method should be removed that runs when the size is smaller than the minimum eviction element count (`MIN_EVICTION_ELEMENT_COUNT`) [[#4124]](https://github.com/hazelcast/hazelcast/issues/4124).
- Hazelcast Enterprise Native Memory operations should be updated in relation with the Hazelcast sync listener changes [[#4089]](https://github.com/hazelcast/hazelcast/issues/4089).
- The completion listener (JCache) relies on event ordering but if the completion listener is registered in another node then event ordering is not guaranteed [[#4073]](https://github.com/hazelcast/hazelcast/issues/4073).
- AWS joiner classname should be fixed since EC2 discovery is not working after the restructure [[#4025]](https://github.com/hazelcast/hazelcast/issues/4025).
- If an IMap has a near cache configured, accessing the near cache via the method `get(key)` does not count as an access to the underlying IMap. The near cache has its own `max-idle-seconds` element. However, if an entry is expired/evicted in the IMap, it also causes a near cache removal operation for the entry regardless of the `max-idle-seconds` of that entry in the near cache. The entry expires and is evicted even if the near cache is being hit constantly. When a near cache is hit, the underlying map should reset the idle time for that key [[#4016]](https://github.com/hazelcast/hazelcast/issues/4016).
- Getting a pre-configured Cache instance is not working as expected [[#4009]](https://github.com/hazelcast/hazelcast/issues/4009).
- Bounded Queue section in the Reference Manual is unclear and wrong [[#3995]](https://github.com/hazelcast/hazelcast/issues/3995).
- The method `checkFullyProcessed` of MapReduce throws null pointer exception. The reason may be that multiple threads attempt to start the final processing state in the JobSupervisor [[#3952]](https://github.com/hazelcast/hazelcast/issues/3952).
- Merge operation after a split brain syndrome does not guarantee that the merging is over [[#3863]](https://github.com/hazelcast/hazelcast/issues/3863).
- When a client with near cache configuration enabled is shut down, `RejectedExecutionException` is thrown [[#3669]](https://github.com/hazelcast/hazelcast/issues/3669).
- In Hazelcast `IMap` and `TransactionalMap`, read-only operations such as `get()`, `containsKey()`, `keySet()`, and `containsValue()` break the transaction atomicity [[#3191]](https://github.com/hazelcast/hazelcast/issues/3191).
- Documentation should clearly list features of and differences between native clients [[#2385]](https://github.com/hazelcast/hazelcast/issues/2385).
- Sections of Hazelcast configuration should be able to be imported so that these sections can be shared between other Hazelcast configurations [[#406]](https://github.com/hazelcast/hazelcast/issues/406).