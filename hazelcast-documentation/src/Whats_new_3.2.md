# What's New in Hazelcast 3.2



## Release Notes

### New Features
This section provides the new features introduced with Hazelcast 3.2 release. 

-	**NIO Client**: New architecture based on NIO introduced to support more scalable and concurrent client usage.
-	**MapReduce Framework**: MapReduce implemented for your key-value collections that need to be reduced by grouping the keys. Please see [the interview](http://www.infoq.com/news/2014/02/hazelcast-mapreduce-api) and [MapReduce](#mapreduce) section.
-	**Order/Limit Support**: Now you can order and limit results returned by queries performed on Hazelcast Distributed Map.
-	**C++ Client**: Native C++ client developed for C++ users which can connect to a Hazelcast cluster and realize almost all operations that a node can perform. Please see [Native Clients](#native-clients).
-	**C# Client**: Also, Native C# client that has a very similar API with Native Java client developed. Please see [Native Clients](#native-clients).

### Improvements
This section provides the improvements performed for Hazelcast 3.2 release.

-	Size of a distributed queue via REST API can be returned. [[#1809]](https://github.com/hazelcast/hazelcast/pull/1809)	
-	InitialLoadMode configuration parameter (having Lazy and Eager as values) added to MapStoreConfig. [[#1751]](https://github.com/hazelcast/hazelcast/pull/1751)
-	Tagging support for Executor Service introduced such that nodes can be tagged for IExecutorService. [[1457]](https://github.com/hazelcast/hazelcast/issues/1457)
-	`getForUpdate()` operation for transactional map introduced. [[#1033]](https://github.com/hazelcast/hazelcast/issues/1033)
-	Entry processor can run on a set of keys with the introduction of `executeOnKeys(keys,entryprocessor)` method for IMap. [[1423]](https://github.com/hazelcast/hazelcast/pull/1423)
-	`getNearCacheStats()` introduced. Statistics for near cache can be retrieved. [[#30]](https://github.com/hazelcast/hazelcast/issues/30)

Please see the list of all enhancement issues [here](https://github.com/hazelcast/hazelcast/issues?labels=enhancement&milestone=29&page=3&state=closed).

### Fixes

**3.2.2 Fixes**

This section lists issues solved for Hazelcast 3.2.2 release.

-	Client security callable fix: https://github.com/hazelcast/hazelcast/pull/2561
-	Updating a key in a transaction gives listeners an entryAdded() callback instead of entryUpdated() https://github.com/hazelcast/hazelcast/issues/2542
-	Client ssl engine doesn't need keyStore and keyStorePassword https://github.com/hazelcast/hazelcast/pull/2525
-	Added support for Mapper, Combiner, Reducer, KeyValueSource to implement HazelcastInstanceAware https://github.com/hazelcast/hazelcast/pull/2502
-	Fixed alter function https://github.com/hazelcast/hazelcast/pull/2496
-	Return cached value upon IMap.get() if near cache is enabled https://github.com/hazelcast/hazelcast/pull/2482
-	Exception initialising hz:client https://github.com/hazelcast/hazelcast/issues/2480
-	Fixed portable serialization between different services versions https://github.com/hazelcast/hazelcast/pull/2478
-	Resolves a data race in the client proxy that can lead to an NPE. https://github.com/hazelcast/hazelcast/pull/2474
-	Fixed partition group hostname matching https://github.com/hazelcast/hazelcast/pull/2470
-	Client shutdown issue: Improve logging https://github.com/hazelcast/hazelcast/issues/2442
-	Unnecessary synchronized lock when invoking com.hazelcast.instance.LifecycleServiceImpl.isRunning() https://github.com/hazelcast/hazelcast/issues/2454
-	If MapStoreFactory throws exception, instance hangs https://github.com/hazelcast/hazelcast/issues/2445
-	Semaphore is given to the thread that is coming late https://github.com/hazelcast/hazelcast/issues/2443
-	Lots of exceptions when shutting down connection https://github.com/hazelcast/hazelcast/issues/2441
-	Migration fails when statistics are disabled https://github.com/hazelcast/hazelcast/issues/2436
-	3.2.1 regression: nested transactions are not caught and prevented. https://github.com/hazelcast/hazelcast/issues/2404
-	Heartbeat for Java Client https://github.com/hazelcast/hazelcast/issues/2297
-	Client proxy init synced https://github.com/hazelcast/hazelcast/pull/2376
-	Fixes hostname matching problem when interface has wildcards https://github.com/hazelcast/hazelcast/pull/2398
-	Fix weblogic shutdown backport https://github.com/hazelcast/hazelcast/pull/2391
-	NotWritablePropertyException connectionAttemptLimit with ssl client config https://github.com/hazelcast/hazelcast/issues/2335
-	Map-Reduce Operation fails, when another instance tries to form a cluster with an instance running a map reduce task https://github.com/hazelcast/hazelcast/issues/2354
-	EntryEvent getMember returning null when a node leaves the cluster https://github.com/hazelcast/hazelcast/issues/2358
-	NullPointerException in Bundle Activator https://github.com/hazelcast/hazelcast/issues/2489

Please see [here](https://github.com/hazelcast/hazelcast/issues?labels=&milestone=46&page=1&state=closed) for the full list of solved issues.

**3.2.1 Fixes**

This section lists issues solved for Hazelcast 3.2.1 release.

-	JCA problems have been fixed [#2025](https://github.com/hazelcast/hazelcast/issues/2025).
-	C++ client compilation problems are fixed.
-	Redo problem about Java dummy client is fixed.
-	Round robin load balancer of Java client is improved.
-	Initial timeout is for the initial connections in Java clients.
-	Wildcard configuration improvement in near cache configuration.
-	Unneeded serializations in EntryProcessor should be removed when the object format is *In-Memory* [#2139](https://github.com/hazelcast/hazelcast/issues/2139).
-	Race condition in near cache has been solved, immediate invalidation of local near cache was needed [#2163](https://github.com/hazelcast/hazelcast/issues/2163).
-	Predicate issue seen in transactions is solved.
-	Comparator issue in map eviction is solved.
-	Map eviction part has been refactored due to a race condition on map listener [#2324](https://github.com/hazelcast/hazelcast/issues/2324).
-	Stale data problem in client near cache has been solved [#2065](https://github.com/hazelcast/hazelcast/issues/2065).
-	Many checkstyle and findbugs issues are solved.

Please see [here](https://github.com/hazelcast/hazelcast/issues?labels=defect&milestone=43&page=1&state=open) for the full list of solved issues.




**3.2 Fixes**

This section lists issues solved for Hazelcast 3.2 release.

-	`LocalMapStats.getNearCacheStats()` can return null when it is called before a map get that calls `initNearCache()`. [[#2009]](https://github.com/hazelcast/hazelcast/issues/2009)
-	`testMapWithIndexAfterShutDown` fails in OpenJDK. [[#2001]](https://github.com/hazelcast/hazelcast/issues/2001)
-	Portable Serialization needs objects to be shared between client and server. [[#1957]](https://github.com/hazelcast/hazelcast/issues/1957)
-	Near cache entries should be locally invalidate on `IMap.executeOnKey()`. [[#1951]](https://github.com/hazelcast/hazelcast/issues/1951)
-	OperationTimeoutException is thrown when executing task that runs longer than `hazelcast.operation.call.timeout.millis`. [[#1949]](https://github.com/hazelcast/hazelcast/issues/1949)
-	`MapStore#store` was called when executing AbstractEntryProcessor on backup. [[#1940]](https://github.com/hazelcast/hazelcast/issues/1940)
-	After an OperationTimeoutException is thrown from `ILock.tryLock()	 (and after the system is back in a normal state), the named lock remains locked. [[#1937]](https://github.com/hazelcast/hazelcast/issues/1937)
-	Hazelcast client needs OutOfMemoryErrorDispatcher. [[#1933]](https://github.com/hazelcast/hazelcast/issues/1933)
-	Near Cache: Caching of local entries may lead to race condition. [[#1905]](https://github.com/hazelcast/hazelcast/issues/1905)
-	After key owner node dies, it takes too much time for threads to wakeup from `condition.await()`. [[#1879]](https://github.com/hazelcast/hazelcast/issues/1879)
-	Possible improvements/fixes for NearCache. [[#1863]](https://github.com/hazelcast/hazelcast/issues/1863)
-	`MultipleEntryBackupOperation` does not handle deletion of entries. [[#1854]](https://github.com/hazelcast/hazelcast/issues/1854)
-	If topics are created/destroyed, then the statistics for that topic are not destroyed and this can cause a memory leak. [[#1847]](https://github.com/hazelcast/hazelcast/issues/1847)
-	PartitionService backup/replication fixes. [[#1840]](https://github.com/hazelcast/hazelcast/issues/1840)
-	Cached null values remain in near cache after `evict` is called. [[#1829]](https://github.com/hazelcast/hazelcast/issues/1829)
-	NullPointerException in MultiMap when the service is shutdown before the migration is processed. [[#1823]](https://github.com/hazelcast/hazelcast/issues/1823)
-	Network interruption causes node to continually warn with WrongTargetException. [[#1815]](https://github.com/hazelcast/hazelcast/issues/1815)
-	`DefaultRecordStore#removeAll` should be modified so that it keeps "key objects to delete" as a list, not a set. [[#1795]](https://github.com/hazelcast/hazelcast/issues/1795)
-	Very long `operation.run()` call stack especially when high partition count is used. [[#1745]](https://github.com/hazelcast/hazelcast/issues/1745)
-	When executing an entry processor with an index aware predicate, the index is not used, instead the predicate is applied to the entire entry set. [[#1719]](https://github.com/hazelcast/hazelcast/issues/1719)
-	When one node goes down in a cluster with 2 nodes (where near cache is enabled), `containsKey` call hangs in the second node. [[#1688]](https://github.com/hazelcast/hazelcast/issues/1688)
-	When deleting an entry from an entry processor by setting the value to null, it is not removed from the backup store. [[#1687]](https://github.com/hazelcast/hazelcast/issues/1687)
-	Client calls executed at server side cause unwanted (de)serialization. [[#1669]](https://github.com/hazelcast/hazelcast/issues/1669)
-	In `TrackableJobFuture.get(long, TimeUnit)`, there is a 100 ms of sleep-spin while waiting for the result of a MapReduce task to be set. [[#1648]](https://github.com/hazelcast/hazelcast/issues/1648)
-	If `storeAll` takes much time and if instance terminates while map store is running, data can be lost. [[#1644]](https://github.com/hazelcast/hazelcast/issues/1644)
-	A missing Spring 4 Cache method added to hazelcast-spring package (namely `public T get(Object key, Class type)`). [[#1627]](https://github.com/hazelcast/hazelcast/issues/1627)
-	When eviction tasks are canceled, `scheduledExecutorService` is not cleaned. [[#1595]](https://github.com/hazelcast/hazelcast/issues/1595)
-	`storeAll()` with new value for the same key should not be executed until any previous `storeAll()` operations with the same key are not completed. [[#1592]](https://github.com/hazelcast/hazelcast/issues/1592)
-	When using native client to interact with Hazelcast cluster, some JMX MBean attribute values on cluster nodes are not set/updated. [[#1576]](https://github.com/hazelcast/hazelcast/issues/1576)
-	`IMap.getAll(keys)` method does not read from near cache. [[#1532]](https://github.com/hazelcast/hazelcast/issues/1532)
-	Near Cache *cache-local-entries* attribute is missing in `hazelcast-spring-3.2` XSD. [[#1524]](https://github.com/hazelcast/hazelcast/issues/1524)
-	Exception while executing script in OpenJDK 8. [[#1518]](https://github.com/hazelcast/hazelcast/issues/1518)
-	Infinite waiting on merge operations when cluster shuts down. [[#1504]](https://github.com/hazelcast/hazelcast/issues/1504)
-	Client side socket interceptor is not needed to be MemberSocketInterceptor. [[#1444]](https://github.com/hazelcast/hazelcast/issues/1444)
-	Near cache on the local node should be enabled if its InMemoryFormat is different from that of the map. [[#1438]](https://github.com/hazelcast/hazelcast/issues/1438)
-	Async EntryProcessor does not deserialize the value before it is called back. [[#1433]](https://github.com/hazelcast/hazelcast/issues/1433)
-	A submitted task cannot be canceled via the native client. [[#1394]](https://github.com/hazelcast/hazelcast/issues/1394)
-	`executeOnKeys(keys,entryprocessor)` introduced on IMap. With this feature entry processor can be run on a set of keys. [[#1339]](https://github.com/hazelcast/hazelcast/issues/1339)
-	FINEST logging should be guarded where appropriate. [[#1332]](https://github.com/hazelcast/hazelcast/issues/1332)
-	False errors reported in Eclipse due to schema definition. [[#1330]](https://github.com/hazelcast/hazelcast/issues/1330)
-	Index based operations are not synchronized with partition changes. [[#1297]](https://github.com/hazelcast/hazelcast/issues/1297)
-	Management Center: InvocationTargetException in Tomcat console when a node is started and then stopped. [[#1267]](https://github.com/hazelcast/hazelcast/issues/1267)
-	The system property `hazelcast.map.load.chunk.size` is being ignored in Hazelcast 3.1. [[#1110]](https://github.com/hazelcast/hazelcast/issues/1110)
-	Master should fire repartitioning after getting confirmation from nodes. [[#1058]](https://github.com/hazelcast/hazelcast/issues/1058)
-	SqlPredicate does not Implement equals/hashCode. [[#960]](https://github.com/hazelcast/hazelcast/issues/960)
-	`DelegatingFuture.isDone` seems to always return false until the method `DelegatingFuture.get` is called. [[#850]](https://github.com/hazelcast/hazelcast/issues/850)
-	Predicate support for entry processor. [[#826]](https://github.com/hazelcast/hazelcast/issues/826)



**RC2 Fixes**

-	`ClientService.getConnectedClients` returns all end points [[#1883]](https://github.com/hazelcast/hazelcast/issues/1883).
-	MultiMap is throwing `ConcurrentModificationExceptions` [[#1882]](https://github.com/hazelcast/hazelcast/issues/1882).
-	`executorPoolSize` field of ClientConfig cannot be configured using XML [[#1867]](https://github.com/hazelcast/hazelcast/issues/1867).
-	Partition processing cannot be postponed [[#1856]](https://github.com/hazelcast/hazelcast/pull/1856).
-	Memory leak at client endpoints [[#1842]](https://github.com/hazelcast/hazelcast/pull/1842).
-	Errors related to management center configuration on startup [[#1821]](https://github.com/hazelcast/hazelcast/pull/1821).
-	XML parsing error by client [[#1818]](https://github.com/hazelcast/hazelcast/pull/1818).
-	`ClientReAuthOperation` cannot return response without call ID [[#1816]](https://github.com/hazelcast/hazelcast/issues/1816).
-	`MemberAttributeOperationType` should be introduced to remove the dependency to `MapOperationType` [[#1811]](https://github.com/hazelcast/hazelcast/pull/1811).
-	Entry listener removal from MultiMap [[#1810]](https://github.com/hazelcast/hazelcast/pull/1810).
-	Change `DefaultRecordStore#removeAll` to keep "key objects to delete" as a list, not a set [[#1795]](https://github.com/hazelcast/hazelcast/issues/1795).

**RC1 Fixes**

-	*TransactionalMap* does not support `put(K,V,long,TimeUnit)` [[#1718]](https://github.com/hazelcast/hazelcast/issues/1718).
-	Entry is not removed from backup store when it is deleted using entry processor [[#1687]](https://github.com/hazelcast/hazelcast/issues/1687).
-	Possibility of losing data when MapStore takes a long time [[#1644]](https://github.com/hazelcast/hazelcast/issues/1644).
-	When eviction tasks are cancelled, `scheduledExecutorService` should be cleaned	[[#1595]](https://github.com/hazelcast/hazelcast/issues/1595).
-	A fix related to *StoreAll* is needed in a write-behind scenario [[#1592]](https://github.com/hazelcast/hazelcast/issues/1592).
-	Update problem at map statistics [[#1576]](https://github.com/hazelcast/hazelcast/issues/1576).
-	Exception while executing script in OpenJDK 8 [[#1518]](https://github.com/hazelcast/hazelcast/issues/1518).
-	StackOverflowError at `AndResultSet` [[#1501]](https://github.com/hazelcast/hazelcast/issues/1501).
-	Near Cache using `InMemoryFormat.OBJECT` also for local node [[#1438]](https://github.com/hazelcast/hazelcast/issues/1438).
-	Async entry processor is not deserializing the value before returning [[#1433]](https://github.com/hazelcast/hazelcast/issues/1433).
-	Distributed Executor; *Future Cancel* is not working [[#1394]](https://github.com/hazelcast/hazelcast/issues/1394).
-	`HazelcastInstanceFactory$InstanceFuture.get()` never returns when `newHazelcastInstance()` method fails/throws exception [[#1253]](https://github.com/hazelcast/hazelcast/issues/1253).
-	Changes for *Vertx* on Openshift [[#1176]](https://github.com/hazelcast/hazelcast/pull/1176).
-	Serialization should be performed after database interaction for MapStore [[#1115]](https://github.com/hazelcast/hazelcast/issues/1115).
-	System property related to chunk size is passed over in Hazelcast 3.1 [[#1110]](https://github.com/hazelcast/hazelcast/issues/1110).
-	Map backups lack eviction of some specific data [[#1085]](https://github.com/hazelcast/hazelcast/issues/1085).
-	`DelegatingFuture.isDone` always returns false until get is called [[#850]](https://github.com/hazelcast/hazelcast/issues/850).
-	Predicate support for entry processor [[#826]](https://github.com/hazelcast/hazelcast/issues/826).
-	Full replication of Maps should be performed [[#360]](https://github.com/hazelcast/hazelcast/issues/360).


### Known Issues & Workarounds

Please see [here](https://github.com/hazelcast/hazelcast/issues?labels=&milestone=43&page=1&state=open) for the known issues.









