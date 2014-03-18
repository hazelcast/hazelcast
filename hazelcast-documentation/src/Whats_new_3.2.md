# What's New in Hazelcast 3.2

## Release Notes
### RC2 Fixed Issues
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

### RC1 Fixed Issues

-	*TransactionalMap* does not support `put(K,V,long,TimeUnit)` [[#1718]](https://github.com/hazelcast/hazelcast/issues/1718).
-	Entry is not removed from backup store when it is deleted using entry processor [[#1687]](https://github.com/hazelcast/hazelcast/issues/1687).
-	Possibility of losing data when MapStore takes a long time [[#1644]](https://github.com/hazelcast/hazelcast/issues/1644).
-	When eviction tasks are cancelled, `scheduledExecutorService` should be cleaned	[[#1595]](https://github.com/hazelcast/hazelcast/issues/1595).
-	A fix related to *StoreAll* is needed in a write-behind scenario [[#1592]](https://github.com/hazelcast/hazelcast/issues/1592).
-	Update problem at map statistics [[#1576]](https://github.com/hazelcast/hazelcast/issues/1576).
-	Exception while executing script in OpenJDK 8 [[#1518]](https://github.com/hazelcast/hazelcast/issues/1518).
-	StackOverflowError at `AndResultSet` [[#1501]](https://github.com/hazelcast/hazelcast/issues/1501).
-	Allow tagging of members for `IExecutorService` [[#1457]](https://github.com/hazelcast/hazelcast/issues/1457).
-	Near Cache using `InMemoryFormat.OBJECT` also for local node [[#1438]](https://github.com/hazelcast/hazelcast/issues/1438).
-	Async entry processor is not deserializing the value before returning [[#1433]](https://github.com/hazelcast/hazelcast/issues/1433).
-	Distributed Executor; *Future Cancel* is not working [[#1394]](https://github.com/hazelcast/hazelcast/issues/1394).
-	`HazelcastInstanceFactory$InstanceFuture.get()` never returns when `newHazelcastInstance()` method fails/throws exception [[#1253]](https://github.com/hazelcast/hazelcast/issues/1253).
-	Changes for *Vertx* on Openshift [[#1176]](https://github.com/hazelcast/hazelcast/pull/1176).
-	Serialization should be performed after database interaction for MapStore [[#1115]](https://github.com/hazelcast/hazelcast/issues/1115).
-	System property related to chunk size is passed over in Hazelcast 3.1 [[#1110]](https://github.com/hazelcast/hazelcast/issues/1110).
-	Map backups lack eviction of some specific data [[#1085]](https://github.com/hazelcast/hazelcast/issues/1085).
-	`getForUpdate()` for TransactionalMap [[#1033]](https://github.com/hazelcast/hazelcast/issues/1033).
-	`DelegatingFuture.isDone` always returns false until get is called [[#850]](https://github.com/hazelcast/hazelcast/issues/850).
-	Predicate support for entry processor [[#826]](https://github.com/hazelcast/hazelcast/issues/826).
-	Full replication of Maps should be performed [[#360]](https://github.com/hazelcast/hazelcast/issues/360).
-	Near cache statistics [[#30]](https://github.com/hazelcast/hazelcast/issues/30).







