# What's New in Hazelcast 3.3



## Release Notes

### New Features
This section provides the new features introduced with Hazelcast 3.3 release. 

- Heartbeat for Java client: Before this release, a Java client could not detect a node as dead, if the client is not trying to connect to it. With this heartbeat feature, each node will be pinged periodically. If no response is returned from a node, it will be deemed as dead. Main goal of this feature is to decrease the time for detection of dead (disconnected) nodes by Java clients, so that the user operations will be sent directly to a responsive one. For more information, please see [Client Properties](#client-properties).
- Tomcat 6 and 7 Web Sessions Clustering: Please see [Session Replication](#session-replication).
- Replicated Map implemented: Please see [Replicated Map](#replicated-map-beta)
- WAN Replication improved: Added configurable replication queue size [WAN Replication Queue Size](#wan-replication-queue-size).
- Data Aggregation implemented: Added common data aggregations, please find [Aggregators](#aggregators) documentation.
- EvictAll and LoadAll features for IMap: `evictAll` and `loadAll` methods have been introduced to be able to evict all entries except the locked ones and that loads all or a set of keys from a configured map store, respectively.
- JCache implementation introduced: Please see [Hazelcast JCache Implementation](#hazelcast-jcache-implementation) for details.

### Fixes

This section lists issues solved for Hazelcast 3.3 release.

- `java.lang.NullPointerException` is thrown when publishing an event in ClientEndPointImpl [[#3407]](https://github.com/hazelcast/hazelcast/issues/3407).
- The `entryUpdated()` callback of a listener during a transaction always has a null `oldValue` in the EntryEvent [[#3406]](https://github.com/hazelcast/hazelcast/issues/3406).
- Hazelcast write-behind with `map.replace()` stores replaced items [[#3386]](https://github.com/hazelcast/hazelcast/issues/3386).
- XAResource's `setTransactionTimeout()` method is not correctly implemented [[#3384]](https://github.com/hazelcast/hazelcast/issues/3384).
- Hazelcast web session replication filter may die if response committed [[#3360]](https://github.com/hazelcast/hazelcast/issues/3360).
- Resource adapter state is never reset to `isStarted == false`, resulting in errors down the line [[#3350]](https://github.com/hazelcast/hazelcast/issues/3350).
- Parallel execution of `MapStore#store` method for the same key triggered by `IMap#flush` [[#3338]](https://github.com/hazelcast/hazelcast/issues/3338).
- When offering null argument in queue throws an exception but it adds null argument to collection, then `addAll()` performed on this list does not throw an exception [[#3330]](https://github.com/hazelcast/hazelcast/issues/3330).
- `java.io.FileNotFoundException` thrown by MapLoaderTest [[#3324]](https://github.com/hazelcast/hazelcast/issues/3324).
- MapMaxSizeTest Stabilizer test with SoftKill [[#3291]](https://github.com/hazelcast/hazelcast/issues/3291).
- Incompatible Spring and Hazelcast configuration XSDs [[#3275]](https://github.com/hazelcast/hazelcast/issues/3275).
- `ExpirationManager` partition sorting can fail [[#3271]](https://github.com/hazelcast/hazelcast/issues/3271).
- Configuration validation is broken [[#3257]](https://github.com/hazelcast/hazelcast/issues/3257).
- Code Samples for Spring Security and WebFilter Integration [[#3252]](https://github.com/hazelcast/hazelcast/issues/3252).
- WebFilter Test Cases are slow [[#3250]](https://github.com/hazelcast/hazelcast/issues/3250).
- Management Center and Weblogic Deployment Problem [[#3247]](https://github.com/hazelcast/hazelcast/issues/3247).
- Enabling Multicast and TCP/IP node discovery methods freeze the instances [[#3246]](https://github.com/hazelcast/hazelcast/issues/3246).
- `PagingPredicate.getAnchor` does not return the correct value [[#3241]](https://github.com/hazelcast/hazelcast/issues/3241).
- `getOldValue` and `getValue` returns the same value when removing item from IMap [[#3198]](https://github.com/hazelcast/hazelcast/issues/3198).
- MapTransactionContextTest: member SoftKill and then HazelcastSerializationException and IegalStateException: Nested are thrown [[#3196]](https://github.com/hazelcast/hazelcast/issues/3196).
- `IMap.delete()` should not call `MapLoader.load()`[[#3178]](https://github.com/hazelcast/hazelcast/issues/3178).
- 3.3-RC3+: NPE in the method `connectionMarkedAsNotResponsive` [[#3169]](https://github.com/hazelcast/hazelcast/issues/3169).
- `WebFilter.HazelcastHttpSession.isNew()` does not check the Hazelcast Session Cache [[#3132]](https://github.com/hazelcast/hazelcast/issues/3132).
- Hazelcast Spring XSD files are not version agnostic [[#3131]](https://github.com/hazelcast/hazelcast/issues/3131).
- `ClassCastException: java.lang.Integer` cannot be cast to `java.lang.String` Query [[#3091]](https://github.com/hazelcast/hazelcast/issues/3091).
- Predicate returns a value not matching the predicate [[#3090]](https://github.com/hazelcast/hazelcast/issues/3090).
- Modifications made by Entry Processor are lost in 3.3-RC-2 [[#3062]](https://github.com/hazelcast/hazelcast/issues/3062).
- Hazelcast Session Clustering with Spring Security Problem [[#3049]](https://github.com/hazelcast/hazelcast/issues/3049).
- PagingPredicate returning duplicated elements results in an infinite loop [[#3047]](https://github.com/hazelcast/hazelcast/issues/3047).
- `expirationTime` on EntryView is not set [[#3038]](https://github.com/hazelcast/hazelcast/issues/3038).
- `BasicRecordStoreLoader` cannot handle retry responses [[#3033]](https://github.com/hazelcast/hazelcast/issues/3033). 
- Short `await()` on condition of contended lock causes IllegalStateException [[#3025]](https://github.com/hazelcast/hazelcast/issues/3025). 
- Indices and Comparable<T>: not documented [[#3024]](https://github.com/hazelcast/hazelcast/issues/3024). 
- Marking Heartbeat as healthy is too late [[#3014]](https://github.com/hazelcast/hazelcast/issues/3014).
- 3.3-RC2: `IMap#keySet` triggers value deserialization [[#3008]](https://github.com/hazelcast/hazelcast/issues/3008).
- `map.destroy()` throws DistributedObjectDestroyedException [[#3001]](https://github.com/hazelcast/hazelcast/issues/3001).
- Stabilizer tests Final profile, Xlarge cluster OperationTimeoutException [[#2999]](https://github.com/hazelcast/hazelcast/issues/2999).
- `com.hazelcast.jca.HazelcastConnection::getExecutorService` returns plain ExecutorService [[#2986]](https://github.com/hazelcast/hazelcast/issues/2986).
- Serialization NPE in MapStoreTest stabilizer, 3.3-RC3-SNAPSHOT [[#2985]](https://github.com/hazelcast/hazelcast/issues/2985).
- Bug with `IMap.getAll()` [[#2982]](https://github.com/hazelcast/hazelcast/issues/2982).
- Client deadlock on single core machines [[#2971]](https://github.com/hazelcast/hazelcast/issues/2971).
- Retrieve number of futures in loop in calling thread [[#2964]](https://github.com/hazelcast/hazelcast/issues/2964).


**RC2 Fixes**

This section lists issues solved for Hazelcast 3.3-RC2 release.

-	NPE exception in MapStoreTest [[#2956]](https://github.com/hazelcast/hazelcast/issues/2956).
-	All read operations of map should respect expired keys [[#2946]](https://github.com/hazelcast/hazelcast/issues/2946).
-	WebFilter does not clean up timed-	out sessions [[#2930]](https://github.com/hazelcast/hazelcast/issues/2930).
-	Fixes data loss issue when partition table is being synced and a node is gracefully shutdown [[#2908]](https://github.com/hazelcast/hazelcast/issues/2908).
-	`evictAll` should flush to staging area [#2969](https://github.com/hazelcast/hazelcast/issues/2969).
-	MapStoreConfig; implementation instance is not set, when configured via XML [[#2898]](https://github.com/hazelcast/hazelcast/issues/2898).
-	Fixed `AddSessionEntryProcessor` [[#2955]](https://github.com/hazelcast/hazelcast/issues/2955).
-   Added `StripedExecutor` to WanReplicationService [[#2947]](https://github.com/hazelcast/hazelcast/issues/2947).
-	Data loss fix in *hazelcast-wm* module [[#2927]](https://github.com/hazelcast/hazelcast/issues/2927).
-	Fix leaking empty concurrent hashmaps [[#2929]](https://github.com/hazelcast/hazelcast/issues/2929).
-	Configured event queue capacity [[#2924]](https://github.com/hazelcast/hazelcast/issues/2924).
-	Close the owner connection if heartbeat timeout when client is smart [[#2916]](https://github.com/hazelcast/hazelcast/issues/2916).
-	Client closes owner connection when a connection to the same address is closed [[#2921]](https://github.com/hazelcast/hazelcast/issues/2921).
-	Set application buffer size to not exceed `tls` record size [[#2914]](https://github.com/hazelcast/hazelcast/issues/2914).
-	LocalMapStats does not record stats about locked entries in 3.x [[#2876]](https://github.com/hazelcast/hazelcast/issues/2876).
-	Concurrency security interceptor [[#2874]](https://github.com/hazelcast/hazelcast/issues/2874).
-	Client hangs during split, if split occurs due to network error [[#2850]](https://github.com/hazelcast/hazelcast/issues/2850).
-	Network connection loss does not release lock [[#2818]](https://github.com/hazelcast/hazelcast/issues/2818).
-	It is not possible to copy the link from *http://hazelcast.org/download/* and run `wget` on it [[#2814]](https://github.com/hazelcast/hazelcast/issues/2814).










