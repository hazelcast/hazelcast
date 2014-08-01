# What's New in Hazelcast 3.3



## Release Notes

### New Features
This section provides the new features introduced with Hazelcast 3.3 release. 

- Heartbeat for Java client: Before this release, a Java client could not detect a node as dead, if the client is not trying to connect to it. With this heartbeat feature, each node will be pinged periodically. If no response is returned from a node, it will be deemed as dead. Main goal of this feature is to decrease the time for detection of dead (disconnected) nodes by Java clients, so that the user operations will be sent directly to a responsive one.
- Tomcat 6 and 7 Web Sessions Clustering: Please see [Session Replication](#session-replication).
- Replicated Map implemented: Please see [Replicated Map](#replicated-map-beta)
- WAN Replication improved: Added configurable replication queue size [WAN Replication Queue Size](#wan-replication-queue-size).
- Data Aggregation implemented: Added common data aggregations, please find [Aggregators](#aggregators) documentation.
- EvictAll and LoadAll features for IMap: `evictAll` and `loadAll` methods have been introduced to be able to evict all entries except the locked ones and that loads all or a set of keys from a configured map store, respectively.

### Fixes

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










