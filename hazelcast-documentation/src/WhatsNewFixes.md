
### Fixes

**3.5.1 Fixes**

This section lists issues solved for Hazelcast 3.5.1 release.

- Hazelcast Management Center uses `UpdateMapConfigOperation` to update map configurations. This operation simply replaces the map configuration of the related map container. However, this replacement has no effect for `maxIdleSeconds` and `timeToLiveSeconds` properties of the map configuration since they are not used in the map container directly. They are assigned to the final variables during map container creation and never touched again [[#5593]](https://github.com/hazelcast/hazelcast/issues/5593).
- Destroying a map just after creating it produces double create/destroy events for `DistributedObjectListener` [[#5592]](https://github.com/hazelcast/hazelcast/issues/5592).
- Map does not allow changing its maximum size, TTL and maximum idle properties. However, these fields are editable in the "Map Config" popup of Management Center. These fields should be disabled to prevent misguiding [[#5591]](https://github.com/hazelcast/hazelcast/issues/5591).
- Map is destroyed using `IMap.destroy()` but then it is immediately recreated [[#5554]](https://github.com/hazelcast/hazelcast/issues/5554).
- 


<br><br>

**3.5 Fixes**

This section lists issues solved for Hazelcast 3.5 release.

- Operation timeout mechanism is not working [[#5468]](https://github.com/hazelcast/hazelcast/issues/5468).
- `MapLoader` exception is not logged: Exception should be logged and propagated back to the client that triggered the loading of the map [[#5430]](https://github.com/hazelcast/hazelcast/issues/5430).
- Replicated Map documentation page does not mention that it is in the beta stage [[#5424]](https://github.com/hazelcast/hazelcast/issues/5424).
- The method `XAResource.rollback()` should not need the transaction to be in the prepared state when called from another member/client [[#5401]](https://github.com/hazelcast/hazelcast/issues/5401).
- The method `XAResource.end()` should not need to check `threadId` [[#5400]](https://github.com/hazelcast/hazelcast/issues/5400).
- The method `IList::remove()` should publish the event `REMOVED` [[#5386]](https://github.com/hazelcast/hazelcast/issues/5386).
- `IllegalStateException` with wrong partition is thrown when the method `IMap::getOperation()` is invoked [[#5341]](https://github.com/hazelcast/hazelcast/issues/5341).
- `WrongTarget` warnings appear in the log since the operations are not sent to the replicas when a map has no backups [[#5324]](https://github.com/hazelcast/hazelcast/issues/5324).
- When the method `finalizeCombine()` is used, Hazelcast throws `NullPointerException` [[#5283]](https://github.com/hazelcast/hazelcast/issues/5283).
- `WanBatchReplication` causes `OutOfMemoryException` when the default value for WAN Replication Batch Size (50) is used [[#5280]](https://github.com/hazelcast/hazelcast/issues/5280).
- When testing Hazelcast, it does not start as an OSGI bundle. After the OSGI package was refactored, the dynamic class loading of the Script engine was missed [[#5274]](https://github.com/hazelcast/hazelcast/issues/5274).
- XA Example from Section 11.3.5 in the Reference Manual broken after the latest XA Improvements are committed [[#5273]](https://github.com/hazelcast/hazelcast/issues/5273).
- XA Transaction throws `TransactionException` instead of an `XAException` on timeout [[#5260]](https://github.com/hazelcast/hazelcast/issues/5260).
- The test for unbounded return values runs forever with the new client implementation [[#5230]](https://github.com/hazelcast/hazelcast/issues/5230).
- The new client method `getAsync()` fails with a `NegativeArraySizeException` [[#5229]](https://github.com/hazelcast/hazelcast/issues/5229).
- The method `putTransient` actuated the MapStore unexpectedly in an environment with multiple instances [[#5225]](https://github.com/hazelcast/hazelcast/issues/5225).
- Changes made by the interceptor do not appear in the backup [[#5211]](https://github.com/hazelcast/hazelcast/issues/5211).
- The method `removeAttribute` will prevent any updates by the method `setAttribute` in the deferred write mode [[#5186]](https://github.com/hazelcast/hazelcast/issues/5186).
- Backward compatibility of eviction configuration for cache is broken since `CacheEvictionConfig` class was renamed to `EvictionConfig` for general usage [[#5180]](https://github.com/hazelcast/hazelcast/issues/5180).
- Value passed into `ICompletableFuture.onResponse()` is not deserialized [[#5158]](https://github.com/hazelcast/hazelcast/issues/5158).
- Map Eviction section in the Reference Manual needs more clarification [[#5120]](https://github.com/hazelcast/hazelcast/issues/5120).
- When host names are not registered in DNS or in `/etc/hosts` and the members are configured manually with IP addresses and while one node is running, a second node joins to the cluster 5 minutes after it started [[#5072]](https://github.com/hazelcast/hazelcast/issues/5072).
- The method `OperationService.asyncInvokeOnPartition()` sometimes fails [[#5069]](https://github.com/hazelcast/hazelcast/issues/5069).
- The `SlowOperationDTO.operation` shows only the class name, not the package. This can lead to ambiguity and the actual class cannot be tracked [[#5041]](https://github.com/hazelcast/hazelcast/issues/5041).
- There is no documentation comment for the `MessageListener` interface of ITopic [[#5019]](https://github.com/hazelcast/hazelcast/issues/5019).
- The method `InvocationFuture.isDone` returns `true` as soon as there is a response including `WAIT_RESPONSE`. However, `WAIT_RESPONSE` is an intermediate response, not a final one [[#5002]](https://github.com/hazelcast/hazelcast/issues/5002).
- The method `InvocationFuture.andThen` does not deal with the null response correctly [[#5001]](https://github.com/hazelcast/hazelcast/issues/5001).
- `CacheCreationTest` fails due to the multiple `TestHazelcastInstanceFactory` creations in the same test [[#4987]](https://github.com/hazelcast/hazelcast/issues/4987).
- When Spring dependency is upgraded to 4.1.x, an exception related to the `putIfAbsent` method is thrown [[#4981]](https://github.com/hazelcast/hazelcast/issues/4981).
- HazelcastCacheManager should offer a way to access the underlying cache manager [[#4978]](https://github.com/hazelcast/hazelcast/issues/4978).
- Hazelcast Client code allows to use the value *0* for the `connectionAttemptLimit` property which internally results in `int.maxValue`. However, the XSD of the Hazelcast Spring configuration requires it to be at least 1 [[#4967]](https://github.com/hazelcast/hazelcast/issues/4967).
- Updates from Entry Processor does not take `write-coalescing` into account [[#4967]](https://github.com/hazelcast/hazelcast/issues/4957).
- CachingProvider does not honor custom URI [[#4943]](https://github.com/hazelcast/hazelcast/issues/4943).
- Test for the method `getLocalExecutorStats()` fails spuriously [[#4911]](https://github.com/hazelcast/hazelcast/issues/4911).
- Missing documentation of network configuration for JCache [[#4905]](https://github.com/hazelcast/hazelcast/issues/4905).
- Slow operation detector throws a `NullPointerException` [[#4855]](https://github.com/hazelcast/hazelcast/issues/4855).
- Consider use of `System.nanoTime` in `sleepAtLeast` test code [[#4835]](https://github.com/hazelcast/hazelcast/issues/4835).
- When upgraded to 3.5-SNAPSHOT for testing, Hazelcast project gives a warning that mentions a missing configuration for `hazelcastmq.txn-topic` [[#4790]](https://github.com/hazelcast/hazelcast/issues/4790).
- `ClassNotFoundException` when using WAR classes with JCache API [[#4775]](https://github.com/hazelcast/hazelcast/issues/4775).
- When Hazelcast is installed using Maven in Windows environment, the test `XmlConfigImportVariableReplacementTest` fails [[#4758]](https://github.com/hazelcast/hazelcast/issues/4758).
- When a request cannot be executed due to a problem (connection error, etc.), if the operation redo is enabled, request is retried. Retried operations are offloaded to an executor, but after offloading, the user thread still tries to retry the request. This causes anomalies like operations being executed twice or operation responses being handled incorrectly [[#4693]](https://github.com/hazelcast/hazelcast/issues/4693).
- Client destroys all connections when a reconnection happens [[#4692]](https://github.com/hazelcast/hazelcast/issues/4692).
- The `size()` method for a replicated map should return `0` when the entry is removed [[#4666]](https://github.com/hazelcast/hazelcast/issues/4666).
- `NullPointerException` on the `CachePutBackupOperation` class [[#4660]](https://github.com/hazelcast/hazelcast/issues/4660).
- When removing keys from a MultiMap with a listener, the method `entryRemoved()` is called. In order to get the removed value, one must call the `event.getValue()` instead of `event.getOldValue()` [[#4644]](https://github.com/hazelcast/hazelcast/issues/4644).
- Unnecessary deserialization at the server side when using `Cache.get()` [[#4632]](https://github.com/hazelcast/hazelcast/issues/4632).
- Operation timeout exception during `IMap.loadAllKeys()` [[#4618]](https://github.com/hazelcast/hazelcast/issues/4618).
- There have been Hazelcast AWS exceptions after the version of AWS signer had changed (from v2 to v4) [[#4571]](https://github.com/hazelcast/hazelcast/issues/4571).
- In the declarative configuration; when a variable is used to specify the value of an element or attribute, Hazelcast ignores the strings that come before the variable [[#4533]](https://github.com/hazelcast/hazelcast/issues/4533).
- `LocalRegionCache` cleanup is working wrongly [[#4445]](https://github.com/hazelcast/hazelcast/issues/4445).
- Repeatable-read does not work in a transaction [[#4414]](https://github.com/hazelcast/hazelcast/issues/4414).
- Hazelcast instance name with `Hibernate` still creates multiple instances [[#4374]](https://github.com/hazelcast/hazelcast/issues/4374).
- In Hazelcast 3.3.4, `FinalizeJoinOperation` times out if the method `MapStore.loadAllKeys()` takes more than 5 seconds [[#4348]](https://github.com/hazelcast/hazelcast/issues/4348).
- JCache sync listener completion latch problems: Status of `ICompletableFuture` while waiting for completion latch in the cache must be checked [[#4335]](https://github.com/hazelcast/hazelcast/issues/4335).
- Classloader issue with `javax.cache.api` and Hazelcast 3.3.1 [[#3792]](https://github.com/hazelcast/hazelcast/issues/3792).
- Failed backup operation on transaction commit causes ""Nested transactions are not allowed!" warning [[#3577]](https://github.com/hazelcast/hazelcast/issues/3577).
- Hazelcast Client should not ignore the fact that the XML is for server and should not use default XML feature to connect to `localhost` [[#3256]](https://github.com/hazelcast/hazelcast/issues/3256).
- Owner connection `read()` forever [[#3401]](https://github.com/hazelcast/hazelcast/issues/3401).


