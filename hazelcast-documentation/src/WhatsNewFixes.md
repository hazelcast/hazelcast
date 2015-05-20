
### Fixes

**3.5 Fixes**

This section lists issues solved for **Hazelcast 3.5** release.

- When the method `finalizeCombine()` is used, Hazelcast throws `NullPointerException` [[#5283]](https://github.com/hazelcast/hazelcast/issues/5283).
- `WanBatchReplication` causes `OutOfMemoryException` when the default value for WAN Replication Batch Size (50) is used [[#5280]](https://github.com/hazelcast/hazelcast/issues/5280).
- When testing Hazelcast, it does not start as an OSGI bundle. After the OSGI package was refactored, the dynamic class loading of the Script engine was missed [[#5274]](https://github.com/hazelcast/hazelcast/issues/5274).
- The test for unbounded return values runs forever with the new client implementation [[#5230]](https://github.com/hazelcast/hazelcast/issues/5230).
- The method `putTransient` actuated the MapStore unexpectedly in an environment with multiple instances [[#5225]](https://github.com/hazelcast/hazelcast/issues/5225).
- The method `removeAttribute` will prevent any updates by the method `setAttribute` in the deferred write mode [[#5186]](https://github.com/hazelcast/hazelcast/issues/5186).
- Backward compatibility of eviction configuration for cache is broken since `CacheEvictionConfig` class was renamed to `EvictionConfig` for general usage [[#5180]](https://github.com/hazelcast/hazelcast/issues/5180).
- When host names are not registered in DNS or in `/etc/hosts` and the members are configured manually with IP addresses and while one node is running, a second node joins to the cluster 5 minutes after it started [[#5072]](https://github.com/hazelcast/hazelcast/issues/5072).
- The `SlowOperationDTO.operation` shows only the class name, not the package. This can lead to ambiguity and the actual class cannot be tracked [[#5041]](https://github.com/hazelcast/hazelcast/issues/5041).
- There is no documentation comment for the `MessageListener` interface of ITopic [[#5019]](https://github.com/hazelcast/hazelcast/issues/5019).
- The method `InvocationFuture.isDone` returns `true` as soon as there is a response including `WAIT_RESPONSE`. However, `WAIT_RESPONSE` is an intermediate response, not a final one [[#5002]](https://github.com/hazelcast/hazelcast/issues/5002).
- The method `InvocationFuture.andThen` does not deal with the null response correctly [[#5001]](https://github.com/hazelcast/hazelcast/issues/5001).
- `CacheCreationTest` fails due to the multiple `TestHazelcastInstanceFactory` creations in the same test [[#4987]](https://github.com/hazelcast/hazelcast/issues/4987).
- When Spring dependency is upgraded to 4.1.x, an exception related to the `putIfAbsent` method is thrown [[#4981]](https://github.com/hazelcast/hazelcast/issues/4981).
- Hazelcast Client code allows to use the value *0* for the `connectionAttemptLimit` property which internally results in `int.maxValue`. However, the XSD of the Hazelcast Spring configuration requires it to be at least 1 [[#4967]](https://github.com/hazelcast/hazelcast/issues/4967).
- When upgraded to 3.5-SNAPSHOT for testing, Hazelcast project gives a warning that mentions a missing configuration for `hazelcastmq.txn-topic` [[#4790]](https://github.com/hazelcast/hazelcast/issues/4790).
- When Hazelcast is installed using Maven in Windows environment, the test `XmlConfigImportVariableReplacementTest` fails [[#4758]](https://github.com/hazelcast/hazelcast/issues/4758).
- When a request cannot be executed due to a problem (connection error, etc.), if the operation redo is enabled, request is retried. Retried operations are offloaded to an executor, but after offloading, the user thread still tries to retry the request. This causes anomalies like operations being executed twice or operation responses being handled incorrectly [[#4693]](https://github.com/hazelcast/hazelcast/issues/4693).
- The `size()` method for a replicated map should return `0` when the entry is removed [[#4666]](https://github.com/hazelcast/hazelcast/issues/4666).
- There have been Hazelcast AWS exceptions after the version of AWS signer had changed (from v2 to v4) [[#4571]](https://github.com/hazelcast/hazelcast/issues/4571).
- In the declarative configuration; when a variable is used to specify the value of an element or attribute,
Hazelcast ignores the strings that come before the variable
[[#4533]](https://github.com/hazelcast/hazelcast/issues/4533).
- In Hazelcast 3.3.4, `FinalizeJoinOperation` times out if the method `MapStore.loadAllKeys()` takes more than 5
seconds [[#4348]](https://github.com/hazelcast/hazelcast/issues/4348).
- Owner connection `read()` forever [[#3401]](https://github.com/hazelcast/hazelcast/issues/3401).

