
### Fixes

**3.5 Fixes**

This section lists issues solved for **Hazelcast 3.5** release.

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
