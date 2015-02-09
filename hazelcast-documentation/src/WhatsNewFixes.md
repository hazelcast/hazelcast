
### Fixes

**3.5 Fixes**

This section lists issues solved for **Hazelcast 3.5** release.


- In Hazelcast 3.3.4, `FinalizeJoinOperation` times out if the method `MapStore.loadAllKeys()` takes more than 5 seconds [[#4348]](https://github.com/hazelcast/hazelcast/issues/4348).
- Owner connection `read()` forever [[#3401]](https://github.com/hazelcast/hazelcast/issues/3401).
