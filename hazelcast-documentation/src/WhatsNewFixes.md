
### Fixes

**3.5 Fixes**

This section lists issues solved for **Hazelcast 3.5** release.

- There have been Hazelcast AWS exceptions after the version of AWS signer had changed (from v2 to v4) [[#4571]](https://github.com/hazelcast/hazelcast/issues/4571).
- In the declarative configuration; when a variable is used to specify the value of an element or attribute,
Hazelcast ignores the strings that come before the variable
[[#4533]](https://github.com/hazelcast/hazelcast/issues/4533).
- In Hazelcast 3.3.4, `FinalizeJoinOperation` times out if the method `MapStore.loadAllKeys()` takes more than 5
seconds [[#4348]](https://github.com/hazelcast/hazelcast/issues/4348).
- Owner connection `read()` forever [[#3401]](https://github.com/hazelcast/hazelcast/issues/3401).
