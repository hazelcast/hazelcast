
### Fixes

**3.4-RC1 Fixes**

This section lists issues solved for **Hazelcast 3.4-RC1** (Release Candidate 1) release.

- Hazelcast Enterprise Native Memory operations should be updated in relation with the Hazelcast sync listener changes [[#4089]](https://github.com/hazelcast/hazelcast/issues/4089).
- The completion listener (JCache) relies on event ordering but if the completion listener is registered in another node then event ordering is not guaranteed [[#4073]](https://github.com/hazelcast/hazelcast/issues/4073).
- Event packets sent to the client do not have "partitionId" [[#4071]](https://github.com/hazelcast/hazelcast/issues/4071).
- AWS joiner classname should be fixed since EC2 discovery is not working after the restructure [[#4025]](https://github.com/hazelcast/hazelcast/issues/4025).
- Getting a pre-configured Cache instance is not working as expected [[#4009]](https://github.com/hazelcast/hazelcast/issues/4009).
- Bounded Queue section in the Reference Manual is unclear and wrong [[#3995]](https://github.com/hazelcast/hazelcast/issues/3995).
- The method `checkFullyProcessed` of MapReduce throws null pointer exception. The reason may be that multiple threads attempt to start the final processing state in the JobSupervisor [[#3952]](https://github.com/hazelcast/hazelcast/issues/3952).



**3.4-EA Fixes**

This section lists issues solved for **Hazelcast 3.4-EA** (Early Access) release.


- Merge operation after a split brain syndrome does not guarantee that the merging is over [[#3863]](https://github.com/hazelcast/hazelcast/issues/3863).
- A Hazelcast client should not be a `HazelcastInstance`. It should be a "factory" and this factory should be able to shut down Hazelcast clients. [[#3781]](https://github.com/hazelcast/hazelcast/issues/3781).
- When a client with near cache configuration enabled is shut down, `RejectedExecutionException` is thrown [[#3669]](https://github.com/hazelcast/hazelcast/issues/3669).
- The method `MultiMap.get()` returns `collection`, but this method should return the correct collection type (`Set` or `List`) [[#3214]](https://github.com/hazelcast/hazelcast/issues/3214).
- In Hazelcast `IMap` and `TransactionalMap`, read-only operations such as `get()`, `containsKey()`, `keySet()`, and `containsValue()` break the transaction atomicity [[#3191]](https://github.com/hazelcast/hazelcast/issues/3191).
- Documentation should clearly list features of and differences between native clients [[#2385]](https://github.com/hazelcast/hazelcast/issues/2385).
- Support for Log4j 2.x has been implemented [[#2345]](https://github.com/hazelcast/hazelcast/issues/2345).
- Sections of Hazelcast configuration should be able to be imported so that these sections can be shared between other Hazelcast configurations [[#406]](https://github.com/hazelcast/hazelcast/issues/406).