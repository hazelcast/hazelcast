
### Fixes

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