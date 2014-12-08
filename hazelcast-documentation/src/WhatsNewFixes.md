
### Fixes

**3.4 Fixes**

This section lists issues solved for **Hazelcast 3.4** release.

- Deadlock happens in MapReduce implementation when there is a high load on the system. The issue has been solved by offloading Distributed MapReduce result collection to the async executor [[#4238]](https://github.com/hazelcast/hazelcast/issues/4238).
- When the class `ClientExecutorServiceSubmitTest.java` is compiled using the Eclipse compiler, it gives a compile error: "*The method submit(Runnable, ExecutionCallback) is ambiguous for the type IExecutorService*". The reason is that the `IExecutorService.java` class does not have some generics. The issue has been solved by adding these missing generics to the `IExecutorService.java` class [[#4234]](https://github.com/hazelcast/hazelcast/issues/4234).
- JCache declarative listener registration does not work [[#4215]](https://github.com/hazelcast/hazelcast/issues/4215).
- JCache evicts the records which are not expired yet. To solve this issue, the `clear` method should be removed that runs when the size is smaller than the minimum eviction element count (`MIN_EVICTION_ELEMENT_COUNT`) [[#4124]](https://github.com/hazelcast/hazelcast/issues/4124).
- Hazelcast Enterprise Native Memory operations should be updated in relation with the Hazelcast sync listener changes [[#4089]](https://github.com/hazelcast/hazelcast/issues/4089).
- The completion listener (JCache) relies on event ordering but if the completion listener is registered in another node then event ordering is not guaranteed [[#4073]](https://github.com/hazelcast/hazelcast/issues/4073).
- AWS joiner classname should be fixed since EC2 discovery is not working after the restructure [[#4025]](https://github.com/hazelcast/hazelcast/issues/4025).
- If an IMap has a near cache configured, accessing the near cache via the method `get(key)` does not count as an access to the underlying IMap. The near cache has its own `max-idle-seconds` element. However, if an entry is expired/evicted in the IMap, it also causes a near cache removal operation for the entry regardless of the `max-idle-seconds` of that entry in the near cache. The entry expires and is evicted even if the near cache is being hit constantly. When a near cache is hit, the underlying map should reset the idle time for that key [[#4016]](https://github.com/hazelcast/hazelcast/issues/4016).
- Getting a pre-configured Cache instance is not working as expected [[#4009]](https://github.com/hazelcast/hazelcast/issues/4009).
- Bounded Queue section in the Reference Manual is unclear and wrong [[#3995]](https://github.com/hazelcast/hazelcast/issues/3995).
- The method `checkFullyProcessed` of MapReduce throws null pointer exception. The reason may be that multiple threads attempt to start the final processing state in the JobSupervisor [[#3952]](https://github.com/hazelcast/hazelcast/issues/3952).
- Merge operation after a split brain syndrome does not guarantee that the merging is over [[#3863]](https://github.com/hazelcast/hazelcast/issues/3863).
- When a client with near cache configuration enabled is shut down, `RejectedExecutionException` is thrown [[#3669]](https://github.com/hazelcast/hazelcast/issues/3669).
- In Hazelcast `IMap` and `TransactionalMap`, read-only operations such as `get()`, `containsKey()`, `keySet()`, and `containsValue()` break the transaction atomicity [[#3191]](https://github.com/hazelcast/hazelcast/issues/3191).
- Documentation should clearly list features of and differences between native clients [[#2385]](https://github.com/hazelcast/hazelcast/issues/2385).
- Sections of Hazelcast configuration should be able to be imported so that these sections can be shared between other Hazelcast configurations [[#406]](https://github.com/hazelcast/hazelcast/issues/406).