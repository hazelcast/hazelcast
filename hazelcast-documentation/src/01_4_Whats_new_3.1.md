

## What's new in 3.1?

**Elastic Memory (Enterprise Edition Only):**

- Elastic Memory is now available. For additional info see [Elastic Memory](#elastic-memory) section.

**Security (Enterprise Edition Only):**

- Hazelcast Security is now available. For additional info see [Security](#security) section.

**JCA**

- Hazelcast JCA integration is back. For additional info see [J2EE Integration](#j2ee-integration) section.

**Controlled Partitioning**

- Controlled Partitioning is the ability to control the partition of certain DistributedObjects like the IQueue, IAtomicLong or ILock. This will make collocating related data easier. For additional info see our blog post: Controlled Partitioning
- Hazelcast map also supports custom partitioning strategies. A PartitioningStrategy can be defined in map configuration.

**Map**

- TransactionalMap now supports `keySet()`, `keySet(predicate)`, `values()` and `values(predicate)` methods.
- Eviction based on `USED_HEAP_PERCENTAGE` or `USED_HEAP_SIZE` now takes account real heap memory size consumed by map.
- `SqlPredicate` now supports '\' as escape character. See [MapQuery](#query) section for more info.
- `SqlPredicate` now supports regular expressions using `REGEX` keyword. For example; `map.values(new SqlPredicate("name REGEX .*earl$"))` See [MapQuery](#query) section for more info.

**Queue**

- Hazelcast queue now supports `QueueStoreFactory` that will be used to create custom `QueueStore`s for persistent queues. `QueueStoreFactory` is similar to map's MapStoreFactory.
- TransactionalQueue now supports `peek()` and `peek(timeout, timeunit)` methods.

**Client**

- Client now has SSL support. See [SSL](#ssl) section.
- Client also supports custom socket implementations using `SocketFactory` API. A custom socket factory can be defined in `ClientConfig`; `clientConfig.getSocketOptions().setSocketFactory(socketFactory)`.

**Other Features**

- Hazelcast `IList` and `ISet` now have their own configurations. They can be configured using config API, xml and Spring. See ListConfig and SetConfig classes.
- HazelcastInstance.shutdown() method added back.
- OSGI compatibility is improved significantly.

**Fixed Issues**

- [Version 3.1](https://github.com/hazelcast/hazelcast/issues?milestone=23&state=closed)
- [Version 3.1.1](https://github.com/hazelcast/hazelcast/issues?milestone=30&state=closed)
- [Version 3.1.2](https://github.com/hazelcast/hazelcast/issues?milestone=33&state=closed)




