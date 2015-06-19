
### Enhancements

This section lists the enhancements performed for Hazelcast 3.5 release.

- **Eventing System Improvements**: RingBuffer and Reliable Topic structures are introduced.
- **XA Transactions Improvements**: With this improvement, you can now obtain a Hazelcast XA Resource instance through `HazelcastInstance`. For more information, please see [XA Transactions](#xa-transactions).
- **Query**: Predicates are now evaluated using a single thread to prevent a parallel slowdown. This has proven to be a beneficent for most use-cases. You can revert to the old behavior by setting the System Property `hazelcast.query.predicate.parallel.evaluation` to `true`.

The following are the other improvements performed to solve the enhancement issues opened by the Hazelcast customers/team.
 
- While configuring JCache, duration of the `ExpiryPolicy` can be set programmatically but not declaratively [[#5347]](https://github.com/hazelcast/hazelcast/issues/5347).
- Since near cache is currently not supported as embedded but only supported at the client, there is no need for `NearCacheConfig` in `CacheConfig` [[#5215]](https://github.com/hazelcast/hazelcast/issues/5215).
- Support for parametrized test is needed [[#5182]](https://github.com/hazelcast/hazelcast/issues/5182).
- `SlowOperationDetector` should have an option to not log the stacktraces to the log file. There is no need to have the stacktraces written to the normal log file if the Hazelcast Management Center or the performance monitor is being used [[#5043]](https://github.com/hazelcast/hazelcast/issues/5043).
- The batch launcher should include the JCache API [[#4902]](https://github.com/hazelcast/hazelcast/issues/4902).
- There are no Spring tags available for Native Memory configuration [[#4772]](https://github.com/hazelcast/hazelcast/issues/4772).
- In the class `BasicInvocationFuture`, there is no need to create an additional `AtomicInteger` object. It should be
replaced with `AtomicIntegerFieldUpdater` [[#4408]](https://github.com/hazelcast/hazelcast/issues/4408).
- There is no need to use the class `IsStillExecutingOperation` to check if an operation is running locally. One
can directly access the scheduler [[#4407]](https://github.com/hazelcast/hazelcast/issues/4407).
- Configuring NearCache in a Client/Server system only talks about the programmatic configuration of NearCache on the clients. The declarative configuration (XML) of the same is not mentioned [[#4376]](https://github.com/hazelcast/hazelcast/issues/4376).
- XML schema and XML configuration validation is not compliant for AWS configuration [[#4310]](https://github.com/hazelcast/hazelcast/issues/4310).
- The JavaDoc for the methods `KeyValueSource.hasNext/element/key` and `Iterator.hasNext/next` should emphasize the differences between each other, i.e. the state changing behavior should be clarified [[#4218]](https://github.com/hazelcast/hazelcast/issues/4218).
- While migration is in progress, the nodes will have different partition state versions. If the query is running
at that time, it can get results from the nodes at different stages of the migration. By adding partition state
version to the query results, it can be checked whether the migration was happening and the query can be
re-run [[#4206]](https://github.com/hazelcast/hazelcast/issues/4206).
- XML Config Schema does not allow you to set a `SecurityInterceptor`
Implementation [[#4118]](https://github.com/hazelcast/hazelcast/issues/4118).
- Currently, certain types of remote executed calls are stored in the `executingCalls` map. The key
(and value) is a `RemoteCallKey` object. The functionality provided is the ability to ask on the remote side
if an operation is still executing. For a partition-aware operation, this is not needed. When an operation is
scheduled by a partition specific operation thread, the operation can be stored in a volatile field in that
thread [[#4079]](https://github.com/hazelcast/hazelcast/issues/4079).
- The class `TcpIpJoinerOverAWS` fails at the recently launched AWS eu-central-1 region. The reason for the fail is that the region requires v4 signatures [[#3963]](https://github.com/hazelcast/hazelcast/issues/3963).
- API change in `EntryListener` breaks the compatibility with the Camel Hazelcast component [[#3859]](https://github.com/hazelcast/hazelcast/issues/3859).
- The `hazelcast-spring-<`*version*`>.xsd` should include the User Defined Services (SPI) elements and
attributes [[#3565]](https://github.com/hazelcast/hazelcast/issues/3565).
- XA Transactions run on multiple threads [[#3385]](https://github.com/hazelcast/hazelcast/issues/3385).
- Hazelcast client fails to connect when you provide variables from the system properties [[#3270]](https://github.com/hazelcast/hazelcast/issues/3270).
- Entry listeners are not called when the entries are modified by WAN replication [[#2981]](https://github.com/hazelcast/hazelcast/issues/2981).
- Map wildcard matching is confusing. There should be a pluggable wildcard configuration
resolver [[#2431]](https://github.com/hazelcast/hazelcast/issues/2431).
- The method `loadAllKeys()` in map is not scalable [[#2266]](https://github.com/hazelcast/hazelcast/issues/2266).
- Back pressure feature should be added [[#1781]](https://github.com/hazelcast/hazelcast/issues/1781).

