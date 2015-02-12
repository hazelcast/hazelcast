
### Enhancements

This section lists the enhancements performed for Hazelcast 3.5 release.

- In the class `BasicInvocationFuture`, there is no need to create an additional `AtomicInteger` object. It should be
replaced with `AtomicIntegerFieldUpdater` [[4408]](https://github.com/hazelcast/hazelcast/issues/4408).
- There is no need to use the class `IsStillExecutingOperation` to check if an operation is running locally. One
can directly access to the scheduler [[4407]](https://github.com/hazelcast/hazelcast/issues/4407).
- Configuring NearCache in a Client/Server system only talks about the programmatic configuration of NearCache on
the clients. The declarative configuration (XML) of the same is not
mentioned [[4376]](https://github.com/hazelcast/hazelcast/issues/4376).
- The JavaDoc for the methods `KeyValueSource.hasNext/element/key` and `Iterator.hasNext/next` should emphasize
the differences between each other, i.e. the state changing behavior should be
clarified [[4218]](https://github.com/hazelcast/hazelcast/issues/4218).
- While migration is in progress, the nodes will have different partition state versions. If the query is running
at that time, it can get results from the nodes at different stages of the migration. By adding partition state
version to the query results, it can be checked whether the migration was happening and the query can be
re-run [[4206]](https://github.com/hazelcast/hazelcast/issues/4206).
- XML Config Schema does not allow to set a `SecurityInterceptor`
Implementation [[4118]](https://github.com/hazelcast/hazelcast/issues/4118).
- Currently, certain types of remote executed calls are stored into the `executingCalls` map. The key
(and value) is a `RemoteCallKey` object. The functionality provided is the ability to ask on the remote side
if an operation is still executing. For a partition-aware operation, this is not needed. When an operation is
scheduled by a partition specific operation thread, the operation can be stored in a volatile field in that
thread [[4079]](https://github.com/hazelcast/hazelcast/issues/4079).
- The class `TcpIpJoinerOverAWS` fails at AWS' recently launched eu-central-1 region. The reason for the fail is
that the region requires v4 signatures [[#3963]](https://github.com/hazelcast/hazelcast/issues/3963).
- The `hazelcast-spring-<`*version*`>.xsd` should include the User Defined Services (SPI) elements and
attributes [[#3565]](https://github.com/hazelcast/hazelcast/issues/3565).
- Map wildcard matching is confusing. There should be a pluggable wildcard configuration
resolver [[#2431]](https://github.com/hazelcast/hazelcast/issues/2431).



