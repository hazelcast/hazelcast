
### Improvements

This section lists the improvements performed for Hazelcast 3.4 release.

- Event packets sent to the client do not have "partitionId" [[#4071]](https://github.com/hazelcast/hazelcast/issues/4071).
- Spring Configuration for ReplicatedMap is Missing [[#3966]](https://github.com/hazelcast/hazelcast/issues/3966).
- NodeMulticastListener floods log file with INFO-level messages when debug is enabled [[#3787]](https://github.com/hazelcast/hazelcast/issues/3787).
- A Hazelcast client should not be a `HazelcastInstance`. It should be a "factory" and this factory should be able to shut down Hazelcast clients. [[#3781]](https://github.com/hazelcast/hazelcast/issues/3781).
- InvalidateSessionAttributesEntryProcessor could avoid creating strings at every call to process [[#3767]](https://github.com/hazelcast/hazelcast/issues/3767).
- SocketConnector timeout cannot be configured [[#3613]](https://github.com/hazelcast/hazelcast/issues/3613).
- The method `MultiMap.get()` returns `collection`, but this method should return the correct collection type (`Set` or `List`) [[#3214]](https://github.com/hazelcast/hazelcast/issues/3214).
- HazelcastConnection is not aligned with HazecastInstance [[#2997]](https://github.com/hazelcast/hazelcast/issues/2997).
- Support for Log4j 2.x has been implemented [[#2345]](https://github.com/hazelcast/hazelcast/issues/2345).
- Management Center console behaviour on node shutdown [[#2215]](https://github.com/hazelcast/hazelcast/issues/2215).
- When queue-store not enabled, QueueStoreFactory should not be instantiated [[#1906]](https://github.com/hazelcast/hazelcast/issues/1906).
- Management Center should be able to say when cluster is safe and all backups are up to date [[#963]](https://github.com/hazelcast/hazelcast/issues/963).

