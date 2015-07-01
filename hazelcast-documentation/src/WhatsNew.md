# What's New in Hazelcast 3.5



## Release Notes

### New Features

This section provides the new features introduced with Hazelcast 3.5 release. 

- **Async Back Pressure**: The Back Pressure introduced with Hazelcast 3.4 now supports async operations. For more information, please see the [Back Pressure section](#back-pressure).
- **Client Configuration Import**: Hazelcast now supports replacing variables with system properties in the declarative configuration of Hazelcast client. Moreover, now you can compose the Hazelcast client declarative configuration out of smaller configuration snippets. For more information, please see the [Composing Declarative Configuration section](#composing-declarative-configuration).
- **Cluster Quorum**: This feature enables you to define the minimum number of machines required in a cluster for the cluster to remain in an operational state. For more information, please see  the [Cluster Quorum section](#cluster-quorum).
- **Hazelcast Client Protocol**: Starting with 3.5, Hazelcast introduces the support for different versions of clients in a cluster. Please keep in mind that this support is not valid for the releases before 3.5. Please see the important note at the last paragraph of the [Hazelcast Java Client chapter's](#hazelcast-java-client) introduction.
- **Listener for Lost Partitions**: This feature notifies you for possible data loss occurrences. Please see the [Partition Lost Listener section](#partition-lost-listener) and [Listening for Lost Partitions section](#listening-for-lost-map-partitions).
- **Increased Visibility of Slow Operations**: With the introduction of the `SlowOperationDetector` feature, slow operations are logged and can be seen on the Hazelcast Management Center. Please see the [SlowOperationDetector section](#slowoperationdetector) and [Management Center:Members section](#members).
- **Enterprise WAN Replication**: Hazelcast Enterprise implementation of the WAN Replication. Please see the [Enterprise WAN Replication section](#enterprise-wan-replication).
- **Sub-Listener Interfaces for Map Listener**: This feature enables you to listen to map-wide or entry-based events. With this new feature, the listener formerly known as `EntryListener` has been changed to `MapListener` and `MapListener` has sub-interfaces to catch map/entry related events. Please see the [Listening to Map Events section](#listening-to-map-events) for more information.
- **Scalable Map Loader**: With this feature, you can load your keys incrementally if the number of your keys is large. Please see the [Loading Keys Incrementally section](#loading-keys-incrementally).
- **Near Cache for JCache**: Now you can use a near cache with Hazelcast's JCache implementation. Please see [JCache Near Cache](#jcache-near-cache) for details. 
- **Fail Fast on Invalid Configuration**: With this feature, Hazelcast throws a meaningful exception if there is an error in the declarative or programmatic configuration. Please see the note at the end of the [Configuration Overview section](#configuration-overview).
- **Continuous Query Caching**: (Enterprise only, since 3.5) Provides an always up to date view of an IMap according to the given predicate. Please see the [Continuous Query Cache section](#continuous-query-cache). 
- **Management of Unbounded Return Values**: Introduces a `QueryResultSizeLimiter`. Please see the [Preventing Out of Memory Exceptions section](#preventing-out-of-memory-exceptions).
- Dynamic Selector Rebalancing



