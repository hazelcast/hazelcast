# What's New in Hazelcast 3.5



## Release Notes

### New Features

This section provides the new features introduced with Hazelcast 3.5 release. 

- **Async Back Pressure**: The Back Pressure introduced with Hazelcast 3.4 now supports async operations. For more information, please see the [Back Pressure section](#back-pressure).
- **Client Configuration Import**: Hazelcast now supports replacing variables with system properties in the declarative configuration of Hazelcast client. Moreover, now you can compose the Hazelcast client declarative configuration out of smaller configuration snippets. For more information, please see the [Composing Declarative Configuration section](#composing-declarative-configuration).
- **Cluster Quorum**: This feature enables you to define the minimum number of machines required in a cluster for the cluster to remain in an operational state. For more information, please see  the [Cluster Quorum section](#cluster-quorum).
- **Hazelcast Client Protocol**: Starting with 3.5, Hazelcast introduces the support for different versions of clients in a cluster. Please keep in mind that this support is not valid for the releases before 3.5.
- **Listener for Lost Partitions**: This feature notifies you for possible data loss occurrences. Please see the [Partition Lost Listener section](#partition-lost-listener) and [MapPartitionLostListener section](#mappartitionlostlistener).
- **Increased Visibility of Slow Operations**: With the introduction of the `SlowOperationDetector` feature, slow operations are logged and can be seen on the Hazelcast Management Center. Please see the [SlowOperationDetector section](#slowoperationdetector) and [Management Center:Members section](#members).
- **Enterprise WAN Replication**: Hazelcast Enterprise implementation of the WAN Replication. Please see the [Enterprise WAN Replication section](#enterprise-wan-replication).
- **Sub-Listener Interfaces for Map Listener**: This feature enables you to listen to map-wide or entry-based events. With this new feature, the listener formerly known as `EntryListener` has been changed to `MapListener` and `MapListener` has sub-interfaces to catch map/entry related events. Please see the [Map Listener section](#map-listener) for more information.
- **Scalable Map Loader**: With this feature, you can load your keys incrementally if the number of your keys is large. Please see the [Incremental Key Loading section](#incremental-key-loading).
- **Near Cache for JCache**: Now you can use a near cache with Hazelcast's JCache implementation. Please see [JCache Near Cache](#jcache-near-cache) for details. 
- Continuous Query Caching
- Dynamic Selector Rebalancing
- Fail Fast on Invalid Configuration
- Management of Unbounded Return Values



