

## What's new in 3.0?

**Core architecture:**

-   Multi-thread execution: Operations are now executed by multiple threads (by factor of processor cores). With Hazelcast 2, there was a only single thread.
-   SPI
    : Service Programming Interface for developing new partitioned services, data structures. All Hazelcast data structures like Map, Queue are reimplemented with SPI.

**Serialization**

-   IdentifiedDataSerializable
    : A slightly optimized version of DataSerializable that doesn't use class name and reflection for de-serialization.
-   Portable Serialization
    : Another Serialization interface that doesn't use reflection and can navigate through binary data and fetch/query/index individual field without having any reflection or whole object de-serialization.
-   Custom Serialization
    : Support for custom serialization that can be plugged into Hazelcast.

**Map**

-   Entry Processor
    : Executing an EntryProcessor on the key or on all entries. Hazelcast implicitly locks the entree and guarantees no migration while the execution of the Processor.
-   In Memory Format
    : Support for storing entries in Binary, Object and Cached format.
-   Continuous Query
    : Support for listeners that register with a query and are notified when there is a change on the Map that matches the Query.
-   Interceptors
    : Ability to intercept the Map operation before/after it is actually executed.
-   Lazy Indexing
    :Ability to index existing items in the map. No need to add indexes at the very beginning.

**Queue**

-   No more dependency on the distributed map
-   Scales really well as you have thousands of separate queues.
-   Persistence
    Support for persistence with QueueStore.

**Multimap**

-   Values can be Set/List/Queue.

**Topic**

-   Total Ordering : Support for global ordering where all Nodes receive all messages in the same order.

**Transactions**

-   Distributed Transaction
    : Support for both 1-phase (local) and 2 phase transactions with a totally new API.

**Client**

-   New Binary Protocol: A new binary protocol based on portable serialization. The same protocol is used for Java/C/C\# and other client
-   Smart client: Support for dummy and smart client. Where a dummy client will maintain a connection to only one member, whereas the smart client can route the operations to the Node that owns the data.

