
# Storage

## Hazelcast Breakout Memory Store

![](images/enterprise-onlycopy.jpg)

Hazelcast Breakout Memory Store, the successor to Hazelcast Elastic Memory, is the new enterprise grade backend storage solution. This solution is used with the Hazelcast JCache implementation.


By default, Hazelcast offers a production ready, low garbage collection (GC) pressure, storage backend. Serialized keys and values are still stored in the standard Java map like data structures on the heap. Whereas they are already stored in serialized form for the highest data compaction, the data structures are still subject to Java Garbage Collection.

In Hazelcast Enterprise, the Breakout Memory Store is built around a pluggable memory manager which enables multiple memory stores. These memory stores are all accessible using a common access layer to scale up to Terabytes of main memory on a single JVM. At the same time, by further minimizing the GC pressure, Breakout Memory Store enables predictable application scaling and boosts performance as well as latency while minimizing pauses for Java Garbage Collection.

This foundation includes but is not limited to storing keys and values next to the heap in a native memory region.

<br></br>
***RELATED INFORMATION***

*Please refer to [Hazelcast JCache](#hazelcast-jcache) chapter for the details of Hazelcast JCache implementation. As mentioned, Breakout Memory is used with Hazelcast JCache implementation.*
<br></br>




