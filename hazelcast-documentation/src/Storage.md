
# Storage

## Hazelcast Hi-Density Cache

![](images/enterprise-onlycopy.jpg)

Hazelcast Hi-Density Cache, the successor to Hazelcast Elastic Memory, is the new enterprise grade backend storage solution. This solution is used with the Hazelcast JCache implementation.


By default, Hazelcast offers a production ready, low garbage collection (GC) pressure, storage backend. Serialized keys and values are still stored in the standard Java map like data structures on the heap. Whereas they are already stored in serialized form for the highest data compaction, the data structures are still subject to Java Garbage Collection.

In Hazelcast Enterprise, the Hi-Density Cache is built around a pluggable memory manager which enables multiple memory stores. These memory stores are all accessible using a common access layer to scale up to Terabytes of main memory on a single JVM. At the same time, by further minimizing the GC pressure, Hi-Density Cache enables predictable application scaling and boosts performance as well as latency while minimizing pauses for Java Garbage Collection.

This foundation includes but is not limited to storing keys and values next to the heap in a native memory region.

<br></br>
***RELATED INFORMATION***

*Please refer to [Hazelcast JCache](#hazelcast-jcache) chapter for the details of Hazelcast JCache implementation. As mentioned, Hi-Density Cache is used with Hazelcast JCache implementation.*
<br></br>




## Hazelcast Elastic Memory

![](images/enterprise-onlycopy.jpg)


By default, Hazelcast stores your distributed data (map entries, queue items) into Java heap which is subject to garbage collection (GC). As your heap gets bigger, garbage collection might cause your application to pause tens of seconds, badly effecting your application performance and response times. Elastic Memory is Hazelcast with direct memory storage off the heap to avoid GC pauses. Even if you have terabytes of cache in-memory with lots of updates, GC will have almost no effect; resulting in more predictable latency and throughput.

Here are the steps to enable Elastic Memory:

- Set the maximum direct memory JVM can allocate, e.g. `java -XX:MaxDirectMemorySize=60G`

- Enable Elastic Memory by setting `hazelcast.elastic.memory.enabled` Hazelcast configuration property to true.

- Set the total direct memory size for HazelcastInstance by setting `hazelcast.elastic.memory.total.size` Hazelcast configuration property. Size can be in MB or GB and abbreviation can be used, such as 60G and 500M.

- Set the chunk size by setting `hazelcast.elastic.memory.chunk.size` Hazelcast configuration property. Hazelcast will partition the entire elastic memory into chunks. Default chunk size is 1K.

- You can enable `sun.misc.Unsafe` based elastic memory storage implementation instead of `java.nio.DirectByteBuffer` based one, by setting `hazelcast.elastic.memory.unsafe.enabled property` to **true**. Default value is **false**.

- Configure maps that will use Native Memory by setting `InMemoryFormat` to **OFFHEAP**. Default value is **BINARY**.

Below is the declarative configuration.

```xml
<hazelcast>
  ...
  <map name="default">
    ...
    <in-memory-format>OFFHEAP</in-memory-format>
  </map>
</hazelcast>
```


And, the programmatic configuration:

```java
MapConfig mapConfig = new MapConfig();
mapConfig.setInMemoryFormat( InMemoryFormat.OFFHEAP );
```

<br> </br>

