
# Storage

## High-Density Memory Store

![](images/enterprise-onlycopy.jpg)

Hazelcast High-Density Memory Store, the successor to Hazelcast Elastic Memory, is Hazelcast's new enterprise grade backend storage solution. This solution is used with the Hazelcast JCache implementation.


By default, Hazelcast offers a production ready, low garbage collection (GC) pressure, storage backend. Serialized keys and values are still stored in the standard Java map, such as data structures on the heap. The data structures are stored in serialized form for the highest data compaction, and are still subject to Java Garbage Collection.

In Hazelcast Enterprise, the High-Density Memory Store is built around a pluggable memory manager which enables multiple memory stores. These memory stores are all accessible using a common access layer that scales up to Terabytes of main memory on a single JVM. At the same time, by further minimizing the GC pressure, High-Density Memory Store enables predictable application scaling and boosts performance and latency while minimizing pauses for Java Garbage Collection.

This foundation includes, but is not limited to, storing keys and values next to the heap in a native memory region.

<br></br>
***RELATED INFORMATION***

*Please refer to the [Hazelcast JCache chapter](#hazelcast-jcache) for the details of Hazelcast JCache implementation. As mentioned, High-Density Memory Store is used with Hazelcast JCache implementation.*
<br></br>


### Configuring Hi-Density Memory Store

To use the Hi-Density memory storage, the native memory usage must be enabled using the programmatic or declarative configuration.
Also, you can configure its size, memory allocator type, minimum block size, page size and metadata space percentage.

- **size:** Size of the total native memory to allocate. Default value is **512 MB**.
- **allocator type:** Type of the memory allocator. Available values are:
  * STANDARD: allocate/free memory using default OS memory manager.
  * POOLED: manage memory blocks in thread local pools. 

  Default value is **POOLED**.
- **minimum block size:** Minimum size of the blocks in bytes to split and fragment a page block to assign to an allocation request. It is used only by the **POOLED** memory allocator. Default value is **16**.
- **page size:** Size of the page in bytes to allocate memory as a block. It is used only by the **POOLED** memory allocator. Default value is `1 << 22` = **4194304 Bytes**, about **4 MB**.
- **metadata space percentage:** Defines the percentage of the allocated native memory that is used for the metadata such as indexes, offsets, etc. It is used only by the **POOLED** memory allocator. Default value is **12.5**.

The following is the programmatic configuration example.

```java
MemorySize memorySize = new MemorySize(512, MemoryUnit.MEGABYTES);
NativeMemoryConfig nativeMemoryConfig =
                new NativeMemoryConfig()
                        .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                        .setSize(memorySize)
                        .setEnabled(true)
                        .setMinBlockSize(16)
                        .setPageSize(1 << 20);
```

The following is the declarative configuration example.

```xml
<native-memory enabled="true" allocator-type="POOLED">
  <size value="512" unit="MEGABYTES"/>
</native-memory>
```

## Elastic Memory (High-Density Memory First Generation)

By default, Hazelcast stores your distributed data (map entries, queue items) into Java heap which is subject to garbage collection (GC). As your heap gets bigger, garbage collection might cause your application to pause tens of seconds, badly effecting your application performance and response times. Elastic Memory (High-Density Memory First Generation) is Hazelcast with off-heap memory storage to avoid GC pauses. Even if you have terabytes of cache in-memory with lots of updates, GC will have almost no effect; resulting in more predictable latency and throughput.

Here are the steps to enable Elastic Memory:

- Set the maximum direct memory JVM can allocate, e.g. `java -XX:MaxDirectMemorySize=60G`.
- Enable Elastic Memory by setting the `hazelcast.elastic.memory.enabled` property to true.
- Set the total direct memory size for HazelcastInstance by setting the `hazelcast.elastic.memory.total.size` property. Size can be in MB or GB and abbreviation can be used, such as 60G and 500M.
- Set the chunk size by setting the `hazelcast.elastic.memory.chunk.size` property. Hazelcast will partition the entire off-heap memory into chunks. Default chunk size is 1K.
- You can enable `sun.misc.Unsafe` based off-heap storage implementation instead of `java.nio.DirectByteBuffer` based one, by setting the `hazelcast.elastic.memory.unsafe.enabled` property to true. Default value is false.
- Configure maps that will use Elastic Memory by setting `InMemoryFormat` to OFFHEAP. Default value is BINARY.

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

And, the following are the High-Denisty Memory First Generation related system properties.

|Property|Default Value|Type|Description|
|:-------|:-------|:-----------|
|`hazelcast.elastic.memory.enabled`|false|bool|Enables/disables Elastic Memory usage.
|`hazelcast.elastic.memory.total.size`|128|int|Elastic Memory storage total size in MB or GB.
|`hazelcast.elastic.memory.chunk.size`|1|int|Elastic Memory storage chunk size in KB.
|`hazelcast.elastic.memory.shared.storage`|false|bool|Enables/disables Elastic Memory shared storage.
|`hazelcast.elastic.memory.unsafe.enabled`|false|bool|Enables/disables usage of `sun.misc.Unsafe` when allocating, reading and modifying off-heap storage.

