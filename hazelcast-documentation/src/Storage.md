
# Storage

## High-Density Memory Store

![](images/enterprise-onlycopy.jpg)

Hazelcast High-Density Memory Store, the successor to Hazelcast Elastic Memory, is Hazelcast's new enterprise grade backend storage solution. This solution is used with the Hazelcast JCache implementation.


By default, Hazelcast offers a production ready, low garbage collection (GC) pressure, storage backend. Serialized keys and values are still stored in the standard Java map, such as data structures on the heap. The data structures are stored in serialized form for the highest data compaction, and are still subject to Java Garbage Collection.

In Hazelcast Enterprise, the High-Density Memory Store is built around a pluggable memory manager which enables multiple memory stores. These memory stores are all accessible using a common access layer that scales up to Terabytes of main memory on a single JVM. At the same time, by further minimizing the GC pressure, High-Density Memory Store enables predictable application scaling and boosts performance and latency while minimizing pauses for Java Garbage Collection.

This foundation includes, but is not limited to, storing keys and values next to the heap in a native memory region.

***Configuring Hi-Density Memory Store***

To use Hi-Density memory storage, native memory usage must be enabled by programmatically or by XML file.
Also, you can configure its size, memory allocator type, minimum block size, page size and metadata space percentage.

- **size:** Size of total native memory to allocate. Default value is **512 MB**.
- **allocator type:** Type of memory allocator. Valid values are:
  * STANDARD: allocate/free memory using default OS memory manager
  * POOLED: manage memory blocks in thread local pools. 

  Default value is **POOLED**.
- **minimum block size:** Minimum size of blocks in bytes to split and fragment a page block for assigning to an allocation request. Used only by **POOLED** memory allocator. Default value is **16**.
- **page size:** Size of page in bytes to allocate memory as block. Used only by **POOLED** memory allocator. Default value is `1 << 22` = **4194304 Bytes**, about **4 MB**.
- **metadata space percentage:** Percentage value about how many percentage of allocated native memory is used for metadata such as indexes, offsets, etc ... Used only by **POOLED** memory allocator. Default value is **12.5**.

Here is programmatic configuration sample:

~~~~ java
MemorySize memorySize = new MemorySize(512, MemoryUnit.MEGABYTES);
NativeMemoryConfig nativeMemoryConfig =
                new NativeMemoryConfig()
                        .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                        .setSize(memorySize)
                        .setEnabled(true)
                        .setMinBlockSize(16)
                        .setPageSize(1 << 20);
~~~~

Here is XML configuration sample:

~~~~ xml
<native-memory enabled="true" allocator-type="POOLED">
  <size value="512" unit="MEGABYTES"/>
</native-memory>
~~~~

<br></br>
***RELATED INFORMATION***

*Please refer to the [Hazelcast JCache chapter](#hazelcast-jcache) chapter for the details of Hazelcast JCache implementation. As mentioned, High-Density Memory Store is used with Hazelcast JCache implementation.*
<br></br>




