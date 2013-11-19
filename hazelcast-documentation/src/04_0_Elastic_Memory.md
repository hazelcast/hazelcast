

# Elastic Memory (Enterprise Edition Only) #

By default, Hazelcast stores your distributed data (map entries, queue items) into Java heap which is subject to garbage collection. As your heap gets bigger, garbage collection might cause your application to pause tens of seconds, badly effecting your application performance and response times. Elastic Memory is Hazelcast with off-heap (direct) memory storage to avoid GC pauses. Even if you have terabytes of cache in-memory with lots of updates, GC will have almost no effect; resulting in more predictable latency and throughput.

Here are the steps to enable Elastic Memory:

- Set the maximum direct memory JVM can allocate. Example `java -XX:MaxDirectMemorySize=60G`...

- Enable Elastic Memory by setting `hazelcast.elastic.memory.enabled` Hazelcast Config Property to true.

- Set the total direct memory size for HazelcastInstance by setting `hazelcast.elastic.memory.total.size` Hazelcast Config Property. Size can be in MB or GB and abbreviation can be used, such as 60G and 500M.

- Set the chunk size by setting `hazelcast.elastic.memory.chunk.size` Hazelcast Config Property. Hazelcast will partition the entire offheap memory into chunks. Default chunk size is 1K.

- You can enable `sun.misc.Unsafe` based off-heap storage implementation instead of `java.nio.DirectByteBuffer` based one by setting hazelcast.elastic.memory.unsafe.enabled property to `true`. Default value is `false`.

- Configure maps you want them to use Elastic Memory by setting `StorageFormat` to `OFFHEAP`. Default value is `BINARY`.

Using XML configuration:

```xml
<hazelcast>
    ...
    <map name="default">
        ...
        <in-memory-format>OFFHEAP</in-memory-format>
    </map>
</hazelcast>
```

Using Config API:

```java
MapConfig mapConfig = new MapConfig();
mapConfig.setStorageFormat(StorageFormat.OFFHEAP);
```