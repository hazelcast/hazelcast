
# Storage


## Native Memory

![](images/enterprise-onlycopy.jpg)


By default, Hazelcast stores your distributed data (map entries, queue items) into Java heap which is subject to garbage collection (GC). As your heap gets bigger, garbage collection might cause your application to pause tens of seconds, badly effecting your application performance and response times. Native Memory is Hazelcast with off-heap (direct) memory storage to avoid GC pauses. Even if you have terabytes of cache in-memory with lots of updates, GC will have almost no effect; resulting in more predictable latency and throughput.

Here are the steps to enable Native Memory:

- Set the maximum direct memory JVM can allocate, e.g. `java -XX:MaxDirectMemorySize=60G`

- Enable Native Memory by setting `hazelcast.elastic.memory.enabled` Hazelcast configuration property to true.

- Set the total direct memory size for HazelcastInstance by setting `hazelcast.elastic.memory.total.size` Hazelcast configuration property. Size can be in MB or GB and abbreviation can be used, such as 60G and 500M.

- Set the chunk size by setting `hazelcast.elastic.memory.chunk.size` Hazelcast configuration property. Hazelcast will partition the entire native memory into chunks. Default chunk size is 1K.

- You can enable `sun.misc.Unsafe` based native memory storage implementation instead of `java.nio.DirectByteBuffer` based one, by setting `hazelcast.elastic.memory.unsafe.enabled property` to **true**. Default value is **false**.

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

