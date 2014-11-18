
### ReplicatedMap Configuration

**Declarative:**

```xml
<replicatedmap name="myMap">
   <in-memory-format>"OBJECT"</in-memory-format>
   <concurrency-level>"32"</concurrency-level>
   <replication-delay-millis>"200"</replication-delay-millis>
   <async-fillup>"true"</async-fillup>
   <statistics-enabled>"true"</statistics-enabled>
   <entry-listeners>
      <entry-listener>???</entry-listener>
   </entry-listeners>
</replicatedmap>
```

**Programmatic:**

```java
Config config = new Config();
ReplicatedMapConfig rmConfig = config.getReplicatedMapConfig();

rmConfig.setName("myMap").setInMemoryFormat("OBJECT")
        .setReplicationDelayMillis("200").setAsyncFillup("true");
```

It has below parameters.
 
- name: Name for your WAN replication configuration.
- in-memory-format: Create a group and its password using this parameter.
- concurrency-level: Number of parallel mutexes to minimize contention on keys. The default value is 32 which is a good number for lots of applications. If higher contention is seen on writes to values inside of the replicated map this value can be adjusted to the needs.
- replication-delay-millis: Defines a number of milliseconds after a put is executed before the value is replicated to other nodes. In this time multiple puts can be operated and are cached up to be send at once. This highers the latency for eventually consistency but lowers IO operations. Default value is 100ms before a replication is operated, if set to 0 no delay is used and all values are replicated one by one.
- async-fillup: This value defines it the replicated map is available for reads before the initial replication is completed. Default is true. If set to false no Exception will be thrown when replicated map is not yet ready but call will block until finished.
- statistics-enabled: ???
- entry-listeners: ???

