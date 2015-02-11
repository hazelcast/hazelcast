
### ReplicatedMap Configuration

The ReplicatedMap can be configured using two different way. As most other features in Hazelcast these are:

- programmatically: the typical Hazelcast way, using the Config API seen above),
- and declaratively: using `hazelcast.xml`.

#### ReplicatedMap Declarative Configuration

You can declare your ReplicatedMap configuration in the Hazelcast configuration file `hazelcast.xml`. The configuration can be
used to tune the behavior of the internal replication algorithm such as the replication delay which is used to batch up replication
for better network utilization.

```xml
<replicatedmap name="default">
  <in-memory-format>BINARY</in-memory-format>
  <concurrency-level>32</concurrency-level>
  <replication-delay-millis>100</replication-delay-millis>
  <async-fillup>true</async-fillup>
  <statistics-enabled>true</statistics-enabled>
  <entry-listeners>
    <entry-listener include-value="true">
      com.hazelcast.examples.EntryListener
    </entry-listener>
  </entry-listeners>
</replicatedmap>
```

- `in-memory-format`: Configuration of the internal storage format, please see [In-Memory Format](#in-memory-format-on-replicatedmap). The default is BINARY.
- `concurrency-level`: Number of parallel mutexes to minimize contention on keys. The default value is 32 which is a good number for lots of applications. If higher contention is seen on writes to values inside of the replicated map this value can be adjusted to the needs.
- `replication-delay-millis`: Defines a number of milliseconds after a put is executed before the value is replicated to other nodes. In this time multiple puts can be operated and are cached up to be send at once. This highers the latency for eventually consistency but lowers IO operations. Default value is 100ms before a replication is operated, if set to 0 no delay is used and all values are replicated one by one.
- `async-fillup`: This value defines it the replicated map is available for reads before the initial replication is completed. Default is true. If set to false no Exception will be thrown when replicated map is not yet ready but call will block until finished.
- `statistics-enabled`: If set to true, statistics like cache hits and misses are collected. Its default value is false.
- `entry-listener`: The value of this tag is the full canonical classname of the EntryListener implementation.
  - `entry-listener#include-value`: This attribute defines if the event will include the value or not. Sometimes the key is enough to react on an event. In those situations set this value to false will save a deserialization cycle. Default value is true.
  - `entry-listener#local`: This attribute is unused for ReplicatedMap since listeners are always local.

#### ReplicatedMap Programmatic Configuration

For programmatic configuration the Config API is used as for all other data structures in Hazelcast. The configuration must be
created upfront instantiating the `HazelcastInstance`.

A basic example how to configure the ReplicatedMap using the programmatic API will be shown in the following snippet:

```java
Config config = new Config();

ReplicatedMapConfig replicatedMapConfig =
    config.getReplicatedMapConfig( "default" );

replicatedMapConfig.setInMemoryFormat( InMemoryFormat.BINARY );
replicatedMapConfig.setConcurrencyLevel( 32 );
```

All properties that can be configured using the declarative configuration are also available using programmatic configuration
by transforming the tag names into getter or setter names.

#### In Memory Format on Replicated Map

Currently two `in-memory-format` values are usable with the Replicated Map.

- `OBJECT` (default): The data will be stored in deserialized form. This configuration is the default choice since
data replication is mostly used for high speed access. Please be aware that changing values without a `Map::put` is
not reflected on other nodes but is visible on the changing nodes for later value accesses.

- `BINARY`: The data is stored in serialized binary format and has to be deserialized on every request. This
option offers higher encapsulation since changes to values are always discarded as long as the newly changed object is
not explicitly `Map::put` into the map again.
