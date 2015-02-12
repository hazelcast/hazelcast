
### Replicated Map Configuration

Replicated Map can be configured using the following ways, as the most other features in Hazelcast:

- Programmatic: the typical Hazelcast way, using the Config API seen above.
- Declarative: using `hazelcast.xml`.

#### Replicated Map Declarative Configuration

You can declare your Replicated Map configuration in the Hazelcast configuration file `hazelcast.xml`. The configuration can be
used to tune the behavior of the internal replication algorithm such as the replication delay which is used to batch up the replication
for better network utilization. See the following example declarative configuration.

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

- `in-memory-format`: Defines the internal storage format.  Please see the [In-Memory Format section](#in-memory-format-on-replicated-map). The default value is `BINARY`.
- `concurrency-level`: Number of parallel mutexes to minimize the contention on the keys. The default value is 32 which is a good number for lots of applications. If higher contention is seen on writes to values inside the replicated map, this value can be adjusted according to the needs.
- `replication-delay-millis`: Defines the period in milliseconds after a put is executed, before the value is replicated to other nodes. During this time, multiple puts can be operated and are cached up to be sent at once. This increases the latency for eventual consistency but lowers the I/O operations. The default value is 100ms before a replication is operated. If it is set to 0, no delay is used and all values are replicated one by one.
- `async-fillup`: Defines if the replicated map is available for reads before the initial replication is completed. The default value is `true`. If it is set to `false`, no exception will be thrown when replicated map is not yet ready but the call will block until it is finished.
- `statistics-enabled`: If it is set to `true`, the statistics like cache hits and misses are collected. The default value is `false`.
- `entry-listener`: The value of this element is the full canonical classname of the `EntryListener` implementation.
  - `entry-listener#include-value`: This attribute defines if the event will include the value or not. Sometimes the key is enough to react on an event. In those situations, setting this value to `false` will save a deserialization cycle. The default value is `true`.
  - `entry-listener#local`: This attribute is not used for Replicated Map since listeners are always local.

#### Replicated Map Programmatic Configuration

The Config API is used for programmatic configuration as for all other data structures in Hazelcast. The configuration must be
created upfront instantiating the `HazelcastInstance`.

A basic example on how to configure the Replicated Map using the programmatic approach is shown in the following snippet.

```java
Config config = new Config();

ReplicatedMapConfig replicatedMapConfig =
    config.getReplicatedMapConfig( "default" );

replicatedMapConfig.setInMemoryFormat( InMemoryFormat.BINARY );
replicatedMapConfig.setConcurrencyLevel( 32 );
```

All properties that can be configured using the declarative configuration are also available using programmatic configuration
by transforming the tag names into getter or setter names.

#### In-Memory Format on Replicated Map

Currently, two `in-memory-format` values are usable with the Replicated Map.

- `OBJECT` (default): The data will be stored in deserialized form. This configuration is the default choice since
the data replication is mostly used for high speed access. Please be aware that changing the values without a `Map::put` is
not reflected on the other nodes but is visible on the changing nodes for later value accesses.

- `BINARY`: The data is stored in serialized binary format and has to be deserialized on every request. This
option offers higher encapsulation since changes to values are always discarded as long as the newly changed object is
not explicitly `Map::put` into the map again.
