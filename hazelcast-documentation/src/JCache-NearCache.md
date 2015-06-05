### JCache Near Cache

Cache entries in Hazelcast are stored as partitioned across the cluster. 
When you try to read a record with the key `k`, if the current node is not the owner of that key (i.e. not the owner of partition that the key belongs to), 
Hazelcast sends a remote operation to the owner node. Each remote operation means lots of network trips. 
If your cache is used for mostly read operations, it is advised to use a near cache storage in front of the cache itself to read cache records faster and consume less network traffic.
<br><br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Near cache for JCache is only available for clients NOT servers.*
<br><br>

However, using near cache comes with some trade-off for some cases:

- There will be extra memory consumption for storing near cache records at local.
- If invalidation is enabled and entries are updated frequently, there will be many invalidation events across the cluster.
- Near cache does not give strong consistency but gives eventual consistency guarantees. It is possible to read stale data.

#### JCache Near Cache Invalidation

Invalidation is the process of removing an entry from the near cache since the entry is not valid anymore (its value is updated or it is removed from actual cache). Near cache invalidation happens asynchronously at the cluster level, but synchronously in real-time at the current node. This means when an entry is updated (explicitly or via entry processor) or removed (deleted explicitly or via entry processor, evicted, expired), it is invalidated from all near caches asynchronously within the whole cluster but updated/removed at/from the current node synchronously. Generally, whenever the state of an entry changes in the record store by updating its value or removing it, the invalidation event is sent for that entry.

Invalidation events can be sent either individually or in batches. If there are lots of mutating operations such as put/remove on the cache, sending the events in batches is advised. This reduces the network traffic and keeps the eventing system less busy. 

You can use the following system properties to configure the sending of invalidation events in batches:

- `hazelcast.cache.invalidation.batch.enabled`: Specifies whether the cache invalidation event batch sending is enabled or not. The default value is `true`.
- `hazelcast.cache.invalidation.batch.size`: Defines the maximum number of cache invalidation events to be drained and sent to the event listeners in a batch. The default value is `100`.
- `hazelcast.cache.invalidation.batchfrequency.seconds`: Defines cache invalidation event batch sending frequency in seconds. When event size does not reach to `hazelcast.cache.invalidation.batch.size` in the given time period, those events are gathered into a batch and sent to the target. The default value is `5` seconds.

So if there are so many clients or so many mutating operations, batching should remain enabled and the batch size should be configured with the `hazelcast.cache.invalidation.batch.size` system property to a suitable value.

#### JCache Near Cache Expiration

Expiration means the eviction of expired records. A record is expired: 
- if it is not touched (accessed/read) for `<max-idle-seconds>`,
- `<time-to-live-seconds>` passed since it is put to near-cache.

Expiration is performed in two cases:

- When a record is accessed, it is checked about if it is expired or not. If it is expired, it is evicted and returns `null` to caller.
- In the background, there is an expiration task that periodically (currently 5 seconds) scans records and evicts the expired records.

#### JCache Near Cache Eviction

In the scope of near cache, eviction means evicting (clearing) the entries selected according to the given `eviction-policy` when the specified `max-size-policy` has been reached. Eviction is handled with `max-size policy` and `eviction-policy` elements. Please see the [JCache Near Cache Configuration section](#jcache-near-cache-configuration).

##### `max-size-policy`

This element defines the state when near cache is full and whether the eviction should be triggered. The following policies for maximum cache size are supported by the near cache eviction:

- **ENTRY_COUNT:** Maximum size based on the entry count in the near cache. Available only for `BINARY` and `OBJECT` in-memory formats.
- **USED_NATIVE_MEMORY_SIZE:** Maximum used native memory size of the specified near cache in MB to trigger the eviction. If the used native memory size exceeds this threshold, the eviction is triggered.  Available only for `NATIVE` in-memory format. This is supported only by Hazelcast Enterprise.
- **USED_NATIVE_MEMORY_PERCENTAGE:** Maximum used native memory percentage of the specified near cache to trigger the eviction. If the native memory usage percentage (relative to maximum native memory size) exceeds this threshold, the eviction is triggered. Available only for `NATIVE` in-memory format. This is supported only by Hazelcast Enterprise.
- **FREE_NATIVE_MEMORY_SIZE:** Minimum free native memory size of the specified near cache in MB to trigger the eviction.  If free native memory size goes down below of this threshold, eviction is triggered. Available only for `NATIVE` in-memory format. This is supported only by Hazelcast Enterprise.
- **FREE_NATIVE_MEMORY_PERCENTAGE:** Minimum free native memory percentage of the specified near cache to trigger eviction. If free native memory percentage (relative to maximum native memory size) goes down below of this threshold, eviction is triggered. Available only for `NATIVE` in-memory format. This is supported only by Hazelcast Enterprise.

##### `eviction-policy` 

Once a near cache is full (reached to its maximum size as specified with the `max-size-policy` element), an eviction policy determines which, if any, entries must be evicted. Currently, the following eviction policies are supported by near cache eviction:

- LRU (Least Recently Used)
- LFU (Least Frequently Used)

#### JCache Near Cache Configuration

The following are the example configurations.

**Declarative**:

```xml
<hazelcast-client>
    ...
    <near-cache name="myCache">
        <in-memory-format>BINARY</in-memory-format>
        <invalidate-on-change>true</invalidate-on-change>
        <cache-local-entries>false</cache-local-entries>
        <time-to-live-seconds>3600000</time-to-live-seconds>
        <max-idle-seconds>600000</max-idle-seconds>
        <eviction size="1000" max-size-policy="ENTRY_COUNT" eviction-policy="LFU"/>
    </near-cache>
    ...
</hazelcast-client>
```

**Programmatic**:

```java
EvictionConfig evictionConfig = new EvictionConfig();
evictionConfig.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
evictionConfig.setEvictionPolicy(EvictionPolicy.LFU);
evictionConfig.setSize(10000);
 
NearCacheConfig nearCacheConfig =
    new NearCacheConfig()
        .setName("myCache")
        .setInMemoryFormat(InMemoryFormat.BINARY)
        .setInvalidateOnChange(true)
        .setCacheLocalEntries(false)
        .setTimeToLiveSeconds(60 * 60 * 1000) // 1 hour TTL
        .setMaxIdleSeconds(10 * 60 * 1000) // 10 minutes max idle seconds
        .setEvictionConfig(evictionConfig); 
...

clientConfig.addNearCacheConfig(nearCacheConfig);
```

The following are the definitions of the configuration elements and attributes.

- `in-memory-format`: Storage type of near cache entries. Available values are `BINARY`, `OBJECT` and `NATIVE_MEMORY`. `NATIVE_MEMORY` is available only for Hazelcast Enterprise. Default value is `BINARY`.
- `invalidate-on-change`: Specifies whether the cached entries are evicted when the entries are changed (updated or removed) on the local and global. Available values are `true` and `false`. Default value is `true`.
- `cache-local-entries`: Specifies whether the local cache entries are stored eagerly (immediately) to near cache when a put operation from the local is performed on the cache. Available values are `true` and `false`. Default value is `false`.
- `time-to-live-seconds`: Maximum number of seconds for each entry to stay in the near cache. Entries that are older than `<time-to-live-seconds>` will be automatically evicted from the near cache. It can be any integer between `0` and `Integer.MAX_VALUE`. `0` means **infinite**. Default value is `0`.
- `max-idle-seconds`: Maximum number of seconds each entry can stay in the near cache as untouched (not-read). Entries that are not read (touched) more than `<max-idle-seconds>` value will be removed from the near cache. It can be any integer between `0` and `Integer.MAX_VALUE`. `0` means `Integer.MAX_VALUE`. Default is `0`.
- `eviction`: Specifies when the eviction is triggered (`max-size policy`) and which eviction policy (`LRU` or `LFU`) is used for the entries to be evicted. The default value for `max-size-policy` is `ENTRY_COUNT`, default `size` is `10000` and default `eviction-policy` is `LRU`. For High-Density Memory Store near cache, since `ENTRY_COUNT` eviction policy is not supported yet, eviction must be explicitly configured with one of the supported policies:
	- `USED_NATIVE_MEMORY_SIZE`
	- `USED_NATIVE_MEMORY_PERCENTAGE`
	- `FREE_NATIVE_MEMORY_SIZE`
	- `FREE_NATIVE_MEMORY_PERCENTAGE`.

Near cache can be configured only at the client side.

#### Notes About Client Near Cache Configuration

Near cache configuration can be defined at the client side (using `hazelcast-client.xml` or `ClientConfig`) as independent configuration (independent from the `CacheConfig`). Near cache configuration lookup is handled as described below:

- Look for near cache configuration with the name of the cache given in the client configuration.
- If a defined near cache configuration is found, use this near cache configuration defined at the client.
- Otherwise: 
	- If there is a defined default near cache configuration is found, use this default near cache configuration.
	- If there is no default near cache configuration, it means there is no near cache configuration for cache.
	



