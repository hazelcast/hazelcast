### JCache Near Cache

Cache entries in Hazelcast are stored as partitioned across the cluster. 
When you try to read a record with key `k`, if the current node is not the owner of that key (not the owner of partition that the key belongs to), 
a remote operation is sent to the owner node. So each remote operation means lots of network trips. 
If your cache is used for reading generally, it is advised to use a near-cache storage in front of cache itself for reading cache records faster and consume less network traffic.
Also note that near-cache is **only available for clients** not servers.

However, using near-cache comes with some trade-off for some cases:
-   There will be extra the memory consumption for storing near-cache records at local.
-   If invalidation is enabled and entries are updated frequently, there will be many invalidation events across the cluster.
-   Near-Cache doesn't give strong consistency but gives eventual consistency guarantees. It is possible to read stale data.

#### Invalidation

Invalidation means that a near-cache entry is not valid anymore (its value is updated or it is removed from actual cache) so it should be removed from near-cache. Invalidation from near-cache is done eventually at whole cluster asynchronously but done at current node in real-time as synchronously. This means when an entry updated (explicitly or over entry processor) or removed (deleted explicitly or over entry processor, evicted, expired), it is invalidated from all near-caches asynchronously at whole cluster but updated/removed at/from current node synchronously. In general terms, whenever an entry state is changed in record store by updating its value in someway or removing it in someway, invalidation event is sent for that entry.

There are two types of invalidation event sending as single or batch. Batch event sending is advised if there are so many mutating operations such as put and remove on cache. This reduces network traffic and keeps event system busy less. 

Here are configurations for batch event sending:
- `hazelcast.cache.invalidation.batch.enabled`: Defines cache invalidation event batch sending is enabled or not. The default value is `true`.
- `hazelcast.cache.invalidation.batch.size`: Defines the maximum number of cache invalidation events to be drained and sent to the event listeners in a batch. The default value is `100`.
- `hazelcast.cache.invalidation.batchfrequency.seconds`: Defines cache invalidation event batch sending frequency in seconds. When event size does not reach to `hazelcast.cache.invalidation.batch.size` in the given time period, those events are gathered into a batch and sent to target. The default value is `5` seconds.

So if there are so many clients or so many mutating operation, batching should remain enabled and batch size should be configured with `hazelcast.cache.invalidation.batch.size` for a suitable value.

#### Expiration

Expiration means that evicting expired records. 
So a record is expired, 
- If it is not touched (accessed/read) during `<max-idle-seconds>`
- `<time-to-live-seconds>` passed since it is put to near-cache

Expiration are done in two cases:
- When a record is accessed, it is checked about if it is expired or not. If it is expired, it is evicted and returns `null` to caller.
- At background there is an expiration task that periodically (currently 5 seconds) scans records and evicts expired records.

#### Eviction

In Near-Cache scope, eviction means that evicting (clearing) entries selected as specified eviction policy when specified max-size policy has been reached.
Eviction is handled with max-size policy and eviction policy configurations.

##### Max-Size Policy

Max-Size policy defines the state when near-cache is full and eviction should be triggered.
The following max-size policies are supported by near-cache eviction:
- **ENTRY_COUNT:** Decides maximum based on the count of the number of entries in the near-cache. 
Available only for `BINARY` and `OBJECT` in-memory formats.
- **USED_NATIVE_MEMORY_SIZE:** Decides maximum used native memory size in MB of the specified near-cache to trigger eviction. 
If used native memory size by the specified near-cache exceeds this threshold, eviction is done. 
Available only for `NATIVE` in-memory format (supported only by Enterprise module).
- **USED_NATIVE_MEMORY_PERCENTAGE:** Decides maximum used native memory percentage of the specified near-cache to trigger eviction. 
If used native memory percentage (relative to maximum native memory size) by the specified near-cache exceeds this threshold, eviction is done. 
Available only for `NATIVE` in-memory format (supported only by Enterprise module).
- **FREE_NATIVE_MEMORY_SIZE:** Decides minimum free native memory size in MB of the specified near-cache to trigger eviction. 
If free native memory size by the specified near-cache goes down below of this threshold, eviction is done.
Available only for `NATIVE` in-memory format (supported only by Enterprise module).
- **FREE_NATIVE_MEMORY_PERCENTAGE:** Decides minimum free native memory percentage of the specified near-cache to trigger eviction. 
If free native memory percentage (relative to maximum native memory size) by the specified near-cache goes down below of this threshold, eviction is done.
Available only for `NATIVE` in-memory format (supported only by Enterprise module).

##### Eviction Policy 
Once a near-cache is full (reached to its max size as specified with max-size policy), an eviction policy determines which, if any, entries must be evicted.
Currently, the following eviction policies are supported by near-cache eviction: 
- LRU
- LFU

#### Configuration

Here are properties of near-cache configuration:
- **In Memory Format:** Defines the storage type of near cache entries. Valid values are `BINARY`, `OBJECT` and `NATIVE_MEMORY`. `NATIVE_MEMORY` is only available for enterprise module. Default value is `BINARY`.
- **Cache Local Entries:** Defines should the local cache entries be stored eagerly (immediately) to near cache on cache put from local. Valid values are `true` and `false`. Default value is `false`.
- **Invalidate on Change:** Defines should the cached entries be evicted if the entries are changed (updated or removed) from local and global also. Valid values are `true` and `false`. Default value is `true`.
- **Time to Live Seconds:** Maximum number of seconds for each entry to stay in the near cache. Entries that are older than `<time-to-live-seconds>` will get automatically evicted from the near cache. Any integer between `0` and `Integer.MAX_VALUE`. `0` means **infinite**. Default value is `0`.
- **Max Idle Seconds:** Maximum number of seconds each entry can stay in the near cache as untouched (not-read). Entries that are not read (touched) more than `<max-idle-seconds>` value will get removed from the near cache. Any integer between `0` and `Integer.MAX_VALUE`. `0` means `Integer.MAX_VALUE`. Default is `0`.
- **Eviction:** Eviction configuration about when eviction is triggered (max-size policy) and which eviction policy (`LRU` or `LFU`) will be used for selecting evicted entry. As default value, max-size policy is `ENTRY_COUNT`, size is `10000` and eviction policy is `LRU`. For HD Near-Cache since `ENTRY_COUNT` eviction policy is not supported yet, eviction must be explicitly configured with one of the supported policies `USED_NATIVE_MEMORY_SIZE`, `USED_NATIVE_MEMORY_PERCENTAGE`, `FREE_NATIVE_MEMORY_SIZE` and `FREE_NATIVE_MEMORY_PERCENTAGE`.

Near-Cache can be configured only at client side.

Here is a declarative near-cache configuration for a cache named `myCache`:

```xml
<hazelcast-client>
    ...
    <near-cache name="myCache">
        <!--
            Store cache entries in "BINARY" format.
        -->
        <in-memory-format>BINARY</in-memory-format>
        <!--
            Invalidate near-cache entries when entries are changed (updated or removed) from local or global.
        -->
        <invalidate-on-change>true</invalidate-on-change>
        <!--
            Don't put local cache entries eagerly to near-cache on local cache put.
        -->
        <cache-local-entries>false</cache-local-entries>
 
        <!--
            Evict entries these are in near-cache more than 1 hour.
        -->
        <time-to-live-seconds>3600000</time-to-live-seconds>
 
        <!--
            Evict entries these are not touched (not read) more than 10 minutes.
        -->
        <max-idle-seconds>600000</max-idle-seconds>
 
        <!--
            When entry count in near-cache reaches to 1000, 
            select entry to be evicted as LFU eviction policy and evict it.
        -->
        <eviction size="1000" max-size-policy="ENTRY_COUNT" eviction-policy="LFU"/>
    </near-cache>
    ...
</hazelcast-client>
```

Here is a programmatic near-cache configuration for a cache named `myCache`:

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

#### Notes About Client Near-Cache Configuration
Near-Cache configuration can be defined at client side (`hazelcast-client.xml` or `ClientConfig`) as independent configuration (independent from `CacheConfig`). 
So at client, near-cache configuration lookup is handled like this:
- Look for near-cache configuration with name of cache at client configuration
- If there is defined near-cache configuration found, use this near-cache configuration defined at client
- Otherwise, 
  * If there is defined default near-cache configuration found, use this default near-cache configuration
  * Otherwise, this means that, there is no near-cache configuration for cache
