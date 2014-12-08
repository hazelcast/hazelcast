
### ICache Configuration

As mentioned in the [JCache Declarative Configuration section](#jcache-declarative-configuration), the Hazelcast ICache extension offers
additional configuration properties over the default JCache configuration. These additional properties include internal storage format, backup counts
and eviction policy.

The declarative configuration for ICache is a superset of the previously discussed JCache configuration:

```xml
<cache>
  <!-- ... default cache configuration goes here ... -->
  <backup-count>1</backup-count>
  <async-backup-count>1</async-backup-count>
  <in-memory-format>BINARY</in-memory-format>
  <eviction-policy>LRU</eviction-policy>
  <max-size size="1000000" policy="ENTRY_COUNT"/>
</cache>
```

- `backup-count`: The number of synchronous backups. Those backups are executed before the mutating cache operation is finished. The mutating operation is blocked. `backup-count` default value is 1.
- `async-backup-count`: The number of asynchronous backups. Those backups are executed asynchronously so the mutating operation is not blocked and it will be done immediately. `async-backup-count` default value is 0.  
- `in-memory-format`: Defines the internal storage format. For more information, please see the [In Memory Format section](#in-memory-format). Default is `BINARY`.
- `eviction-policy`: The eviction policy defines which entries are evicted (removed) from the cache when the cache reaches its maximum size defined in `max-size` configuration. Its default value is `LRU`. The following eviction policies are available:
  - `LRU`: Abbreviation for Least Recently Used. When `eviction-policy` is set to `LRU`, the longest unused (not accessed) entry is removed from the cache.  
  - `LFU`: Abbreviation for Least Frequently Used. When `eviction-policy` is set to `LFU`, the entry that is used (accessed) least frequently is removed from the cache.
- `max-size`: The max size property defines a maximum size maximum size is reached, cache is evicted based on the eviction policy. Size can be any integer between `0` and `Integer.MAX_VALUE`. Default policy is `ENTRY_COUNT` and default size is `10.000`. The following eviction policies are available:
  - `ENTRY_COUNT`: Maximum number of cache entries in the cache. **Available on heap based cache record store only.**
  - `USED_NATIVE_MEMORY_SIZE`: Maximum used native memory size in megabytes for each instance. **Available on High-Density Memory cache record store only.**
  - `USED_NATIVE_MEMORY_PERCENTAGE`: Maximum used native memory size percentage for each instance. **Available on High-Density Memory cache record store only.**
  - `FREE_NATIVE_MEMORY_SIZE`: Maximum free native memory size in megabytes for each instance. **Available on High-Density Memory cache record store only.**
  - `FREE_NATIVE_MEMORY_PERCENTAGE`: Maximum free native memory size percentage for each instance. **Available on High-Density Memory cache record store only.**

Since `javax.cache.configuration.MutableConfiguration` misses the above additional configuration properties, Hazelcast ICache extension
provides an extended configuration class called `com.hazelcast.config.CacheConfig`. This class is an implementation of `javax.cache.configuration.CompleteConfiguration` and all the properties shown above can be configured
using its corresponding setter methods.

