
### ICache Configuration

As mentioned in [JCache Declarative Configuration](#jcache-declarative-configuration), the Hazelcast ICache extension offers
additional configuration properties over the default JCache configuration. These additional properties include internal storage format, backup counts
and eviction policy.

The declarative configuration for ICache is a superset of the previously discussed JCache configuration:

```xml
<cache>
  <!-- ... default cache configuration goes here ... -->
  <backup-count>1</backup-count>
  <async-backup-count>1</async-backup-count>
  <in-memory-format>BINARY</in-memory-format>
  <eviction-policy>NONE</eviction-policy>
  <eviction-percentage>25</eviction-percentage>
  <eviction-threshold-percentage>25</eviction-threshold-percentage>
</cache>
```

- `backup-count`: The number of synchronous backups. Those backups are executed before the mutating cache operation is finished. The mutating operation is blocked. `backup-count` default value is 1.
- `async-backup-count`: The number of asynchronous backups. Those backups are executed asynchronously so the mutating operation is not blocked and it will be done immediately. `async-backup-count` default value is 0.  
- `in-memory-format`: Defines the internal storage format. For more information, please see [In Memory Format](#in-memory-format). Default is `BINARY`.
- `eviction-policy`: The eviction policy **(currently available on High-Density Memory Store only)** defines which entries are evicted (removed) from the cache when the cache is low in space. Its default value is `RANDOM`. The following eviction policies are available:
  - `LRU`: Abbreviation for Least Recently Used. When `eviction-policy` is set to `LRU`, the longest unused (not accessed) entries are removed from the cache.  
  - `LFU`: Abbreviation for Least Frequently Used. When `eviction-policy` is set to `LFU`, the entries that are used (accessed) least frequently are removed from the cache.
  - `RANDOM`: When `eviction-policy` is set to `RANDOM`, random entries are removed from the cache. No information about access frequency or last accessed time are taken into account.
  - `NONE`: When `eviction-policy` is set to `NONE`, no entries are removed from the cache at all.
- `eviction-percentage`: The eviction percentage property **(currently available on High-Density Memory Store only)** defines the amount of percentage of the cache that will be evicted when the threshold is reached. Can be set to any integer number between 0 and 100, defaults to 0.
- `eviction-threshold-percentage`: the eviction threshold property **(currently available on High-Density Memory Store only)** defines a threshold when reached to trigger the eviction process. Can be set to any integer number between 0 and 100, defaults to 0.

Since `javax.cache.configuration.MutableConfiguration` misses the above additional configuration properties, Hazelcast ICache extension
provides an extended configuration class called `com.hazelcast.config.CacheConfig`. This class is an implementation of `javax.cache.configuration.CompleteConfiguration` and all the properties shown above can be configured
using its corresponding setter methods.

