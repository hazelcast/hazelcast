
## Cache Configuration

Following are examples of cache configurations.


**Declarative:**

```xml
<cache name="default">
   <key-type class-name="exampleClass"></key-type>
   <value-type class-name="exampleClass"></value-type>
   <statistics-enabled>true</statistics-enabled>
   <management-enabled>true</management-enabled>
   <read-through>false</read-through>
   <write-through>true</write-through>
   <cache-loader-factory class-name="exampleClass"></cache-loader-factory>
   <cache-writer-factory class-name="exampleClass"></cache-writer-factory>
   <expiry-policy-factory class-name="exampleClass"></expiry-policy-factory>
   <cache-entry-listeners>
      <cache-entry-listener old-value-required="true">
         <cache-entry-listener-factory class-name="exampleClass"</cache-entry-listener-factory>
         <cache-entry-event-filter-factory class-name="exampleClass"</cache-entry-event-filter-factory>
      </cache-entry-listener>
   <cache-entry-listeners>
   <in-memory-format>BINARY</in-memory-format>
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <eviction size="10000" max-size-policy="ENTRY_COUNT"></eviction>
</cache>
```

**Programmatic:**

```java
ICache<Object, Object> cache = cacheManager.getCache().unwrap(ICache.class);
CacheConfig cacheConfig = cache.getConfiguration(CacheConfig.class);

cacheConfig.setStatisticsEnabled( true ).setManagementEnabled( true );
cacheConfig.setReadThrough( true ).setWriteThrough( true );
           .setBackupCount( "1" )
           .setAsyncBackupCount( "0" );
           
CacheEvictionConfig cacheEvictionConfig = cacheConfig.getEvictionConfig();
cacheEvictionConfig.setSize( "10000" ).setMaxSizePolicy( CacheMaxSizePolicy.ENTRY_COUNT );
```
   

Cache configuration has the following elements.

- `name`: Name of the cache.
- `key-type`: Type of the keys provided as a full class name.
- `value-type`: Type of the values provided as a full class name. 
- `statistics-enabled`: If set as `true`, you can retrieve statistics for this cache using the `getLocalCacheStatistics()` method.
- `management-enabled`: Defines whether the management is enabled on this cache. If you set it to `true`, you can monitor this cache on the Hazelcast Management Center.
- `read-through`: If you want to use the read-through caching mode, set this value to `true`.
- `write-through`: If you want to use the write-through caching mode, set this value to `true`. 
- `cache-loader-factory`: Defines the cache loader factory class name.
- `cache-writer-factory`: Defines the cache writer factory class name.
- `expiry-policy-factory`: Defines the expiry policy factory class name.
- `cache-entry-listeners`: Includes the list of cache entry listeners.
- `in-memory-format`: Data type that will be used for storing the records. Possible values are `BINARY` (default), `OBJECT` and `NATIVE`. If you set it to `BINARY`, the keys and values will be stored as binary data. If you set it to `OBJECT`, the values will be stored in their object forms. If it is `NATIVE`, the keys and values will be stored in the native memory.
- `backup-count`: Count of synchronous backups.
- `async-backup-count`: Count of asynchronous backups.
- `eviction`: When the maximum size is reached, the cache is evicted based on the eviction policy. It has the following attributes.
	-  `size`: The cache size can be any integer between 0 and Integer.MAX_VALUE. Default value is 0. 
	- `max-size-policy`: Available policies are listed below.
		- ENTRY_COUNT: Maximum number of cache entries in the cache. This is the default value.
		- USED_NATIVE_MEMORY_SIZE: Maximum used native memory size in megabytes for each JVM.
		- USED_NATIVE_MEMORY_PERCENTAGE: Maximum used native memory size percentage for each JVM. 
		- FREE_NATIVE_MEMORY_SIZE: Maximum free native memory size in megabytes for each JVM.
		- FREE_NATIVE_MEMORY_PERCENTAGE: Maximum free native memory size percentage for each JVM. 
	- `eviction-policy`: Available policies are LRU (Least Recently Used) and LFU (Least Frequently Used). Default value is LRU.

