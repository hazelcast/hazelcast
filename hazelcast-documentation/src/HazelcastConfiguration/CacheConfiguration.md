
## Cache Configuration

The following are the example configurations.


**Declarative:**

```xml
<cache name="default">
   <key-type class-name="???"></key-type>
   <value-type class-name="???"></value-type>
   <statistics-enabled>true</statistics-enabled>
   <management-enabled>true</management-enabled>
   <read-through>false</read-through>
   <write-through>true</write-through>
   <cache-loader-factory class-name="???"></cache-loader-factory>
   <cache-writer-factory class-name="???"></cache-writer-factory>
   <expiry-policy-factory class-name="???"></expiry-policy-factory>
   <cache-entry-listeners>
      <cache-entry-listener old-value-required="true">
         <cache-entry-listener-factory class-name="???"</cache-entry-listener-factory>
         <cache-entry-event-filter-factory class-name="???"</cache-entry-event-filter-factory>
      </cache-entry-listener>
   <cache-entry-listeners>
   <in-memory-format>BINARY</in-memory-format>
   <statistics-enabled>true</statistics-enabled>
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <eviction size="10000" max-size-policy="ENTRY_COUNT"></eviction>
</cache>
```

**Programmatic:**

```java
Config config = new Config();
CacheConfig cacheCfg = config.getCacheConfig();

cacheCfg.setName( "default" );


```
   

It has below elements.


- `backup-count`: ???
- `async-backup-count`: ???.
- `statistics-enabled`: If set as `true`, you can retrieve statistics for this cache. ???
