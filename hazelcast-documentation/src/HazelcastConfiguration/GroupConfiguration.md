
### Group Configuration

This configuration is to create multiple Hazelcast clusters. Each cluster will have its own group and it will not interfere with other clusters. Sample configurations are shown below.

**Declarative:**

```xml
<map name="default">
   <in-memory-format>BINARY</in-memory-format>
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <time-to-live-seconds>0</time-to-live-seconds>
   <max-idle-seconds>0</max-idle-seconds>
   <eviction-policy>NONE</eviction-policy>
   <max-size policy="PER_NODE">0</max-size>
   <eviction-percentage>25</eviction-percentage>
   <min-eviction-check-millis>100</min-eviction-check-millis>
   <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
   <map-store enabled="true">
      <class-name>com.hazelcast.examples.DummyStore</class-name>
      <write-delay-seconds>60</write-delay-seconds>
      <write-batch-size>2</write-batch-size>
   </map-store>
   <near-cache>
      <max-size>5000</max-size>
      <time-to-live-seconds>0</time-to-live-seconds>
      <max-idle-seconds>60</max-idle-seconds>
      <eviction-policy>LRU</eviction-policy>
      <invalidate-on-change>true</invalidate-on-change>
      <cache-local-entries>false</cache-local-entries>
   </near-cache>
   <wan-replication-ref name="my-wan-cluster"></wan-replication-ref>
   <indexes>
      <index ordered="false">name</index>
      <index ordered="true">age</index>
   </indexes>
   <entry-listeners>
      <entry-listener include-value="true" local="false">
         com.hazelcast.examples.EntryListener
      </entry-listener>
   </entry-listeners>
   <partition-strategy>???</partition-strategy>
</map>
```

**Programmatic:**

```java
Config config = new Config();
MapConfig mapConfig = config.getMapConfig();

mapConfig.setName("default").setInMemoryFormat("BINARY")
         .setBackupCount("1");

MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig(); 
mapStoreConfig.setEnabled("true").setClassName("com.hazelcast.examples.DummyStore");

NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
nearCacheConfig.setMaxSize("5000").setInvalidateOnChange("true");

MapIndexConfig mapIndexConfig = mapConfig.getMapIndexConfig();
mapIndexConfig.setAttribute("name").setOrdered("false"); 

EntryListenerConfig entryListenerConfig = mapConfig.getEntryListenerConfig();
entryListenerConfig.setLocal("false").setClassName("com.hazelcast.examples.EntryListener");
```
   

It has below parameters.


- `name`: Name of the group to be created.
- `password`: Password of the group to be created.


