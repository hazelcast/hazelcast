

## Map Configuration

The following are example map configurations.

**Declarative:**

```xml
<hazelcast>
  <map name="default">
    <in-memory-format>BINARY</in-memory-format>
    <backup-count>0</backup-count>
    <async-backup-count>1</async-backup-count>
    <read-backup-data>true</read-backup-data>
    <time-to-live-seconds>0</time-to-live-seconds>
    <max-idle-seconds>0</max-idle-seconds>
    <eviction-policy>LRU</eviction-policy>
    <max-size policy="PER_NODE">5000</max-size>
    <eviction-percentage>25</eviction-percentage>
    <near-cache>
       <invalidate-on-change>true</invalidate-on-change>
       <cache-local-entries>false</cache-local-entries>
    <near-cache>
    <map-store enabled="true">
       <write-delay-seconds>60</write-delay-seconds>
       <write-batch-size>1000</write-batch-size>
       <write-coalescing>true</write-coalescing>
    </map-store>
    <indexes>
        <index ordered="true">id</index>
        <index ordered="false">name</index>
    </indexes>
    <entry-listeners>
        <entry-listener include-value="true" local="false">
           com.hazelcast.examples.EntryListener
        </entry-listener>
    </entry-listeners>   
  </map>
</hazelcast>
```

**Programmatic:**

```java
MapConfig mapConfig = new MapConfig();
mapConfig.setName( "default" ).setInMemoryFormat( "BINARY" );

mapConfig.setBackupCount( "0" ).setAsyncBackupCount( "1" )
         .setReadBackupData( "true" );
         
MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
mapStoreConfig.setWriteDelaySeconds( "60" )
              .setWriteBatchSize( "1000" );
              
MapIndexConfig mapIndexConfig = mapConfig.getMapIndexConfig();
mapIndexConfig.setAttribute( "id" ).setOrdered( "true" );
mapIndexConfig.setAttribute( "name" ).setOrdered( "false" );
```


Map configuration has the following elements.

- `in-memory-format`: Determines how the data will be stored in memory. It has two values: BINARY and OBJECT. BINARY is the default option, storing the data in serialized binary format. If set to OBJECT, data will be stored in deserialized form.
- `backup-count`: Defines the count of synchronous backups. If it is set as 1, for example, backup of a partition will be placed on 1 other node. If it is 2, it will be placed on 2 other nodes.
- `async-backup-count`: The number of synchronous backups. Count behavior is the same as that of `backup-count` element.
- `read-backup-data`: When set to `true`, enables reading of local backup entries.
- `time-to-live-seconds`: Maximum time in seconds for each entry to stay in the map.
- `max-idle-seconds`: Maximum time in seconds for each entry to stay idle in the map.
- `eviction-policy`: Policy for evicting entries. It has three values: NONE, LRU (Least Recently Used) and LFU (Least Frequently Used). If set to NONE, no items will be evicted.
- `max-size`: Maximum size of the map (i.e. maximum entry count of the map). When the maximum size is reached, the map is evicted based on the eviction policy defined. It has four attributes: `PER_NODE` (Maximum number of map entries in each JVM), `PER_PARTITION` (Maximum number of map entries within each partition), `USED_HEAP_SIZE` (Maximum used heap size in megabytes for each JVM) and `USED_HEAP_PERCENTAGE` (Maximum used heap size percentage for each JVM). 
- `eviction-percentage`: When the map reaches `max-size`, this specified percentage of the map will be evicted.
- `merge-policy`: Policy for merging maps after a split-brain syndrome was detected and the different network partitions need to be merged. The available merge policy classes are explained below.
	- `HigherHitsMapMergePolicy` causes the merging entry to be merged from the source map to the destination map if the source entry has more hits than the destination one.
	- `LatestUpdateMapMergePolicy` causes the merging entry to be merged from the source map to the destination map if the source entry has updated more recently than the destination entry. This policy can only be used if the machine clocks are in sync.
	- `PassThroughMergePolicy` causes the merging entry to be merged from source to destination map unless merging entry is null.
	- `PutIfAbsentMapMergePolicy` causes the merging entry to be merged from source to destination map if it does not exist in the destination map.
- `statistics-enabled`: You can retrieve statistics information like owned entry count, backup entry count, last update time, and locked entry count by setting `statistics-enabled` to `true`. The method for retrieving the statistics is `getLocalMapStats()`.
- `wan-replication-ref`: Hazelcast can replicate some or all of the cluster data. For example, suppose you can have 5 different maps but you want only one of these maps to replicate across clusters. To achieve this, you mark the maps to be replicated by adding this element in the map configuration.
- `optimize-queries`: This element is used to increase the speed of query processes in the map. It only works when `in-memory-format` is set as `BINARY` and performs a pre-caching on the entries queried.

### Map Store

- `class-name`: Name of the class implementing MapLoader and/or MapStore.
- `write-delay-seconds`: Number of seconds to delay to call the MapStore.store(key, value). If the value is zero then it is write-through so MapStore.store(key, value) will be called as soon as the entry is updated. Otherwise it is write-behind so updates will be stored after write-delay-seconds value by calling Hazelcast.storeAll(map). Default value is 0.
- `write-batch-size`: Used to create batch chunks when writing map store. In default mode, all map entries will be tried to be written in one go. To create batch chunks, the minimum meaningful value for write-batch-size is 2. For values smaller than 2, it works as in default mode.
- `write-coalescing`: In write-behind mode, by default Hazelcast coalesces updates on a specific key, i.e. applies only the last update on it. You can set this element to `false` to store all updates performed on a key to the data store.

### Near Cache

Most of the map near cache properties have the same names and tasks explained in the map properties above. Below are the ones specific to near cache.

- `invalidate-on-change`: Determines whether the cached entries get evicted if the entries are updated or removed.
- `cache-local-entries`: If you want the local entries to be cached, set this element's value to `true`.

### Indexes
This configuration lets you index the attributes of an object in the map and also order these attributes. See the above declarative and programmatic configuration examples.

### Entry Listeners
This configuration lets you add listeners (listener classes) for the map entries. You can also set the attribute `include-value` to `true` if you want the entry event to contain the entry values, and you can set `local` to `true` if you want to listen to the entries on the local node.


