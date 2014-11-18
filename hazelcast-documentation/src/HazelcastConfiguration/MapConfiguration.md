

### Map Configuration

**Declarative:**

```xml
<group>
   <name>MyGroup</name>
   <password>5551234</password>
</group>
```

**Programmatic:**

```java
Config config = new Config();
config.getGroupConfig().setName( "MyGroup" ).setPassword( "5551234" );
```


It has below attributes.

- in-memory-format: It is used to determine how the data will be stored in memory. It has two values: BINARY and OBJECT. BINARY is the default option and enables to store the data in serialized binary format. If OBJECT is set as the value, data will be stored in deserialized form.
- backup-count: Defines the count of synchronous backups. If it is set as 1, for example, backup of a partition will be placed on another node. If it is 2, it will be placed on 2 other nodes.
- async-backup-count: Defines the count of synchronous backups. Count behavior is the same as that of `backup-count` parameter.
- read-backup-data: This boolean parameter enables reading local backup entries when set as `true`.
- time-to-live-seconds: Maximum time in seconds for each entry to stay in the map.
- max-idle-seconds: Maximum time in seconds for each entry to stay idle in the map.
- eviction-policy: Policy for evicting entries. It has three values: NONE, LRU (Least Recently Used) and LFU (Least Frequently Used). If set as NONE, no items will be evicted.
- max-size:Maximum size of the map (i.e. maximum entry count of the map).  When maximum size is reached, map is evicted based on the eviction policy defined. It has four attributes: PER_NODE (Maximum number of map entries in each JVM), PER_PARTITION (Maximum number of map entries within each partition), USED_HEAP_SIZE (Maximum used heap size in megabytes for each JVM) and USED_HEAP_PERCENTAGE (Maximum used heap size percentage for each JVM). 
- eviction-percentage: When `max-size` is reached, specified percentage of the map will be evicted.
- merge-policy: Policy for merging maps after a split-brain syndrome was detected and the different network partitions need to be merged. Available merge policy classes are explained below:
	- HigherHitsMapMergePolicy causes the merging entry to be merged from source to destination map if source entry has more hits than the destination one.
	- LatestUpdateMapMergePolicy causes the merging entry to be merged from source to destination map if source entry has updated more recently than the destination entry. This policy can only be used of the clocks of the machines are in sync.
	- PassThroughMergePolicy causes the merging entry to be merged from source to destination map unless merging entry is null.
PutIfAbsentMapMergePolicy causes the merging entry to be merged from source to destination map if it does not exist in the destination map.
- statistics-enabled: You can retrieve some statistics like owned entry count, backup entry count, last update time, locked entry count by setting this parameter's value as "true". The method for retrieving the statistics is `getLocalMapStats()`.
- wan-replication-ref: Hazelcast can replicate some or all of the cluster data. For example, you can have 5 different maps but you want only one of these maps replicating across clusters. To achieve this you mark the maps to be replicated by adding this element in the map configuration.
- partition-strategy: ???
- optimize-queries: This parameter is used to increase the speed of query processes in the map. It only works when `in-memory-format` is set as `BINARY` and performs a pre-caching on the entries queried.

#### Map Store

- class-name: Name of the class implementing MapLoader and/or MapStore.
- write-delay-seconds: Number of seconds to delay to call the MapStore.store(key, value). If the value is zero then it is write-through so MapStore.store(key, value) will be called as soon as the entry is updated. Otherwise it is write-behind so updates will be stored after write-delay-seconds value by calling Hazelcast.storeAll(map). Default value is 0.
- write-batch-size: Used to create batch chunks when writing map store. In default mode all entries will be tried to persist in one go. To create batch chunks, minimum meaningful value for write-batch-size is 2. For values smaller than 2, it works as in default mode.

#### Near Cache

Most of map near cache properties have the same names and tasks explained in map properties above. Below are the ones specific to near cache.

- invalidate-on-change: Determines whether the cached entries get evicted if the entries are updated or removed).
- cache-local-entries: If you want the local entries to be cached, set this parameter's value as "true".

#### Indexes
This configuration lets you index the attributes and also order them. See the above sample declarative and programmatic configuration.

#### Entry Listeners
This configuration lets you add listeners (listener classes) for the map entries. You can also set the attributes `include-value` to `true` if you want the entry event to contain the entry values and `local` to `true` if you want to listen the entries on the local node.


