### Eviction

Hazelcast also supports policy based eviction for distributed map. Currently supported eviction policies are LRU (Least Recently Used) and LFU (Least Frequently Used). This feature enables Hazelcast to be used as a distributed cache. If `time-to-live-seconds` is not 0, entries older than `time-to-live-seconds` value will get evicted,
regardless of the eviction policy set. Here is a sample configuration for eviction:

Note that, LRU/LFU evictions work if statistics are enabled.

```xml
<hazelcast>
    ...
    <map name="default">
        <!--
            Number of backups. If 1 is set as the backup-count for example,
            then all entries of the map will be copied to another JVM for
            fail-safety. Valid numbers are 0 (no backup), 1, 2, 3.
        -->
        <backup-count>1</backup-count>

        <!--
            Maximum number of seconds for each entry to stay in the map. Entries that are
            older than <time-to-live-seconds> will get automatically evicted from the map.
            Any integer between 0 and Integer.MAX_VALUE. 0 means infinite. Default is 0.
        -->
        <time-to-live-seconds>0</time-to-live-seconds>

        <!--
            Maximum number of seconds for each entry to stay idle in the map. Entries that are
            idle(not touched) for more than <max-idle-seconds> will get
            automatically evicted from the map.
            Entry is touched if get, put or containsKey is called.
            Any integer between 0 and Integer.MAX_VALUE.
            0 means infinite. Default is 0.
        -->
        <max-idle-seconds>0</max-idle-seconds>

        <!--
            Valid values are:
            NONE (no extra eviction, <time-to-live-seconds> may still apply),
            LRU  (Least Recently Used),
            LFU  (Least Frequently Used).
            NONE is the default.
            Regardless of the eviction policy used, <time-to-live-seconds> will still apply. 
        -->
        <eviction-policy>LRU</eviction-policy>

        <!--
            Maximum size of the map. When max size is reached,
            map is evicted based on the policy defined.
            Any integer between 0 and Integer.MAX_VALUE. 0 means
            Integer.MAX_VALUE. Default is 0.
        -->
        <max-size policy="PER_NODE">5000</max-size>

        <!--
            When max. size is reached, specified percentage of
            the map will be evicted. Any integer between 0 and 100.
            If 25 is set for example, 25% of the entries will
            get evicted.
        -->
        <eviction-percentage>25</eviction-percentage>
    </map>
</hazelcast>
```

**`max-size` Policies**

There are 4 defined policies can be used in max-size configuration.

1.  **PER\_NODE:** Max map size per instance.

    ```
    <max-size policy="PER_NODE">5000</max-size>
    ```
    
2.  **PER\_PARTITION:** Max map size per each partition.

    ```
    <max-size policy="PER_PARTITION">27100</max-size>
    ```
    
3.  **USED\_HEAP\_SIZE:** Max used heap size in MB (mega-bytes) per JVM.

    ```
    <max-size policy="USED_HEAP_SIZE">4096</max-size>
    ```
    
4.  **USED\_HEAP\_PERCENTAGE:** Max used heap size percentage per JVM.

    ```
    <max-size policy="USED_HEAP_PERCENTAGE">75</max-size>
    ```

