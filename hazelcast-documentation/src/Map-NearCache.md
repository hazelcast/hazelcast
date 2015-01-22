

### Near Cache

Map entries in Hazelcast are partitioned across the cluster. Imagine that you are reading the key `k` so many times and `k` is owned by another member in your cluster. Each `map.get(k)` will be a remote operation, meaning lots of network trips. If you have a map that is read-mostly, then you should consider creating a Near Cache for the map so that reads can be much faster and consume less network traffic. All these benefits do not come free. When using Near Cache, you should consider the following issues:

-   JVM will have to hold extra cached data so it will increase the memory consumption.

-   If invalidation is turned on and entries are updated frequently, then invalidations will be costly.

-   Near Cache breaks the strong consistency guarantees; you might be reading stale data.

Near Cache is highly recommended for the maps that are read-mostly. Here is a Near Cache configuration for a map:

```xml
<hazelcast>
  ...
  <map name="my-read-mostly-map">
    ...
    <near-cache>
      <!--
        Maximum size of the near cache. When max size is reached,
        cache is evicted based on the policy defined.
        Any integer between 0 and Integer.MAX_VALUE. 0 means
        Integer.MAX_VALUE. Default is 0.
      -->
      <max-size>5000</max-size>
      
      <!--
        Maximum number of seconds for each entry to stay in the near cache. Entries that are
        older than <time-to-live-seconds> will get automatically evicted from the near cache.
        Any integer between 0 and Integer.MAX_VALUE. 0 means infinite. Default is 0.
      -->
      <time-to-live-seconds>0</time-to-live-seconds>

      <!--
        Maximum number of seconds each entry can stay in the near cache as untouched (not-read).
        Entries that are not read (touched) more than <max-idle-seconds> value will get removed
        from the near cache.
        Any integer between 0 and Integer.MAX_VALUE. 0 means
        Integer.MAX_VALUE. Default is 0.
      -->
      <max-idle-seconds>60</max-idle-seconds>

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
        Should the cached entries get evicted if the entries are changed (updated or removed).
        true of false. Default is true.
      -->
      <invalidate-on-change>true</invalidate-on-change>

      <!--
        You may want also local entries to be cached.
        This is useful when in memory format for near cache is different than the map's one.
        By default it is disabled.
      -->
      <cache-local-entries>false</cache-local-entries>
    </near-cache>
  </map>
</hazelcast>
```

![image](images/NoteSmall.jpg) ***NOTE:*** *Programmatically, Near Cache configuration is done by using the class [NearCacheConfig](https://github.com/hazelcast/hazelcast/blob/607aa5484958af706ee18a1eb15d89afd12ee7af/hazelcast/src/main/java/com/hazelcast/config/NearCacheConfig.java). And this class is used both in the nodes and clients. In a client/server system, you must enable the Near Cache separately on the client, without needing to configure it on the server. For information on how to create a Near Cache on a client (native Java client), please see the [NearCacheConfig section](#nearcacheconfig) of [Java Client](#java-client). Please note that Near Cache configuration is specific to the node or client itself, a map in a node may not have near cache configured while the same map in a client may have.*

![image](images/NoteSmall.jpg) ***NOTE:*** *If you are using Near Cache, you should take into account that your hits to the keys in Near Cache are not reflected as hits to the original keys on the remote nodes; this has an impact on IMap's maximum idle seconds or time-to-live seconds expiration. Therefore, even there is a hit on a key in Near Cache, your original key on the remote node may expire.*

![image](images/NoteSmall.jpg) ***NOTE:*** *Near Cache works only when you access data via `map.get(k)` methods.  Data returned using a predicate is not stored in the near cache*