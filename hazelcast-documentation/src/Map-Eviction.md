
### Eviction

Unless you delete the map entries manually or use an eviction policy, they will remain in the map. Hazelcast supports policy based eviction for distributed maps. Currently supported policies are LRU (Least Recently Used) and LFU (Least Frequently Used). 

There are other eviction properties, shown in the following sample declarative configuration. 

```xml
<hazelcast>
  <map name="default">
    ...
    <time-to-live-seconds>0</time-to-live-seconds>
    <max-idle-seconds>0</max-idle-seconds>
    <eviction-policy>LRU</eviction-policy>
    <max-size policy="PER_NODE">5000</max-size>
    <eviction-percentage>25</eviction-percentage>
    ...
  </map>
</hazelcast>
```

Let's describe each property. 

-	`time-to-live`: Maximum time in seconds for each entry to stay in the map. If it is not 0, entries that are older than this time and not updated for this time are evicted automatically. Valid values are integers between 0 and `Integer.MAX VALUE`. Default value is 0, which means infinite. If it is not 0, entries are evicted regardless of the set `eviction-policy`.  
-	`max-idle-seconds`: Maximum time in seconds for each entry to stay idle in the map. Entries that are idle for more than this time are evicted automatically. An entry is idle if no `get`, `put` or `containsKey` is called. Valid values are integers between 0 and `Integer.MAX VALUE`. Default value is 0, which means infinite.
-	`eviction-policy`: Valid values are described below.
	- NONE: Default policy. If set, no items will be evicted and the property `max-size` will be ignored.  You still can combine it with `time-to-live-seconds` and `max-idle-seconds`.
	- LRU: Least Recently Used.
	- LFU: Least Frequently Used.	

-	`max-size`: Maximum size of the map. When maximum size is reached, the map is evicted based on the policy defined. Valid values are integers between 0 and `Integer.MAX VALUE`. Default value is 0. If you want `max-size` to work, set the `eviction-policy` property to a value other than NONE. Its attributes are described below.
	- `PER_NODE`: Maximum number of map entries in each JVM. This is the default policy.	
	
		`<max-size policy="PER_NODE">5000</max-size>`
		
	- `PER_PARTITION`: Maximum number of map entries within each partition. Storage size depends on the partition count in a JVM. This attribute should not be used often. Avoid using this attribute with a small cluster: if the cluster is small it will be hosting more partitions, and therefore map entries, than that of a larger cluster. Thus, for a small cluster, eviction of the entries will decrease performance (the number of entries is large).
	
		`<max-size policy="PER_PARTITION">27100</max-size>`

	- `USED_HEAP_SIZE`: Maximum used heap size in megabytes for each JVM.
	
		`<max-size policy="USED_HEAP_SIZE">4096</max-size>`

	- `USED_HEAP_PERCENTAGE`: Maximum used heap size percentage for each JVM. If, for example, JVM is configured to have 1000 MB and this value is 10, then the map entries will be evicted when used heap size exceeds 100 MB.
	
		`<max-size policy="USED_HEAP_PERCENTAGE">10</max-size>`

	- `FREE_HEAP_SIZE`: Minimum free heap size in megabytes for each JVM.

		`<max-size policy="FREE_HEAP_SIZE">512</max-size>`

	- `FREE_HEAP_PERCENTAGE`: Minimum free heap size percentage for each JVM. If, for example, JVM is configured to have 1000 MB and this value is 10, then the map entries will be evicted when free heap size is below 100 MB.

		`<max-size policy="FREE_HEAP_PERCENTAGE">10</max-size>`

-	`eviction-percentage`: When `max-size` is reached, the specified percentage of the map will be evicted. For example, if set to 25, 25% of the entries will be evicted. Setting this property to a smaller value will cause eviction of a smaller number of map entries. Therefore, if map entries are inserted frequently, smaller percentage values may lead to overheads. Valid values are integers between 0 and 100. The default value is 25.


#### Sample Eviction Configuration


```xml
<map name="documents">
  <max-size policy="PER_NODE">10000</max-size>
  <eviction-policy>LRU</eviction-policy> 
  <max-idle-seconds>60</max-idle-seconds>
</map>
```

In the above sample, `documents` map starts to evict its entries from a member when the map size exceeds 10000 in that member. Then, the entries least recently used will be evicted. The entries not used for more than 60 seconds will be evicted as well.


#### Evicting Specific Entries


The eviction policies and configurations explained above apply to all the entries of a map. The entries that meet the specified eviction conditions are evicted.


But you may want to evict some specific map entries.  In this case, you can use the `ttl` and `timeunit` parameters of the method `map.put()`. A sample code line is given below.

`myMap.put( "1", "John", 50, TimeUnit.SECONDS )`

The map entry with the key "1" will be evicted 50 seconds after it is put into `myMap`.


#### Evicting All Entries

The method `evictAll()` evicts all keys from the map except the locked ones. If a MapStore is defined for the map, `deleteAll` is not called by `evictAll`. If you want to call the method `deleteAll`, use `clear()`. 

A sample is given below.

```java
public class EvictAll {

    public static void main(String[] args) {
        final int numberOfKeysToLock = 4;
        final int numberOfEntriesToAdd = 1000;

        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        IMap<Integer, Integer> map = node1.getMap(EvictAll.class.getCanonicalName());
        for (int i = 0; i < numberOfEntriesToAdd; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < numberOfKeysToLock; i++) {
            map.lock(i);
        }

        // should keep locked keys and evict all others.
        map.evictAll();

        System.out.printf("# After calling evictAll...\n");
        System.out.printf("# Expected map size\t: %d\n", numberOfKeysToLock);
        System.out.printf("# Actual map size\t: %d\n", map.size());

    }
}
```


![image](images/NoteSmall.jpg) ***NOTE:*** *Only EVICT_ALL event is fired for any registered listeners.*
     

  

