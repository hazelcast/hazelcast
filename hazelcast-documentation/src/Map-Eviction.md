
### Map Eviction

Unless you delete the map entries manually or use an eviction policy, they will remain in the map. Hazelcast supports policy based eviction for distributed maps. Currently supported policies are LRU (Least Recently Used) and LFU (Least Frequently Used).

Map eviction works based on the size of a partition. For example, once you specify a size using the `PER_NODE` attribute for `max-size` (please see [Configuring Map Eviction](#configuring-map-eviction)), Hazelcast internally calculates the maximum size for every partition. Eviction process starts according to this calculated per-partition maximum size when you try to put an entry. Below section gives an example scenario.

#### Example Map Eviction Scenario

Assume that you have the following figures:

* Partition count: 200
* Entry count for each partition: 100
* `max-size` (PER_NODE): 20000
* `eviction-percentage` (please see [Configuring Map Eviction](#configuring-map-eviction)):  10%

The total number of entries here is 20000 (partition count * entry count for each partition). This means you are at the eviction threshold since you set the `max-size` to 20000. When you try to put an entry:

1. Entry goes to the relevant partition.
2. Partition checks whether the eviction threshold is reached (`max-size`).
3. If reached, approximately 10 (100 * 10%) entries are evicted from that particular partition.

As a result of this eviction process, when you check the size of your map, it is ~19990 (20000 - ~10). After this eviction, subsequent put operations will not trigger the next eviction until the map size is again close to the `max-size`.

![image](images/NoteSmall.jpg) ***NOTE:*** *Above scenario is just an example to describe how the eviction process works. Hazelcast finds the most optimum number of entries to be evicted according to your cluster size and selected policy.*


#### Configuring Map Eviction

The following is an example declarative configuration for map eviction. 

```xml
<hazelcast>
  <map name="default">
    ...
    <time-to-live-seconds>0</time-to-live-seconds>
    <max-idle-seconds>0</max-idle-seconds>
    <eviction-policy>LRU</eviction-policy>
    <max-size policy="PER_NODE">5000</max-size>
    <eviction-percentage>25</eviction-percentage>
    <min-eviction-check-millis>100</min-eviction-check-millis>
    ...
  </map>
</hazelcast>
```

Let's describe each element. 

- `time-to-live`: Maximum time in seconds for each entry to stay in the map. If it is not 0, entries that are older than this time and not updated for this time are evicted automatically. Valid values are integers between 0 and `Integer.MAX VALUE`. Default value is 0, which means infinite. If it is not 0, entries are evicted regardless of the set `eviction-policy`.  
- `max-idle-seconds`: Maximum time in seconds for each entry to stay idle in the map. Entries that are idle for more than this time are evicted automatically. An entry is idle if no `get`, `put` or `containsKey` is called. Valid values are integers between 0 and `Integer.MAX VALUE`. Default value is 0, which means infinite.
- `eviction-policy`: Valid values are described below.
	- NONE: Default policy. If set, no items will be evicted and the property `max-size` will be ignored.  You still can combine it with `time-to-live-seconds` and `max-idle-seconds`.
	- LRU: Least Recently Used.
	- LFU: Least Frequently Used.	

- `max-size`: Maximum size of the map. When maximum size is reached, the map is evicted based on the policy defined. Valid values are integers between 0 and `Integer.MAX VALUE`. Default value is 0. If you want `max-size` to work, set the `eviction-policy` property to a value other than NONE. Its attributes are described below.
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

- `eviction-percentage`: When `max-size` is reached, the specified percentage of the map will be evicted. For example, if set to 25, 25% of the entries will be evicted. Setting this property to a smaller value will cause eviction of a smaller number of map entries. Therefore, if map entries are inserted frequently, smaller percentage values may lead to overheads. Valid values are integers between 0 and 100. The default value is 25.
- `min-eviction-check-millis`: The minimum time in milliseconds which should elapse before checking whether a partition of the map is evictable or not. In other terms, this property specifies the frequency of the eviction process. The default value is 100. Setting it to 0 (zero) makes the eviction process run for every put operation.

![image](images/NoteSmall.jpg) ***NOTE:*** *When map entries are inserted frequently, the property `min-eviction-check-millis` should be set to a number lower than the insertion period in order not to let any entry escape from the eviction.*


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
     

  

