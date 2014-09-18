
### Entry Processor

Hazelcast supports entry processing. The interface EntryProcessor gives you the ability to execute your code on an entry in an atomic way. You do not need any explicit lock on entry. Practically, Hazelcast locks the entry, runs the EntryProcessor, and then unlocks the entry. If entry processing is the major operation for a map and the map consists of complex objects, then using object type as `in-memory-format` is recommended to minimize serialization cost.

There are below methods in IMap interface for entry processing:

```java
/**
 	* Applies the user defined EntryProcessor to the entry mapped by the key.
 	* Returns the the object which is result of the process() method of EntryProcessor.
 	*/
 	
	Object executeOnKey(K key, EntryProcessor entryProcessor);

/**
     * Applies the user defined EntryProcessor to the entries mapped by the collection of keys.
     * the results mapped by each key in the collection.
     */
    
    Map<K,Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor);

/**
     * Applies the user defined EntryProcessor to the entry mapped by the key with
     * specified ExecutionCallback to listen event status and returns immediately.
     */

    void submitToKey(K key, EntryProcessor entryProcessor, ExecutionCallback callback);


/**
 	* Applies the user defined EntryProcessor to the all entries in the map.
 	* Returns the results mapped by each key in the map.
 	*/
 	
	Map<K,Object> executeOnEntries(EntryProcessor entryProcessor);
	 
/**
     * Applies the user defined EntryProcessor to the entries in the map which satisfies provided predicate.
     * Returns the results mapped by each key in the map.
     */

    Map<K,Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate);
	```

Using `executeOnEntries	` method, if the number of entries is high and you do need the results, then returning null in `process(..)` method is a good practice.

Here is the EntryProcessor interface:

```java
public interface EntryProcessor<K, V> extends Serializable {

    Object process(Map.Entry<K, V> entry);

    EntryBackupProcessor<K, V> getBackupProcessor();
}
```

If your code is modifying the data, then you should also provide a processor for backup entries:

***NOTE***: You should explicitly call ```setValue``` method of ```Map.Entry``` when modifying data in EP otherwise EP will be accepted as read-only.

```java
public interface EntryBackupProcessor<K, V> extends Serializable {

    void processBackup(Map.Entry<K, V> entry);
}
```

**Example Usage:**

```java
public class EntryProcessorTest {

    @Test
    public void testMapEntryProcessor() throws InterruptedException {
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        map.executeOnKey(1, entryProcessor);
        assertEquals(map.get(1), (Object) 2);
        instance1.getLifecycleService().shutdown();
        instance2.getLifecycleService().shutdown();
    }

    @Test
    public void testMapEntryProcessorAllKeys() throws InterruptedException {
        StaticNodeFactory nodeFactory = new StaticNodeFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessorAllKeys");
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        Map<Integer, Object> res = map.executeOnEntries(entryProcessor);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), (Object) (i+1));
        }
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i)+1, res.get(i));
        }
        instance1.getLifecycleService().shutdown();
        instance2.getLifecycleService().shutdown();
    }

    static class IncrementorEntryProcessor implements EntryProcessor, EntryBackupProcessor, Serializable {
        public Object process(Map.Entry entry) {
            Integer value = (Integer) entry.getValue();
            entry.setValue(value + 1);
            return value + 1;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return IncrementorEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {
            entry.setValue((Integer) entry.getValue() + 1);
        }
    }
}
```
