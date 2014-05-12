


## Entry Processor

### Overview

Hazelcast supports entry processing. Entry processor is a function that executes your code on a map entry in an atomic way. 

Entry processor enables fast in-memory operations on a map without having to worry about locks or concurrency issues. It can be applied to a single map entry or on all map entries and supports choosing target entries using predicates. You do not need any explicit lock on entry. Practically, Hazelcast locks the entry, runs the EntryProcessor, and then unlocks the entry.

Hazelcast sends the entry processor to each cluster member and these members apply it to map entries. So, if you add more members, your processing is completed faster.

If entry processing is the major operation for a map and the map consists of complex objects, then using `OBJECT` as `in-memory-format` is recommended to minimize serialization cost. By default, the entry value is stored as a byte array (BINARY format), but when it is stored as an object (OBJECT format), then entry processor is applied directly on the object. In that case, no serialization or deserialization is performed.

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

And, here is the EntryProcessor interface:

```java
public interface EntryProcessor<K, V> extends Serializable {

    Object process(Map.Entry<K, V> entry);

    EntryBackupProcessor<K, V> getBackupProcessor();
}
```

<font color="red">***Note***</font>: *If you want to execute a task on a single key, you can also use `executeOnKeyOwner` provided by Executor Service. But, in this case, you need to perform a lock and serialization.*

When using `executeOnEntries` method, if the number of entries is high and you do need the results, then returning null in `process()` method is a good practice. By this way, results of processings are not stored in the map and hence out of memory errors are eliminated.


If your code is modifying the data, then you should also provide a processor for backup entries:

```java
public interface EntryBackupProcessor<K, V> extends Serializable {

    void processBackup(Map.Entry<K, V> entry);
}
```

This is required to prevent the primary map entries from having different values than backups. Because entry processor is applied both on primary and backup entries.



### Sample Entry Processor Code

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

### Abstract Entry Processor

`AbstractEntryProcessor` class can be used when the same processing will be performed both on primary and backup map entries (i.e. same logic applies to them). If `EntryProcessor` is used, you need to apply the same logic to backup entries separately. `AbstractEntryProcessor` class brings an easiness on this primary/backup processing.

Please see below sample code.

```java
public abstract class AbstractEntryProcessor <K, V> implements EntryProcessor <K, V> {	private final EntryBackupProcessor <K,V> entryBackupProcessor;	public AbstractEntryProcessor(){ this(true);	}	public AbstractEntryProcessor(boolean applyOnBackup { if(applyOnBackup){		entryBackupProcessor = new EntryBackupProcessorImpl(); }else{		entryBackupProcessor = null; }} 
@Overridepublic abstract Object process(Map.Entry<K, V> entry);@Overridepublic final EntryBackupProcessor <K, V> getBackupProcessor() {	return entryBackupProcessor; 
	private class EntryBackupProcessorImplimplements EntryBackupProcessor <K,V>{	@Override	public void processBackup(Map.Entry<K, V> entry) {		process(entry); 
		}	}	}```

In the above sample, the method `getBackupProcessor` returns an `EntryBackupProcessor` instance. This means, the same processing will be applied to both primary and backup entries. If you want to apply the processing only on the primary entries, then `getBackupProcessor` method should return null. 


### Threading

Hazelcast allows a single thread for entry processing (partition thread). Meaning, no other operations can be performed on a map entry while entry processor is running on it. And, no operations can be performed on a partition different than the current one occupied by the partition thread. Yet, entry processor can call operations on the current partition (for example it can retrieve information from another map needed for processing).


Entry processor runs on a batch of map entries at a time. During this time the partition thread is kept by it. After the batch is processed, it releases the thread and reschedules itself for the next batch. Other map operations run between each batch processing.

Entry processor provides a property to configure batch size:

-	`hazelcast.entryprocessor.batch.max.size`: Specifies the  maximum number of map entries to be executed in a single batch. Its default value is 10.000.

Hazelcast will end the processing and start to schedule for the next batch once this maximum size is reached.

<br> </br>

<font color="red">
***Related Information***
</font>

*Please refer to [Configuration](#configuration) for information on how to set configuration properties.*

<br> </br>




<br> </br>
