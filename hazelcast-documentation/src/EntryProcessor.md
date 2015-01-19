

## Entry Processor

Hazelcast supports entry processing. An entry processor is a function that executes your code on a map entry in an atomic way. 

Entry Processor is a good option if you perform bulk processing on an `IMap`.  Usually, you might consider to perform a loop of keys executing `IMap.get(key)`, then mutating the value and finally putting the entry back in the map using `IMap.put(key,value)`.  If you are performing this process from a client or from a member where the keys do not exist, you effectively perform 2 network hops for each update. The first to retrieve the data and the second to update the mutated value.

If you are doing the above, you should consider Entry Processors. An Entry Processor executes a read and update upon the member where the data resides.  This eliminates the costly network hops as described previously.

### Entry Processor Overview

An entry processor enables fast in-memory operations on a map without having to worry about locks or concurrency issues. It can be applied to a single map entry or to all map entries. It supports choosing target entries using predicates. You do not need any explicit lock on entry: Hazelcast locks the entry, runs the EntryProcessor, and then unlocks the entry.

Hazelcast sends the entry processor to each cluster member and these members apply it to map entries. Therefore, if you add more members, your processing is completed faster.

If entry processing is the major operation for a map and if the map consists of complex objects, then using `OBJECT` as `in-memory-format` is recommended to minimize serialization cost. By default, the entry value is stored as a byte array (`BINARY` format). When it is stored as an object (`OBJECT` format), then the entry processor is applied directly on the object. In that case, no serialization or deserialization is performed. But if there is a defined event listener, a new entry value will be serialized when passing to the event publisher service.

![image](images/NoteSmall.jpg) ***NOTE***: *When `in-memory-format` is `OBJECT`, old value of the updated entry will be null.*

The methods below are in the IMap interface for entry processing.

```java
/**
 * Applies the user defined EntryProcessor to the entry mapped by the key.
 * Returns the the object which is result of the process() method of EntryProcessor.
 */
Object executeOnKey( K key, EntryProcessor entryProcessor );

/**
 * Applies the user defined EntryProcessor to the entries mapped by the collection of keys.
 * the results mapped by each key in the collection.
 */
Map<K, Object> executeOnKeys( Set<K> keys, EntryProcessor entryProcessor );

/**
 * Applies the user defined EntryProcessor to the entry mapped by the key with
 * specified ExecutionCallback to listen event status and returns immediately.
 */
void submitToKey( K key, EntryProcessor entryProcessor, ExecutionCallback callback );


/**
 * Applies the user defined EntryProcessor to the all entries in the map.
 * Returns the results mapped by each key in the map.
 */
Map<K, Object> executeOnEntries( EntryProcessor entryProcessor );
	   
/**
 * Applies the user defined EntryProcessor to the entries in the map which satisfies 
 provided predicate.
 * Returns the results mapped by each key in the map.
 */
Map<K, Object> executeOnEntries( EntryProcessor entryProcessor, Predicate predicate );
```

And, here is the EntryProcessor interface:

```java
public interface EntryProcessor<K, V> extends Serializable {
  Object process( Map.Entry<K, V> entry );

  EntryBackupProcessor<K, V> getBackupProcessor();
}
```

![image](images/NoteSmall.jpg) ***NOTE***: *If you want to execute a task on a single key, you can also use `executeOnKeyOwner` provided by Executor Service. But, in this case, you need to perform a lock and serialization.*

When using `executeOnEntries` method, if the number of entries is high and you do need the results, then returning null in `process()` method is a good practice. By this way, results of the processing is not stored in the map and hence out of memory errors are eliminated.


If your code modifies the data, then you should also provide a processor for backup entries. This is required to prevent the primary map entries from having different values than the backups; it causes the entry processor to be applied both on the primary and backup entries.

```java
public interface EntryBackupProcessor<K, V> extends Serializable {
    void processBackup( Map.Entry<K, V> entry );
}
```


![image](images/NoteSmall.jpg) ***NOTE***: *You should explicitly call `setValue` method of `Map.Entry` when modifying data in Entry Processor. Otherwise, Entry Processor will be accepted as read-only.*

![image](images/NoteSmall.jpg) ***NOTE***: *An EntryProcessor instance is not thread safe. If you are storing partition specific state between invocations be sure to register this in a thread-local.  A EntryProcessor instance can be used by multiple partition threads.*

![image](images/NoteSmall.jpg) ***NOTE***: *EntryProcessors run via Operation Threads that are dedicated to specific partitions.  Therefore with long running EntryProcessor executions other partition operations cannot be processed, such as a 'map.put(key)'.  With this is in mind it is good practice to make your EntryProcessor executions as quick as possible*

