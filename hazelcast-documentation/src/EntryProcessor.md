


## Entry Processor

### Entry Processor Overview

Hazelcast supports entry processing. Entry processor is a function that executes your code on a map entry in an atomic way. 

Entry processor enables fast in-memory operations on a map without having to worry about locks or concurrency issues. It can be applied to a single map entry or on all map entries and supports choosing target entries using predicates. You do not need any explicit lock on entry. Practically, Hazelcast locks the entry, runs the EntryProcessor, and then unlocks the entry.

Hazelcast sends the entry processor to each cluster member and these members apply it to map entries. So, if you add more members, your processing is completed faster.

If entry processing is the major operation for a map and the map consists of complex objects, then using `OBJECT` as `in-memory-format` is recommended to minimize serialization cost. By default, the entry value is stored as a byte array (`BINARY` format), but when it is stored as an object (OBJECT format), then entry processor is applied directly on the object. In that case, no serialization or deserialization is performed. But if there is a defined event listener, new entry value will be serialized when passing to event publisher service.

***NOTE***: When `in-memory-format` is `OBJECT`, old value of the updated entry will be null.

There are below methods in IMap interface for entry processing:

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
 * Applies the user defined EntryProcessor to the entries in the map which satisfies provided predicate.
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

***ATTENTION***: *If you want to execute a task on a single key, you can also use `executeOnKeyOwner` provided by Executor Service. But, in this case, you need to perform a lock and serialization.*

When using `executeOnEntries` method, if the number of entries is high and you do need the results, then returning null in `process()` method is a good practice. By this way, results of the processing is not stored in the map and hence out of memory errors are eliminated.


If your code is modifying the data, then you should also provide a processor for backup entries:

***NOTE***: You should explicitly call `setValue` method of `Map.Entry` when modifying data in Entry Processor. Otherwise, Entry Processpr will be accepted as read-only.

```java
public interface EntryBackupProcessor<K, V> extends Serializable {
    void processBackup( Map.Entry<K, V> entry );
}
```

This is required to prevent the primary map entries from having different values than backups. Because entry processor is applied both on primary and backup entries.



