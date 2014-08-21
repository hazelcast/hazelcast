


## Entry Processor

### Overview

Hazelcast supports entry processing. Entry processor is a function that executes your code on a map entry in an atomic way. 

Entry processor enables fast in-memory operations on a map without having to worry about locks or concurrency issues. It can be applied to a single map entry or on all map entries and supports choosing target entries using predicates. You do not need any explicit lock on entry. Practically, Hazelcast locks the entry, runs the EntryProcessor, and then unlocks the entry.

Hazelcast sends the entry processor to each cluster member and these members apply it to map entries. So, if you add more members, your processing is completed faster.

If entry processing is the major operation for a map and the map consists of complex objects, then using `OBJECT` as `in-memory-format` is recommended to minimize serialization cost. By default, the entry value is stored as a byte array (`BINARY` format), but when it is stored as an object (OBJECT format), then entry processor is applied directly on the object. In that case, no serialization or deserialization is performed. But if there is a defined event listener, new entry value will be serialized when passing to event publisher service.

***NOTE***: When `in-memory-format` is `OBJECT` old value of the updated entry will be null.

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

***NOTE***: You should explicitly call ```setValue``` method of ```Map.Entry``` when modifying data in EP otherwise EP will be accepted as read-only.

```java
public interface EntryBackupProcessor<K, V> extends Serializable {
    void processBackup( Map.Entry<K, V> entry );
}
```

This is required to prevent the primary map entries from having different values than backups. Because entry processor is applied both on primary and backup entries.



### Sample Entry Processor Code

```java
public class EntryProcessorTest {

  @Test
  public void testMapEntryProcessor() throws InterruptedException {
    Config config = new Config().getMapConfig( "default" )
        .setInMemoryFormat( MapConfig.InMemoryFormat.OBJECT );
        
    HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance( config );
    HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance( config );
    IMap<Integer, Integer> map = hazelcastInstance1.getMap( "mapEntryProcessor" );
    map.put( 1, 1 );
    EntryProcessor entryProcessor = new IncrementingEntryProcessor();
    map.executeOnKey( 1, entryProcessor );
    assertEquals( map.get( 1 ), (Object) 2 );
    hazelcastInstance1.getLifecycleService().shutdown();
    hazelcastInstance2.getLifecycleService().shutdown();
  }

  @Test
  public void testMapEntryProcessorAllKeys() throws InterruptedException {
    StaticNodeFactory factory = new StaticNodeFactory( 2 );
    Config config = new Config().getMapConfig( "default" )
        .setInMemoryFormat( MapConfig.InMemoryFormat.OBJECT );
        
    HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance( config );
    HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance( config );
    IMap<Integer, Integer> map = hazelcastInstance1
        .getMap( "mapEntryProcessorAllKeys" );
        
    int size = 100;
    for ( int i = 0; i < size; i++ ) {
      map.put( i, i );
    }
    EntryProcessor entryProcessor = new IncrementingEntryProcessor();
    Map<Integer, Object> res = map.executeOnEntries( entryProcessor );
    for ( int i = 0; i < size; i++ ) {
      assertEquals( map.get( i ), (Object) (i + 1) );
    }
    for ( int i = 0; i < size; i++ ) {
      assertEquals( map.get( i ) + 1, res.get( i ) );
    }
    hazelcastInstance1.getLifecycleService().shutdown();
    hazelcastInstance2.getLifecycleService().shutdown();
  }

  static class IncrementingEntryProcessor
      implements EntryProcessor, EntryBackupProcessor, Serializable {
      
    public Object process( Map.Entry entry ) {
      Integer value = (Integer) entry.getValue();
      entry.setValue( value + 1 );
      return value + 1;
    }

    public EntryBackupProcessor getBackupProcessor() {
      return IncrementingEntryProcessor.this;
    }

    public void processBackup( Map.Entry entry ) {
      entry.setValue( (Integer) entry.getValue() + 1 );
    }
  }
}
```

### Abstract Entry Processor

`AbstractEntryProcessor` class can be used when the same processing will be performed both on primary and backup map entries (i.e. same logic applies to them). If `EntryProcessor` is used, you need to apply the same logic to backup entries separately. `AbstractEntryProcessor` class brings an easiness on this primary/backup processing.

Please see below sample code.

```java
public abstract class AbstractEntryProcessor <K, V>
    implements EntryProcessor <K, V> {
    
  private final EntryBackupProcessor <K,V> entryBackupProcessor;
  public AbstractEntryProcessor() {
    this(true);
  }

  public AbstractEntryProcessor(boolean applyOnBackup) {
    if ( applyOnBackup ) {
      entryBackupProcessor = new EntryBackupProcessorImpl();
    } else {
      entryBackupProcessor = null;
    }
  } 

  @Override
  public abstract Object process(Map.Entry<K, V> entry);

  @Override
  public final EntryBackupProcessor <K, V> getBackupProcessor() {
    return entryBackupProcessor;
  }

  private class EntryBackupProcessorImpl implements EntryBackupProcessor <K,V>{
    @Override
    public void processBackup(Map.Entry<K, V> entry) {
      process(entry); 
    }
  }	
}
```

In the above sample, the method `getBackupProcessor` returns an `EntryBackupProcessor` instance. This means, the same processing will be applied to both primary and backup entries. If you want to apply the processing only on the primary entries, then `getBackupProcessor` method should return null. 

