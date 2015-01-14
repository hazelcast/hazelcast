


### Map Listener

You can listen to map-wide or entry-based events by implementing a `MapListener` sub-interface. 
A map-wide event is fired as a result of a map-wide operation e.g. `IMap#clear` or `IMap#evictAll`.
An entry-based event is fired after the operations that affects a specific entry e.g.`IMap#remove` or `IMap#evict`.


Let's take a look at the below sample code. As you will see, to catch an event you should explicitly implement a corresponding sub-interface of a `MapListener` e.g. `EntryAddedListener` or `MapClearedListener`.

![image](images/NoteSmall.jpg) ***NOTE:*** * `EntryListener` interface still can be implemented, we kept it as is due to the backward compatibility reasons but if you need to listen a different event which is not available in the `EntryListener` interface you should also implement a relevant `MapListener` sub-interface. 

```java
public class Listen {

  public static void main( String[] args ) {
    HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    IMap<String, String> map = hz.getMap( "somemap" );
    map.addEntryListener( new MyEntryListener(), true );
     System.out.println( "EntryListener registered" );
  }

  static class MyEntryListener implements EntryAddedListener<String, String>, EntryRemovedListener<String, String>, EntryUpdatedListener<String, String>, EntryEvictedListener<String, String> , MapEvictedListener, MapClearedListener   {
    @Override
    public void entryAdded( EntryEvent<String, String> event ) {
      System.out.println( "Entry Added:" + event );
    }

    @Override
    public void entryRemoved( EntryEvent<String, String> event ) {
      System.out.println( "Entry Removed:" + event );
    }

    @Override
    public void entryUpdated( EntryEvent<String, String> event ) {
      System.out.println( "Entry Updated:" + event );
    }

    @Override
    public void entryEvicted( EntryEvent<String, String> event ) {
      System.out.println( "Entry Evicted:" + event );
    }

    @Override
    public void mapEvicted( MapEvent event ) {
      System.out.println( "Map Evicted:" + event );
    }
   
    @Override
    public void mapCleared( MapEvent event ) {
      System.out.println( "Map Cleared:" + event );
    }

  }
}
```

And, now let's perform some modifications on the map entries using the following example code.

```java
public class Modify {

  public static void main( String[] args ) {
    HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    IMap<String, String> map = hz.getMap( "somemap");
    String key = "" + System.nanoTime();
    String value = "1";
    map.put( key, value );
    map.put( key, "2" );
    map.delete( key );
  }
}
```

If you execute the class `Listen` and then execute `Modify`, you get the following output produced by the `Listen` class. 

```
entryAdded:EntryEvent {Address[192.168.1.100]:5702} key=251359212222282,
    oldValue=null, value=1, event=ADDED, by Member [192.168.1.100]:5702

entryUpdated:EntryEvent {Address[192.168.1.100]:5702} key=251359212222282,
    oldValue=1, value=2, event=UPDATED, by Member [192.168.1.100]:5702

entryRemoved:EntryEvent {Address[192.168.1.100]:5702} key=251359212222282,
    oldValue=2, value=2, event=REMOVED, by Member [192.168.1.100]:5702
```


```java
public class MyEntryListener implements EntryListener{

    private Executor executor = Executors.newFixedThreadPool(5);

    @Override
    public void entryAdded(EntryEvent event) {
        executor.execute(new DoSomethingWithEvent(event));
    }
...
```
A map listener runs on event threads which are also used by other listeners (e.g. collection listeners, pub/sub message listeners, etc.). This means entry listeners can access other partitions. Consider this when you run long tasks, since listening to those tasks may cause other map/event listeners to starve.

