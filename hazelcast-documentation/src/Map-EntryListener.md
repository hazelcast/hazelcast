


### Entry Listener

Map entry events can be listened. Hazelcast distributed map offers the method `addEntryListener` to add an entry listener to the map and listen to the entry events. 

Let's take a look at the below sample code.

```java
public class Listen {

  public static void main( String[] args ) {
    HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    IMap<String, String> map = hz.getMap( "somemap" );
    map.addEntryListener( new MyEntryListener(), true );
     System.out.println( "EntryListener registered" );
  }

  static class MyEntryListener implements EntryListener<String, String> {
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
  }
}
```

And, now let's perform some modifications on the map entries using the below sample code.

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

Now, let's first execute the class `Listen` and then execute `Modify`. Check out the below output produced by `Listen`. 

```
entryAdded:EntryEvent {Address[192.168.1.100]:5702} key=251359212222282,
    oldValue=null, value=1, event=ADDED, by Member [192.168.1.100]:5702

entryUpdated:EntryEvent {Address[192.168.1.100]:5702} key=251359212222282,
    oldValue=1, value=2, event=UPDATED, by Member [192.168.1.100]:5702

entryRemoved:EntryEvent {Address[192.168.1.100]:5702} key=251359212222282,
    oldValue=2, value=2, event=REMOVED, by Member [192.168.1.100]:5702
```

Entry Listener runs on event threads which are also used by other listeners (e.g. collection listeners, pub/sub message listeners, etc.). This means entry listeners can access to other partitions. So, consideration should be given when running long tasks since listening to those tasks may cause other event listeners to starve.

