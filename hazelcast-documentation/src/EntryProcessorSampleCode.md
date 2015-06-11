

### Sample Entry Processor Code

The EntryProcessorTest class has the following methods.

* `testMapEntryProcessor` puts one map entry and calls `executeOnKey` to process that map entry.
* `testMapEntryProcessor` puts all the entries in a map and calls `executeOnEntries` to process 
   all the entries.

The static class `IncrementingEntryProcessor` creates an entry processor to process the map 
entries in the EntryProcessorTest class.


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

