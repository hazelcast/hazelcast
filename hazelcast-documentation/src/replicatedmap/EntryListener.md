
### EntryListener on ReplicatedMap

A `com.hazelcast.core.EntryListener` used on a ReplicatedMap serves the same purpose as it would on other
data structures in Hazelcast. You can use it to react on add, update, and remove operations. Eviction is not yet
supported by replicated maps.

The fundamental difference in ReplicatedMap behavior, compared to the other data structures, is that an EntryListener only reflects
changes on local data. Since replication is asynchronous, all listener events are fired only when an operation is finished
on a local node. Events can fire at different times on different nodes.

```java
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
ReplicatedMap<String, Customer> customers = hazelcastInstance.getReplicatedMap("customers");

customers.addEntryListener( new EntryListener<String, Customer>() {
  @Override
  public void entryAdded( EntryEvent<String, Customer> event ) {
    log( "Entry added: " + event );
  }

  @Override
  public void entryUpdated( EntryEvent<String, Customer> event ) {
    log( "Entry updated: " + event );
  }

  @Override
  public void entryRemoved( EntryEvent<String, Customer> event ) {
    log( "Entry removed: " + event );
  }

  @Override
  public void entryEvicted( EntryEvent<String, Customer> event ) {
    // Currently not supported, will never fire
  }
});

customers.put( "1", new Customer( "Joe", "Smith" ) ); // add event
customers.put( "1", new Customer( "Ali", "Selam" ) ); // update event
customers.remove( "1" ); // remove event
```

