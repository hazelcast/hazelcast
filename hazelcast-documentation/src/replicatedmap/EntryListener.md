
### EntryListener on ReplicatedMap

In general a `com.hazelcast.core.EntryListener` used on a ReplicatedMap serves the same purpose as it would on other
data structures in Hazelcast. You can use it to react on add, update, remove operations whereas eviction is not yet
supported by replicated maps.

The fundamental difference in behavior, compared to the other data structures, is that an EntryListener only reflects
changes on local data. Since replication is asynchronous, all listener events are fired only when an operation is finished
on a local node. With that in mind, events can fire at different times on different nodes.

```java
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;

Config config = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
ReplicatedMap<String, Customer> mapCustomers = hz.getReplicatedMap("customers");

mapCustomers.addEntryListener(new EntryListener<String, Customer>() {
    @Override
    public void entryAdded(EntryEvent<String, Customer> event) {
        log("Entry added: " + event);
    }

    @Override
    public void entryUpdated(EntryEvent<String, Customer> event) {
        log("Entry updated: " + event);
    }

    @Override
    public void entryRemoved(EntryEvent<String, Customer> event) {
        log("Entry removed: " + event);
    }

    @Override
    public void entryEvicted(EntryEvent<String, Customer> event) {
        // Currently not supported, will never fire
    }
});

mapCustomers.put("1", new Customer("Joe", "Smith")); // add event
mapCustomers.put("1", new Customer("Ali", "Selam")); // update event
mapCustomers.remove("1"); // remove event
```

<br></br>