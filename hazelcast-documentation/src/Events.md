
## Distributed Events

Hazelcast allows you to register for entry events to get notified when entries added, updated or removed. Listeners are cluster wide. When a member adds a listener, it is actually registering for events originated at any member in the cluster. When a new member joins, events originated at the new member will also be delivered. All events are ordered, i.e. listeners will receive and process the events in the order they are actually occurred.

```java
import java.util.Queue;
import java.util.Map; 
import java.util.Set; 
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryEvent; 
import com.hazelcast.config.Config;

public class Sample implements ItemListener, EntryListener {

    public static void main(String[] args) { 
        Sample sample = new Sample();
        Config cfg = new Config();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        IQueue queue = hz.getQueue ("default");
        IMap   map   = hz.getMap   ("default");
        ISet   set   = hz.getSet   ("default");
        //listen for all added/updated/removed entries
        queue.addItemListener(sample, true);
        set.addItemListener  (sample, true); 
        map.addEntryListener (sample, true);        
        //listen for an entry with specific key 
        map.addEntryListener (sample, "keyobj");        
    } 

    public void entryAdded(EntryEvent event) {
        System.out.println("Entry added key=" + event.getKey() + ", value=" + event.getValue());
    }

    public void entryRemoved(EntryEvent event) {
        System.out.println("Entry removed key=" + event.getKey() + ", value=" + event.getValue());
    }

    public void entryUpdated(EntryEvent event) {
        System.out.println("Entry update key=" + event.getKey() + ", value=" + event.getValue());
    } 

    public void entryEvicted(EntryEvent event) {
        System.out.println("Entry evicted key=" + event.getKey() + ", value=" + event.getValue());
    } 
    
    public void itemAdded(Object item) {
        System.out.println("Item added = " + item);
    }

    public void itemRemoved(Object item) {
        System.out.println("Item removed = " + item);
    }     
}
       
```