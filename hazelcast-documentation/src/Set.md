

## Set

Hazelcast Set is distributed and concurrent implementation of `java.util.Set`.

* Hazelcast Set does not allow duplicate elements.
* Hazelcast Set does not preserve the order of elements.
* Hazelcast Set is non-partitioned data structure where values and each backup is represented by its own single partition.
* Hazelcast Set cannot be scaled beyond the capacity of a single machine.
* There is no batching while iterating over Set, items will be copied to local and iteration will occur locally.
* Equals method implementation of Hazelcast Set uses serialized byte version of objects compared to `java.util.HashSet`.

### Sample Set Code

```java
import com.hazelcast.core.Hazelcast;
import java.util.Set;
import java.util.Iterator;
import com.hazelcast.config.Config;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

java.util.Set set = hz.getSet("IBM-Quote-History");
set.add(new Price(10, time1));
set.add(new Price(11, time2));
set.add(new Price(12, time3));
set.add(new Price(11, time4));
//....
Iterator it = set.iterator();
while (it.hasNext()) { 
    Price price = (Price) it.next(); 
    //analyze
}
```

### Event Registration and Configuration

Hazelcast Set uses ItemListener to listen to events which occur when items are added and removed.

```java
import java.util.Queue;
import java.util.Map; 
import java.util.Set; 
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryEvent; 
import com.hazelcast.config.Config;

public class Sample implements ItemListener{

    public static void main(String[] args) { 
        Sample sample = new Sample();
        Config cfg = new Config();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        ISet   set   = hz.getSet   ("default");
        set.addItemListener  (sample, true); 
        
        Price price = new Price(10, time1)
        set.add(price);
        set.remove(price);
    } 

    public void itemAdded(Object item) {
        System.out.println("Item added = " + item);
    }

    public void itemRemoved(Object item) {
        System.out.println("Item removed = " + item);
    }     
}
       
```

<br> </br>

<font color="red">
***Related Information***
</font>

*Please refer to [Listener Configurations](#listener-configurations).*

<br> </br>
