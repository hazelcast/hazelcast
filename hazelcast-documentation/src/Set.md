

## Set

Hazelcast Set is distributed and concurrent implementation of `java.util.Set`.

* Hazelcast Set does not allow duplicate elements.
* Hazelcast Set does not preserve the order of elements.
* Hazelcast Set is non-partitioned data structure which means all the data that belongs to a Set will live on one single partition in that node.
* Hazelcast Set cannot be scaled beyond the capacity of a single machine. Since the whole Set lives on a single partition, storing large amount of data on a single Set may result in causing memory pressures. Therefore, it is advisable to use multiple sets to store large amount of data; this way all the sets will be spread across the cluster, hence sharing the load.
* Backup of Hazelcast Set is stored on partition of another node in the cluster so that data is not lost in the event of primary node failure.
* There is no batching while iterating over Set. All items will be copied to local and iteration will occur locally.
* Equals method implementation of Hazelcast Set uses serialized byte version of objects compared to `java.util.HashSet`.

### Sample Set Code

```java
import com.hazelcast.core.Hazelcast;
import java.util.Set;
import java.util.Iterator;

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

Set<Price> set = hazelcastInstance.getSet( "IBM-Quote-History" );
set.add( new Price( 10, time1 ) );
set.add( new Price( 11, time2 ) );
set.add( new Price( 12, time3 ) );
set.add( new Price( 11, time4 ) );
//....
Iterator<Price> iterator = set.iterator();
while ( iterator.hasNext() ) { 
  Price price = iterator.next(); 
  //analyze
}
```

### Event Registration and Configuration for Set

Hazelcast Set uses ItemListener to listen to events which occur when items are added and removed.

```java
import java.util.Queue;
import java.util.Map; 
import java.util.Set; 
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryEvent; 

public class Sample implements ItemListener {

  public static void main( String[] args ) { 
    Sample sample = new Sample();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    ISet<Price> set = hazelcastInstance.getSet( "default" );
    set.addItemListener( sample, true ); 
        
    Price price = new Price( 10, time1 )
    set.add( price );
    set.remove( price );
  } 

  public void itemAdded( Object item ) {
    System.out.println( "Item added = " + item );
  }

  public void itemRemoved( Object item ) {
    System.out.println( "Item removed = " + item );
  }     
}
       
```

**Note:** *To learn more about the configuration of listeners please refer to [Listener Configurations](#listener-configurations).*

