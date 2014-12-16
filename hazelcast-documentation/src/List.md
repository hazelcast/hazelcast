

## List

Hazelcast List is similar to Hazelcast Set, but Hazelcast List also allows duplicate elements.

* Besides allowing duplicate elements, Hazelcast List preserves the order of elements.
* Hazelcast List is a non-partitioned data structure where values and each backup are represented by their own single partition.
* Hazelcast List cannot be scaled beyond the capacity of a single machine.
* All items are copied to local and iteration occurs locally.

### Sample List Code

```java
import com.hazelcast.core.Hazelcast;
import java.util.List;
import java.util.Iterator;

HazelcastInstance hz = Hazelcast.newHazelcastInstance();

List<Price> list = hz.getList( "IBM-Quote-Frequency" );
list.add( new Price( 10 ) );
list.add( new Price( 11 ) );
list.add( new Price( 12 ) );
list.add( new Price( 11 ) );
list.add( new Price( 12 ) );
        
//....
Iterator<Price> iterator = list.iterator();
while ( iterator.hasNext() ) { 
  Price price = iterator.next(); 
  //analyze
}
```

### Event Registration and Configuration for List

Hazelcast List uses `ItemListener` to listen to events which occur when items are added and removed.


```java
import java.util.Queue;
import java.util.Map; 
import java.util.Set; 
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryEvent; 

public class Sample implements ItemListener{

  public static void main( String[] args ) { 
    Sample sample = new Sample();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    IList<Price> list = hazelcastInstance.getList( "default" );
    list.addItemListener( sample, true ); 
        
    Price price = new Price( 10, time1 )
    list.add( price );
    list.remove( price );
  } 

  public void itemAdded( Object item ) {
    System.out.println( "Item added = " + item );
  }

  public void itemRemoved( Object item ) {
    System.out.println( "Item removed = " + item );
  }     
}
       
```

<br></br>

***RELATED INFORMATION***

*To learn more about the configuration of listeners please refer to the [Listener Configurations section](#listener-configurations).*

<br></br>

***RELATED INFORMATION***


*Please refer to the [List Configuration section](#list-configuration) for a full description of Hazelcast Distributed List configuration.*


