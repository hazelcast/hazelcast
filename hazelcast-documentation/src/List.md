

## List

Hazelcast List is very similar to Hazelcast Set but it allows duplicate elements.

```java
import com.hazelcast.core.Hazelcast;
import java.util.List;
import java.util.Iterator;
import com.hazelcast.config.Config;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

java.util.List list = hz.getList("IBM-Quote-Frequency");
list.add(new Price(10));
list.add(new Price(11));
list.add(new Price(12));
list.add(new Price(11));
list.add(new Price(12));
        
//....
Iterator it = list.iterator();
while (it.hasNext()) { 
    Price price = (Price) it.next(); 
    //analyze
}
```
### Features

1. Hazelcast List allows duplicate elements.
2. Hazelcast List preserves the order of elements.
3. Hazelcast List is non-partitioned data structure where values and each backup is represented by its own single partition.
4. Hazelcast List can not scale beyond the capacity of a single machine.