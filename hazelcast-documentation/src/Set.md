

## Set

Hazelcast Set is distributed and concurrent implementation of`java.util.Set`. 

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
### Features

1. Hazelcast Set does not allow duplicate elements.
2. Hazelcast Set does not preserve the order of elements.
3. Hazelcast Set is non-partitioned data structure where values and  each backup is represented by its own single partition.
4. Hazelcast Set can not scale beyond the capacity of a single machine.
5. Equals method implementation of Hazelcast Set use serialized byte version of objects compared to `java.util.HashSet`
