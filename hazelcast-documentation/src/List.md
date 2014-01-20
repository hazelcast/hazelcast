

## Distributed List

Distributed List is very similar to distributed set, but it allows duplicate elements.

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