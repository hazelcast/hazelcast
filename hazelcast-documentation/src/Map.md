

## Map

### Map Overview

Hazelcast Map (IMap) extends the interface `java.util.concurrent.ConcurrentMap` and hence `java.util.map`. In simple terms, it is the distributed implementation of Java map. And operations like reading and writing from/to a Hazelcast map can be performed with the well known methods like get and put.

#### How It Works

Hazelcast will partition your map entries and almost evenly distribute onto all Hazelcast members. Each member carries approximately "(1/n `*` total-data) + backups", **n** being the number of nodes in the cluster.

Just for exemplary purposes, let's create a Hazelcast instance (node) and fill a map named `Capitals` with key-value pairs using the below code.

```
public class FillMapMember {
   public static void main(String[] args) { 
      HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance();
      Map<String, String> capitalcities = hzInstance.getMap("Capitals"); 
      capitalcities.put("1", "Tokyo");
      capitalcities.put("2", "Paris”);
      capitalcities.put("3", "New York");
      capitalcities.put("4", "Ankara");
      capitalcities.put("5", "Brussels");
      capitalcities.put("6", "Amsterdam");
      capitalcities.put("7", "New Delhi");
      capitalcities.put("8", "London");
      capitalcities.put("9", "Berlin");
      capitalcities.put("10", "Oslo");
      capitalcities.put("11", "Moscow");
      ...
      ...
      capitalcities.put("120", "Stockholm")
   }
}
```

When you run this code, a node is created with a map whose entries are distributed across the node's partitions. See the below illustration. This is a single node cluster for now.

![](images/1Node.jpg)

***Note:*** *Please note that some of the partitions will not contain any data entry since we have 120 objects and the partition count is 271 by default. This count is configurable and can be changed using the system property `hazelcast.partition.count`. Please see [Advanced Configuration Properties](#advanced-configuration-properties).*

Now, let's create a second node which will result in a cluster with 2 nodes. This is where backups of entries are created, too. Please remember the backup partitions mentioned in [Hazelcast Overview](#hazelcast-overview) section. So, run the above code again to create the second node. Below illustration shows two nodes and how the data and its backup is distributed.

![](images/2Nodes.jpg)

As you see, when a new member joins the cluster, it takes ownership (responsibility) and load of -some- of the entire data in the cluster. Eventually, it will carry almost "(1/n `*` total-data) + backups" and reduces the load on others.




Hazelcast instance's `getMap()` actually returns `com.hazelcast.core.IMap` which extends `java.util.concurrent.ConcurrentMap` interface. So methods like `ConcurrentMap.putIfAbsent(key,value)` and `ConcurrentMap.replace(key,value)` can be used on distributed map as shown in the example below.

```
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.concurrent.ConcurrentMap;

Config cfg = new Config();
HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);

Customer getCustomer (String id) {
    ConcurrentMap<String, Customer> map = instance.getMap("customers");
    Customer customer = map.get(id);
    if (customer == null) {
        customer = new Customer (id);
        customer = map.putIfAbsent(id, customer);
    }
    return customer;
}               

public boolean updateCustomer (Customer customer) {
    ConcurrentMap<String, Customer> map = instance.getMap("customers");
    return (map.replace(customer.getId(), customer) != null);            
}
                
public boolean removeCustomer (Customer customer) {
    ConcurrentMap<String, Customer> map = instance.getMap("customers");
    return map.remove(customer.getId(), customer) );           
}
```

All `ConcurrentMap` operations such as `put` and `remove` might wait if the key is locked by another thread in the local or remote JVM. But, they will eventually return with success. `ConcurrentMap` operations never throw `java.util.ConcurrentModificationException`.

Also see:

-   [Data Affinity](#data-affinity).

-   [Map Configuration with wildcards](#wildcard-configuration).

