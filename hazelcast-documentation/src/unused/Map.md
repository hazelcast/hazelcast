
## Distributed Map


Hazelcast will partition your map entries and almost evenly distribute onto all Hazelcast members. Distributed maps have 1 backup by default so that if a member goes down, you do not lose data. Backup operations are synchronous, so when a `map.put(key, value)` returns, it is guaranteed that the entry is replicated to one other node. For the reads, it is also guaranteed that `map.get(key)` returns the latest value of the entry. Consistency is strictly enforced.

```java
import com.hazelcast.core.Hazelcast;
import java.util.Map;
import java.util.Collection;
import com.hazelcast.config.Config;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
Map<String, Customer> mapCustomers = hz.getMap("customers");
mapCustomers.put("1", new Customer("Joe", "Smith"));
mapCustomers.put("2", new Customer("Ali", "Selam"));
mapCustomers.put("3", new Customer("Avi", "Noyan"));

Collection<Customer> colCustomers = mapCustomers.values();
for (Customer customer : colCustomers) {
    // process customer
}
```

`HazelcastInstance.getMap()` actually returns `com.hazelcast.core.IMap` which extends `java.util.concurrent.ConcurrentMap` interface. So methods like `ConcurrentMap.putIfAbsent(key,value)` and `ConcurrentMap.replace(key,value)` can be used on distributed map as shown in the example below.

```java
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


