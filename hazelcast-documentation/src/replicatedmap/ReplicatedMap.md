
## Replicated Map (BETA)

A ReplicatedMap is a fully weakly consistent, distributed key-value storage data structure provided by Hazelcast.

In difference to all other data structures which are partitioned in design, a ReplicatedMap does not partition data
(it does not spread data to different cluster members) but replicates the data to all nodes.

This leads to higher memory consumption but faster read and write access since data are available on all notes and
writes take place on local nodes and eventually being replicated to all other nodes.

Weakly consistency compared to eventually consistency means that replication is done on best effort. Lost or missing updates
are neither tracked nor resend to keep the nodes in a consistent state. This kind of data structures is perfect for immutable
objects, catalogue data or idempotent calculatable data (like HTML pages).

It nearly fully implements the `java.util.Map` interface but lacks the methods from `java.util.concurrent.ConcurrentMap` since
there are no atomic guarantees to writes or reads.

```java
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.Collection;
import java.util.Map;

Config config = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
Map<String, Customer> mapCustomers = hz.getReplicatedMap("customers");
mapCustomers.put("1", new Customer("Joe", "Smith"));
mapCustomers.put("2", new Customer("Ali", "Selam"));
mapCustomers.put("3", new Customer("Avi", "Noyan"));

Collection<Customer> colCustomers = mapCustomers.values();
for (Customer customer : colCustomers) {
    // process customer
}
```

`HazelcastInstance::getReplicatedMap` actually returns `com.hazelcast.core.ReplicatedMap` which, as stated above, extends
`java.util.Map` interface.

The `com.hazelcast.core.ReplicatedMap` interface has some additional methods for registering EntryListeners or retrieving
values in an expected order.
