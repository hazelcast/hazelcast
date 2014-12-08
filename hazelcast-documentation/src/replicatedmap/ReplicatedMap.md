
## Replicated Map

A replicated map is a weakly consistent, distributed key-value data structure provided by Hazelcast.

All other data structures are partitioned in design. A replicated map does not partition data
(it does not spread data to different cluster members); instead, it replicates the data to all nodes.

This leads to higher memory consumption. However, a replicated map has faster read and write access since the data are available on all nodes and
writes take place on local nodes, eventually being replicated to all other nodes.

Weak consistency compared to eventually consistency means that replication is done on a best efforts basis. Lost or missing updates
are neither tracked nor resent. This kind of data structure is suitable for immutable
objects, catalogue data or idempotent calculable data (like HTML pages).

Replicated map nearly fully implements the `java.util.Map` interface, but it lacks the methods from `java.util.concurrent.ConcurrentMap` since
there are no atomic guarantees to writes or reads.

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.Collection;
import java.util.Map;

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Map<String, Customer> customers = hazelcastInstance.getReplicatedMap("customers");
customers.put( "1", new Customer( "Joe", "Smith" ) );
customers.put( "2", new Customer( "Ali", "Selam" ) );
customers.put( "3", new Customer( "Avi", "Noyan" ) );

Collection<Customer> colCustomers = customers.values();
for ( Customer customer : colCustomers ) {
  // process customer
}
```

`HazelcastInstance::getReplicatedMap`returns `com.hazelcast.core.ReplicatedMap` which, as stated above, extends the
`java.util.Map` interface.

The `com.hazelcast.core.ReplicatedMap` interface has some additional methods for registering entry listeners or retrieving values in an expected order.
