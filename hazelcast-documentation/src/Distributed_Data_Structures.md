

# Distributed Data Structures

As mentioned in the [Overview](#hazelcast-overview) section, Hazelcast offers distributed implementations of Java interfaces. Below is the full list with links to their sections in this manual.

- **Standard utility collections:**

	- [Map](#map): Distributed implementation of `java.util.Map`. Operations like reading and writing from/to a Hazelcast map can be performed with the well known methods like get and put.
	- [Queue](#queue): Distributed queue is an implementation of `java.util.concurrent.BlockingQueue`. You can add an item in one machine and remove it from another one.
	- [Set](#set): Distributed and concurrent implementation of `java.util.Set`. It does not allow duplicate elements and does not preserve their order.
	- [List](#list): Very similar to Hazelcast List, except that it allows duplicate elements and preserver their order.
	- [MultiMap](#multimap): It is a specialized Hazelcast map, meaning it is distributed, where multiple values under a single key can be stored.
	- [ReplicatedMap](#replicated-map): It does not partition data, i.e. it does not spread data to different cluster members, but replicates the data to all nodes.
- [**Topic**](#topic): Distributed mechanism for publishing messages that are delivered to multiple subscribers which is also known as publish/subscribe (pub/sub) messaging model. 
- **Concurrency utilities**:
	- [Lock](#lock): Distributed implementation of `java.util.concurrent.locks.Lock`. When you lock using Hazelcast Lock, the critical section that it guards is guaranteed to be executed by only one thread in entire cluster.
	- [Semaphore](#isemaphore): Distributed implementation of `java.util.concurrent.Semaphore`. When performing concurrent activities semaphores offer permits to control the thread counts.
	- [AtomicLong](#iatomiclong): Distributed implementation of `java.util.concurrent.atomic.AtomicLong`. Most of AtomicLong's operations are offered but these operations involve remote calls and hence their performances differ from AtomicLong, due to being distributed.
	- [AtomicReference](#iatomicreference): When you need to deal with a reference in a distributed environment, you can use Hazelcast AtomicReference that is the distributed version of `java.util.concurrent.atomic.AtomicReference`.
	- [IdGenerator](#idgenerator): Hazelcast IdGenerator is used to generate cluster-wide unique identifiers and ID generation occurs almost at the speed of `AtomicLong.incrementAndGet()`.
	- [CountdownLatch](#icountdownlatch): Distributed implementation of `java.util.concurrent.CountDownLatch`. It is a gate keeper for concurrent activities, enabling the threads to wait for other threads to complete their operations.

Common Features of all Hazelcast Data Structures:


-   If a member goes down, its backup replica that also holds the same data, will dynamically redistribute the data including the ownership and locks on them to remaining live nodes. As a result, no data will get lost.
-   There is no single cluster master or something that can cause single point of failure. Every node in the cluster has equal rights and responsibilities. No-one is superior. And no dependency on external 'server' or 'master' kind of concept.

Here is how you can retrieve existing data structure instances (map, queue, set, lock, topic, etc.) and how you can listen for instance events to get notified when an instance is created or destroyed.

```java
import java.util.Collection;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;

public class Sample implements DistributedObjectListener {
  public static void main(String[] args) {
    Sample sample = new Sample();

    Config config = new Config();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    hazelcastInstance.addDistributedObjectListener(sample);

    Collection<DistributedObject> distributedObjects = hazelcastInstance.getDistributedObjects();
    for (DistributedObject distributedObject : distributedObjects) {
      System.out.println(distributedObject.getName() + "," + distributedObject.getId());
    }
  }

  @Override
  public void distributedObjectCreated(DistributedObjectEvent event) {
    DistributedObject instance = event.getDistributedObject();
    System.out.println("Created " + instance.getName() + "," + instance.getId());
  }

  @Override
  public void distributedObjectDestroyed(DistributedObjectEvent event) {
    DistributedObject instance = event.getDistributedObject();
    System.out.println("Destroyed " + instance.getName() + "," + instance.getId());
  }
}
```

