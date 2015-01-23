
# Distributed Events

You can register for Hazelcast entry events so you will be notified when those events occur. Event Listeners are cluster-wide so when a listener is registered in one member of cluster, it is actually registering for events originated at any member in the cluster. When a new member joins, events originated at the new member will also be delivered.

An Event is created only if you registered an event listener. If no listener is registered, then no event will be created. If you provided a predicate when you registered the event listener, pass the predicate before sending the event to the listener (node/client).

As a rule of thumb, your event listener should not implement heavy processes in its event methods which block the thread for a long time. If needed, you can use `ExecutorService` to transfer long running processes to another thread and offload the current listener thread.



## Event Listeners

Hazelcast offers the following event listeners:

- **Membership Listener** for cluster membership events.
- **Distributed Object Listener** for distributed object creation and destroy events.
- **Migration Listener** for partition migration start and complete events.
- **Lifecycle Listener** for HazelcastInstance lifecycle events.
- **Entry Listener** for IMap and MultiMap entry events.
- **Item Listener** for IQueue, ISet and IList item events (please refer to the Event Registration and Configuration parts of the sections [Set](#set) and [List](#list)).
- **Message Listener** for ITopic message events.
- **Client Listener** for client connection events.



### Membership Listener

The Membership Listener allows to get notified for the following events:

- A new member is added to the cluster.
- An existing member leaves the cluster.
- An attribute of a member is changed. Please refer to the [Member Attributes section](#member-attributes) to learn about member attributes.

The following is an example Membership Listener class.

```java
public class ClusterMembershipListener     implements MembershipListener {     
public void memberAdded(MembershipEvent membershipEvent) {  System.err.println("Added: " + membershipEvent);}public void memberRemoved(MembershipEvent membershipEvent) {       System.err.println("Removed: " + membershipEvent);     }

public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {       System.err.println("Member attribute changed: " + memberAttributeEvent);     }
     }```

When a respective event is fired, the membership listener outputs the addresses of the members joined/left and which attribute is changed on which member.

### Distributed Object Listener

The Distributed Object Listener allows to get notified when a distributed object is created or destroyed throughout the cluster.

The following is an example Distributed Object Listener class.


```java
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

When a respective event is fired, the distributed object listener outputs the name, service (e.g. if a Map service provides the distributed object, than it is a Map object) and ID of the object, along with the event type.


### Migration Listener

The Migration Listener allows to get notified for the following events:

- A partition migration is started.
- A partition migration is completed.
- A partition migration is failed.


The following is an example Migration Listener class.


```java
...
```

When a respective event is fired, the migration listener outputs the partition ID, status of the migration, the old member and the new member. The following is an example output:

```
Started: MigrationEvent{partitionId=98, oldOwner=Member [127.0.0.1]:5701,newOwner=Member [127.0.0.1]:5702 this} 
```



### Lifecycle Listener

The Lifecycle Listener allows to get notified for the following events:

- A member is starting.
- A member is started.
- A member is shutting down.
- A member's shutdown is completed.
- A member is merging with the cluster.
- A member's merge operation is completed.
- A Hazelcast Client is connected to the cluster.
- A Hazelcast Client is disconnected from the cluster.


The following is an example Migration Listener class.


```java
...
```
### Item Listener

The Item Listener is used by the Hazelcast IQueue, ISet and IList interfaces. It allows to get notified when an item is added or removed.

The following is an example Item Listener class.


```java
...
```

### Message Listener

The Message Listener is used by the Hazelcast IQueue, ISet and IList interfaces. It allows to get notified when an item is added or removed.

The following is an example Item Listener class.


```java
...
```
