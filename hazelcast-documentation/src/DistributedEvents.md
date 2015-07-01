
# Distributed Events

You can register for Hazelcast entry events so you will be notified when those events occur. Event Listeners are cluster-wide: when a listener is registered in one member of cluster, it is actually registered for events that originated at any member in the cluster. When a new member joins, events originated at the new member will also be delivered.

An Event is created only if you registered an event listener. If no listener is registered, then no event will be created. If you provided a predicate when you registered the event listener, pass the predicate before sending the event to the listener (node/client).

As a rule of thumb, your event listener should not implement heavy processes in its event methods which block the thread for a long time. If needed, you can use `ExecutorService` to transfer long running processes to another thread and thus offload the current listener thread.



## Event Listeners for Hazelcast Nodes

Hazelcast offers the following event listeners:

- **Membership Listener** for cluster membership events.
- **Distributed Object Listener** for distributed object creation and destroy events.
- **Migration Listener** for partition migration start and complete events.
- **Partition Lost Listener** for partition lost events.
- **Lifecycle Listener** for `HazelcastInstance` lifecycle events.
- **Entry Listener** for `IMap` and `MultiMap` entry events (please refer to the [Listening to Map Events section](#listening-to-map-events)).
- **Item Listener** for `IQueue`, `ISet` and `IList` item events (please refer to the Event Registration and Configuration parts of the sections [Set](#set) and [List](#list)).
- **Message Listener** for `ITopic` message events.
- **Client Listener** for client connection events.



### Membership Listener

The Membership Listener allows to get notified for the following events.

- A new member is added to the cluster.
- An existing member leaves the cluster.
- An attribute of a member is changed. Please refer to the [Member Attributes section](#member-attributes) to learn about member attributes.

The following is an example Membership Listener class.

```java
public class ClusterMembershipListener
     implements MembershipListener {
     
public void memberAdded(MembershipEvent membershipEvent) {
  System.err.println("Added: " + membershipEvent);
}

public void memberRemoved(MembershipEvent membershipEvent) {
       System.err.println("Removed: " + membershipEvent);
     }

public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
       System.err.println("Member attribute changed: " + memberAttributeEvent);
     }
     
}
```

When a respective event is fired, the membership listener outputs the addresses of the members that joined and left, and also which attribute changed on which member.

### Distributed Object Listener

The Distributed Object Listener notifies when a distributed object is created or destroyed throughout the cluster.

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

When a respective event is fired, the distributed object listener outputs the event type, and the name, service (for example, if a Map service provides the distributed object, than it is a Map object), and ID of the object.


### Migration Listener

The Migration Listener notifies for the following events:

- A partition migration is started.
- A partition migration is completed.
- A partition migration is failed.


The following is an example Migration Listener class.


```java
public class ClusterMigrationListener implements MigrationListener {
     @Override
     public void migrationStarted(MigrationEvent migrationEvent) {
       System.err.println("Started: " + migrationEvent);
     }
    @Override
     public void migrationCompleted(MigrationEvent migrationEvent) {
       System.err.println("Completed: " + migrationEvent);
     }
     @Override
     public void migrationFailed(MigrationEvent migrationEvent) {
       System.err.println("Failed: " + migrationEvent);
     }
}     
```

When a respective event is fired, the migration listener outputs the partition ID, status of the migration, the old member and the new member. The following is an example output.

```
Started: MigrationEvent{partitionId=98, oldOwner=Member [127.0.0.1]:5701,
newOwner=Member [127.0.0.1]:5702 this} 
```

### Partition Lost Listener

Hazelcast provides fault-tolerance by keeping multiple copies of your data. For each partition, one of your nodes become owner and some of the other nodes become replica nodes based on your configuration. Nevertheless, data loss may occur if a few nodes crash simultaneously.

Let`s consider the following example with three nodes: N1, N2, N3 for a given partition-0. N1 is owner of partition-0, N2 and N3 are the first and second replicas respectively. If N1 and N2 crash simultaneously, partition-0 loses its data that is configured with less than 2 backups.
For instance, if we configure a map with 1 backup, that map loses its data in partition-0 since both owner and first replica of partition-0 have crashed. However, if we configure our map with 2 backups, it does not lose any data since a copy of partition-0's data for the given map
also resides in N3. 

The Partition Lost Listener notifies for possible data loss occurrences with the information of how many replicas are lost for a partition. It listens to `PartitionLostEvent` instances. Partition lost events are dispatched per partition. 

Partition loss detection is done after a node crash is detected by the other nodes and the crashed node is removed from the cluster. Please note that false-positive `PartitionLostEvent` instances may be fired on partial network split errors. 

The following is an example of Partition Lost Listener. 

```java
    public class ConsoleLoggingPartitionLostListener implements PartitionLostListener {
        @Override
        public void partitionLost(PartitionLostEvent event) {
            System.out.println(event);
        }
    } 
```

When a `PartitionLostEvent` is fired, the partition lost listener given above outputs the partition ID, the replica index that is lost and the node that has detected the partition loss. The following is an example output.

```
com.hazelcast.partition.PartitionLostEvent{partitionId=242, lostBackupCount=0, 
eventSource=Address[192.168.2.49]:5701}
```

### Lifecycle Listener

The Lifecycle Listener notifies for the following events:

- A member is starting.
- A member started.
- A member is shutting down.
- A member's shutdown has completed.
- A member is merging with the cluster.
- A member's merge operation has completed.
- A Hazelcast Client connected to the cluster.
- A Hazelcast Client disconnected from the cluster.


The following is an example Lifecycle Listener class.


```java
public class NodeLifecycleListener implements LifecycleListener {
     @Override
     public void stateChanged(LifecycleEvent event) {
       System.err.println(event);
     }
}
```

This listener is local to an individual node. It notifies the application that uses Hazelcast about the events mentioned above for a particular node. 


### Item Listener

The Item Listener is used by the Hazelcast `IQueue`, `ISet` and `IList` interfaces. It notifies when an item is added or removed.

The following is an example Item Listener class.


```java
public class Sample implements ItemListener {

  public static void main( String[] args ) { 
    Sample sample = new Sample();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    ISet<Price> set = hazelcastInstance.getSet( "default" );
    set.addItemListener( sample, true ); 

    Price price = new Price( 10, time1 )
    set.add( price );
    set.remove( price );
  } 

  public void itemAdded( Object item ) {
    System.out.println( "Item added = " + item );
  }

  public void itemRemoved( Object item ) {
    System.out.println( "Item removed = " + item );
  }     
}
```

### Message Listener

The Message Listener is used by the `ITopic` interface. It notifies when a message is received for the registered topic.

The following is an example Message Listener class.


```java
public class Sample implements MessageListener<MyEvent> {

  public static void main( String[] args ) {
    Sample sample = new Sample();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    ITopic topic = hazelcastInstance.getTopic( "default" );
    topic.addMessageListener( sample );
    topic.publish( new MyEvent() );
  }

  public void onMessage( Message<MyEvent> message ) {
    MyEvent myEvent = message.getMessageObject();
    System.out.println( "Message received = " + myEvent.toString() );
    if ( myEvent.isHeavyweight() ) {
      messageExecutor.execute( new Runnable() {
          public void run() {
            doHeavyweightStuff( myEvent );
          }
      } );
    }
  }
```

### Client Listener

The Client Listener is used by the Hazelcast nodes. It notifies the nodes when a client is connected to or disconnected from the cluster.


![image](images/NoteSmall.jpg) ***NOTE:*** *You can also add event listeners to a Hazelcast client. Please refer to [Client Listenerconfig](#client-listener-configuration) for the related information.*

## Event Listeners for Hazelcast Clients

You can add event listeners to a Hazelcast Java client. You can configure the following listeners to listen to the events on the client side. Please see the respective sections under the [Event Listeners for Hazelcast Nodes section](#event-listeners-for-hazelcast-nodes) for example code.


- **Lifecycle Listener**: Notifies when the client is starting, started, shutting down and shutdown.
- **Membership Listener**: Notifies when a node joins to/leaves the cluster to which the client is connected, or when an attribute is changed in a node.
- **DistributedObject Listener**: Notifies when a distributed object is created or destroyed throughout the cluster to which the client is connected.

<br></br>
***RELATED INFORMATION***

*Please refer to the [Client Listenerconfig section](#client-listener-configuration) for more information.*
<br></br>

<br></br>
***RELATED INFORMATION***

*Please refer to the [Listener Configurations section](#listener-configurations) for a configuration wrap-up of event listeners.*
<br></br>


