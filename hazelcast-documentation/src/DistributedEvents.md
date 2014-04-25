
# Distributed Events

Hazelcast allows you to register for entry events to get notified when events occurred. Event Listeners are cluster-wide so when a listener is registered in one member of cluster , it is actually registering for events originated at any member in the cluster. When a new member joins, events originated at the new member will also be delivered. 

### Event Listeners

-   **MembershipListener** for cluster membership events

-   **DistributedObjectListener** for distributed object creation and destroy events

-   **MigrationListener** for partition migration start and complete events

-   **LifecycleListener** for HazelcastInstance lifecycle events

-   **EntryListener** for IMap and MultiMap entry events

-   **ItemListener** for IQueue, ISet and IList item events [todo: add link to related sample Listener examples]

-   **MessageListener** for ITopic message events

-   **ClientListener** for client connection events 


how to configure each listener see [todo: add link to Listener configuration.]


### Global Event Configuration

    - "hazelcast.event.queue.capacity" : default value is 1,000,000
    - "hazelcast.event.queue.timeout.millis" : default value is 250
    - "hazelcast.event.thread.count" : default value is 5


There is a striped executor in each node to control and dispatch received events to user. This striped executor
also guarantees the order. For all events in Hazelcast, the order that events are generated and the order they are
published the user are guaranteed for given keys. For map and multiMap order is preserved for the operations on
same key of the entry. For list,set,topic and queue order is preserved for events on that instance of the distributed
data structure.

Order guarantee is achieved by making only one thread responsible for a particular set of events(entry events of a key
in a map, item events of a collection,  etc) in StripedExecutor.

If event queue reaches the capacity("hazelcast.event.queue.capacity") and last item  cannot be put the event queue
for timeout millis("hazelcast.event.queue.timeout.millis"), these events will be dropped with a warning message like
"EventQueue overloaded! ..... ".

If listeners are doing a computation that requires long time, this can cause event queue to reach its maximum capacity
and lost of events. For map and multiMap "hazelcast.event.thread.count" can be configured to a higher value so that
less collision occurs for keys, therefore workers threads will not block each other in StripedExecutor. For list,set,
topic and queue heavy work should be offloaded to another thread. Notice that, in order to preserver order guarantee
user should implement similar logic with StripedExecutor in offloaded thread pool. 

To how to configure these parameters see [Advanced Configuration Properties]#advanced-configuration-properties
