
# Distributed Events

Hazelcast allows you to register for entry events to get notified when events occurred. Event Listeners are cluster-wide so when a listener is registered in one member of cluster, it is actually registering for events originated at any member in the cluster. When a new member joins, events originated at the new member will also be delivered.

An Event is created only if there is a listener registered. If there is no listener registered, then no event will be created. If a predicate provided while registering the listener, predicate should be passed before sending the event to the listener (node/client).

As a rule of thumb, event listener should not implement heavy processes in its event methods which block the thread for long time. If needed, `ExecutorService` can be used to transfer long running processes to another thread and offload current listener thread.



## Event Listeners

- **MembershipListener** for cluster membership events
- **DistributedObjectListener** for distributed object creation and destroy events
- **MigrationListener** for partition migration start and complete events
- **LifecycleListener** for HazelcastInstance lifecycle events
- **EntryListener** for IMap and MultiMap entry events
- **ItemListener** for IQueue, ISet and IList item events (please refer to Event Registration and Configuration sections of [Set](#set) and [List](#list)).
- **MessageListener** for ITopic message events
- **ClientListener** for client connection events

