
### Event Threading

Hazelcast uses a shared event system to deal with components that rely on events like topic, collections listeners and near-cache. 

Each cluster member has an array of event threads and each thread has its own work queue. When an event is produced,
either locally or remote, an event thread is selected (depending on if there is a message ordering) and the event is placed
in the work queue for that event thread.

The following properties
can be set to alter the behavior of the system:

* `hazelcast.event.thread.count`: Number of event-threads in this array. Its default value is 5.
* `hazelcast.event.queue.capacity`: Capacity of the work queue. Its default value is 1000000.
* `hazelcast.event.queue.timeout.millis`: Timeout for placing an item on the work queue. Its default value is 250.

If you process a lot of events and have many cores, changing the value of `hazelcast.event.thread.count` property to
a higher value is a good idea. By this way, more events can be processed in parallel.

Multiple components share the same event queues. If there are 2 topics, say A and B, for certain messages
they may share the same queue(s) and hence event thread. If there are a lot of pending messages produced by A, then B needs to wait.
Also, when processing a message from A takes a lot of time and the event thread is used for that, B will suffer from this. 
That is why it is better to offload processing to a dedicate thread (pool) so that systems are better isolated.

If events are produced at a higher rate than they are consumed, the queue will grow in size. To prevent overloading system
and running into an `OutOfMemoryException`, the queue is given a capacity of 1M million items. When the maximum capacity is reached, the items are
dropped. This means that the event system is a 'best effort' system. There is no guarantee that you are going to get an
event. It can also be that Topic A has a lot of pending messages, and therefore B cannot receive messages because the queue
has no capacity and messages for B are dropped.

