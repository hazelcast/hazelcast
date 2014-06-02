## Threading Model

Your application server has its own thread. Hazelcast does not use these - it manages its own threads.

### Event Threading:

Hazelcast uses a shared event-system do deal with components that rely on events like:

* Topic
* Collections Listeners
* Near-cache 

Under the hood each member has an array of Thread and each thread has its own work-queue. The following properties
can be set to alter the behavior of the system:

* hazelcast.event.thread.count: the number of threads in this array. By default this is 5.
* hazelcast.event.queue.capacity: the capacity of the work-queue. By default 1000000.
* hazelcast.event.queue.timeout.millis: the timeout for placing an item on the work-queue. By default 250 ms.

If you process a lot of events and have many cores, then probably want to change the 'hazelcast.event.thread.count' to
a higher value so that more events can be processed in parallel. 

Multiple components share the same event queues, so if there are 2 topics A,B then it could be that for certain messages
they share the same queue(s) and therefor thread. If there are a lot of pending messages produced by A, then B needs to wait.
Also when processing a message from A takes a lot of time and the event thread is used for that, B will suffer from this. 
That is why it is better to offload processing to a dedicate thread(pool) so that systems are better isolated.

If events are produced at a higher rate than they are consumed, the queue will grow in size. To prevent overloading system
and running into an OOME, the queue is given a capacity of 1M items. When the maximum capacity is reached, the items are
dropped. This means that the event system is a 'best effort' system; there is no guarantee that you are going to get an
event. It can also be that Topic A has a lot of pending messages, and therefor B can't receive messages because the queue
has no capacity and messages for B are dropped.

### IExecutor Threading:

### Operation Threading:

There are 2 types of operations:

* Operations that are aware of a certain partition, e.g. `map.get`
* Operations that are not partition aware, e.g. `map.size`

Each of these types has a different threading model explained below.

#### Partition-aware Operations

To execute partition-aware operations, an array of operation threads is created. Size of this array is by default two times the number of cores. It can be changed using the `hazelcast.operation.thread.count` property.

Each thread has its own work queue and it will consume messages from this work queue. If a partition-aware operation needs
to be scheduled, the right thread is found using the below formula:

`threadIndex = partitionId % partition-thread-count`

And then the operation is put in the work queue of that worker.

This means that a single operation thread can execute operations for multiple partitions; if there are 271 partitions and
10 partition-threads, then roughly every operation thread will execute operations for 10 partitions.

If one partition is slow, e.g. a map EntryProcessor running slow user code, it will block the execution of other partitions
that map to the same operation thread. So it could be that partition 1 and partition 26 of the same map use the same operation
thread because they map to the same partition. It can even be that partition 1 for map X and partition 26 of map Y, which
are completely independent data structures, still contend for the same thread. So you need to be careful about starving
a partition of threading resources.

Currently, there is no support for work stealing; so different partitions, that map to
the same thread may need to wait till one of the partitions is finished, even though there are other free partition-operation threads
available.

<font color='red'>**Example**</font>

Take a 3 node cluster. Two members will have 90 primary partitions and one member will have 91 primary partitions. Let's
say you have one CPU and 4 cores per CPU. By default, 8 operation threads will be allocated to serve 90 or 91 partitions.


#### Non Partition-aware Operations

To execute non partition-aware operations, e.g. `map.size`, generic operation threads are used. When the Hazelcast instance is started,
an array of operation threads is created. Size of this array also is by default two times the number of cores. It can be changed using the
`hazelcast.operation.generic.thread.count` property.

Unlike the partition-aware operation threads, all the generic operation threads share the same work queue: `genericWorkQueue`.

If a non partition-aware operation needs to be executed, it is placed in that work queue and any generic operation thread can execute it.
The big advantage is that you automatically have work balancing since any generic operation thread is allowed to pick up work from 
this queue.

The disadvantage is that this shared queue can be a point of contention; however, practically we do not see this as in production
as performance is dominated by I/O and the system is not executing that many non partition-aware operations.
 
#### Priority Operations
 
In some cases the system needs to execute operations with a higher priority, e.g. an important system operation. To support priority
operations we do the following:

* For partition-aware operations: each partition thread has of course its own work queue. But apart from that, it also has a priority
  work queue. It will always check this priority queue, before it is going to process work from its normal work queue.

* For non partition-aware operations: next to the `genericWorkQueue`, there also is a `genericPriorityWorkQueue`. So when a priority operation
 needs to be executed, it is put in this `genericPriorityWorkQueue`. And just like the partition-aware operation threads, a generic
 operation thread, will first check the `genericPriorityWorkQueue` for work. 
 
Because a worker thread will block on the normal work queue (either partition specific or generic) it could be that a priority operation
is not picked up because it will not be put on the queue it is blocking on. We always send a 'kick the worker' operation that does 
nothing else than trigger the worker to wakeup and check the priority queue. 

#### Operation-response and Invocation-future

ToDo

#### Local Calls

When a local partition-aware call is done, an operation is made and handed over to the work queue of the correct partition operation thread
and a future is returned. When the calling thread calls get on that future, it will acquire a lock and wait for the result 
to become available. When a response is calculated, the future is looked up, and the waiting thread is notified.  

In the future, this will be optimized to reduce the amount of expensive systems calls like `lock.acquire`/`notify` and the expensive
interaction with the operation-queue. Probably, we will add support for a caller-runs mode, so that an operation is directly executed on
the calling thread.