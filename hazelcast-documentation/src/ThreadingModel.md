


## Threading Model

Your application server will have its own threads.

Hazelcast manages its own threads.

It will have:

* ExecutorService threads
* NIO Threads
* Partition Threads

Because execution can happen with ExecutorService on the Members, it is important to lock data.

A partition on a member only has one Partition Thread. Operations on one partition are queued.

A Member will be using a thread pool, so partition work may have to wait for

## Operation threading:

There are 2 types of operations:

* operations that are aware of a certain partition, e.g. a map.get
* operations that are not partition aware e.g. map.size

Each of these 2 operation types, has a different threading model. 

### Threading model of partition aware operations

To execute partition aware operations, an array of OperationThreads is created. This array is by default 2x number of cores
big and can be changed using the 'hazelcast.operation.thread.count'  property.

Each thread has its own workqueue and it will consumes messages from this workqueue. If an partition-aware-operation needs
to be scheduled, the right thread is found using: threadindex = partitionid mod partition-thread-count. And then the operation
is put in the workqueue of that worker.

This means that a single partition thread can execute operations for multiple partitions; if there are 271 partitions and
10 partition-threads, then roughly every thread will execute of operations of 10 partitions. If one partition is slow, e.g.
a MapEntryProcessor is running which takes time, it will block the execution of other operations that map to the same 
operation-thread. 

Currently there is no support for work stealing; so it could be that different partitions, that map to
the same thread, need to wait till one of the partitions is finished, even though there are other free partition-operation threads
available.

### Threading model of non partition aware operations

To execute partition unware operations, they are called generic operation threads, e.g. map.size, an array of OperationThreads 
is created. This array also is by default 2x number of cores and can be changed using the 
'hazelcast.operation.generic.thread.count' property.

Unlike the partition-aware operation threads, all the generic operation threads, they share the same workqueue: genericWorkQueue. 
If an  partition unware operation needs to be executed, it is placed in that workqueue and any generic operation thread can execute it.
The big advantage is that you automatically have work balancing since any generic operation thread is allowed to pick up work from 
 this queue. The disadvantage is that this shared queue can be a contention point; but we have not seen this in in tests since
 performance is dominated by IO and the system isn't executing that many partition unaware operations.
 
### Priority Operations
 
In some cases the system needs execute operations with a higher priority, e.g. an important system operation. To support priority 
operations we do the following:

* for partition aware operations: each partition thread has of course its own work queue. But apart from that it also has a priority 
  workqueue. It will always check this priority queue, before it is going to process work from its normal work queue.
* for partition unware operations: next to the genericWorkQueue, there also is a priorityWorkQueue. So when an priority operation
 needs to be executed, it is put in this genericPriorityWorkQueue. And just like the partition aware operaton threads, a generic 
 operation thread, will first check the genericPriorityWorkQueue for work. 
 
Because a worker thread will block on the normal work queue (either partition specific or generic) it could be that a priority operation
is not picked up because it will not be put on the queue it is blocking on, we always send a 'kick the worker' operation that does 
nothing else than trigger the worker to wakeup and check the priority queue. 

### Threading model of operation-response and invocation-future

todo: