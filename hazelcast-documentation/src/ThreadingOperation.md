
### Operation Threading

There are 2 types of operations:

* Operations that are aware of a certain partition, e.g. `IMap.get(key)`.
* Operations that are not partition aware, such as the `IExecutorService.executeOnMember(command,member)` operation.

Each of these operation types has a different threading model, explained below.

#### Partition-aware Operations

To execute partition-aware operations, an array of operation threads is created. The size of this array has a default value of two times the number of cores and a minimum value of 2. This value can be changed using the `hazelcast.operation.thread.count` property.

Each operation-thread has its own work queue and it will consume messages from this work queue. If a partition-aware 
operation needs to be scheduled, the right thread is found using the formula below.

`threadIndex = partitionId % partition-thread-count`

After the `threadIndex` is determined, the operation is put in the work queue of that operation-thread. This means that:

 * a single operation thread executes operations for multiple partitions; if there are 271 partitions and
 10 partition-threads, then roughly every operation-thread will execute operations for 27 partitions. 

 * each partition belongs to only 1 operation thread. All operations for a partition will always 
 be handled by exactly the same operation-thread. 

 * no concurrency control is needed to deal with partition-aware operations because once a partition-aware
 operation is put on the work queue of a partition-aware operation thread, only 
 1 thread is able to touch that partition.

Because of this threading strategy, there are two forms of false sharing you need to be aware of:

* false sharing of the partition: two completely independent data structures share the same partitions; e.g. if there
 is a map `employees` and a map `orders`, the method `employees.get(peter)` running on partition 25 may be blocked
 by a `map.get` of `orders.get(1234)` also running on partition 25. If independent data structure share the same partition,
 a slow operation on one data structure can slow down the other data structures.
 
* false sharing of the partition-aware operation-thread: each operation-thread is responsible for executing
 operations of a number of partitions. For example, thread-1 could be responsible for partitions 0,10,20,... thread-2 for partitions
 1,11,21,... etc. If an operation for partition 1 takes a lot of time, it will block the execution of an operation of partition
 11 because both of them are mapped to exactly the same operation-thread.

You need to be careful with long running operations because you could starve operations of a thread. 
As a general rule, the partition thread should be released as soon as possible because operations are not designed
to execute long running operations. That is why, for example, it is very dangerous to execute a long running operation 
using `AtomicReference.alter` or an `IMap.executeOnKey`, because these operations will block other operations to be executed.

Currently, there is no support for work stealing. Different partitions that map to the same thread may need to wait 
till one of the partitions is finished, even though there are other free partition-operation threads available.

**Example:**

Take a 3 node cluster. Two members will have 90 primary partitions and one member will have 91 primary partitions. Let's
say you have one CPU and 4 cores per CPU. By default, 8 operation threads will be allocated to serve 90 or 91 partitions.

#### Non Partition-aware Operations

To execute non partition-aware operations, e.g. `IExecutorService.executeOnMember(command,member)`, generic operation 
threads are used. When the Hazelcast instance is started, an array of operation threads is created. The size of this array 
has a default value of the number of cores divided by two with a minimum value of 2. It can be changed using the 
`hazelcast.operation.generic.thread.count` property. This means that:

* a non partition-aware operation-thread will never execute an operation for a specific partition. Only partition-aware
  operation-threads execute partition-aware operations. 

Unlike the partition-aware operation threads, all the generic operation threads share the same work queue: `genericWorkQueue`.

If a non partition-aware operation needs to be executed, it is placed in that work queue and any generic operation 
thread can execute it. The big advantage is that you automatically have work balancing since any generic operation 
thread is allowed to pick up work from this queue.

The disadvantage is that this shared queue can be a point of contention. We do not practically see this in 
production because performance is dominated by I/O and the system is not executing very many non partition-aware operations.
 
#### Priority Operations
 
In some cases, the system needs to execute operations with a higher priority, e.g. an important system operation. To support priority operations, we do the following:

* For partition-aware operations: each partition thread has its own work queue. But apart from that, it also has a priority
  work queue. It will always check this priority queue before it processes work from its normal work queue.

* For non partition-aware operations: next to the `genericWorkQueue`, there also is a `genericPriorityWorkQueue`. So when a priority operation
 needs to be executed, it is put in this `genericPriorityWorkQueue`. And just like the partition-aware operation threads, a generic
 operation thread will first check the `genericPriorityWorkQueue` for work. 
 
Because a worker thread will block on the normal work queue (either partition specific or generic), a priority operation
may not be picked up because it will not be put on the queue where it is blocking. We always send a 'kick the worker' operation that does 
nothing else than trigger the worker to wake up and check the priority queue. 

#### Operation-response and Invocation-future

When an Operation is invoked, a `Future` is returned. Let's take the example code below. 

```java
GetOperation operation = new GetOperation( mapName, key )
Future future = operationService.invoke( operation )
future.get)
```

The calling side blocks for a reply. In this case, `GetOperation` is set in the work queue for the partition of `key`, where
it eventually is executed. On execution, a response is returned and placed on the `genericWorkQueue` where it is executed by a 
"generic operation thread". This thread will signal the `future` and notifies the blocked thread that a response is available. 
In the `future`, we will expose this Future to the outside world, and we will provide the ability to register a completion listener so you can do asynchronous calls. 

#### Local Calls

When a local partition-aware call is done, an operation is made and handed over to the work queue of the correct partition operation thread,
and a future is returned. When the calling thread calls get on that future, it will acquire a lock and wait for the result 
to become available. When a response is calculated, the future is looked up, and the waiting thread is notified.  

In the future, this will be optimized to reduce the amount of expensive systems calls, such as `lock.acquire`/`notify` and the expensive
interaction with the operation-queue. Probably, we will add support for a caller-runs mode, so that an operation is directly executed on
the calling thread.

