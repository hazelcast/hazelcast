# SQL Multithreaded Execution

## Overview

The Hazelcast Mustang engine executes queries in parallel. This document describes the design of the multithreaded execution
environment of the engine.

The document doesn't discuss operator-level parallelism, which is a different topic.

The rest of this document is organized as follows. In section 1 we discuss the existing threading infrastructure of Hazelcast
IMDG and Hazelcast Jet. In section 2 we analyze why the existing infrastructure is inappropriate for query execution and then
describe the design of the Hazelcast Mustang execution environment. In section 3 we discuss possible alternative approaches
that were rejected.

## 1 Existing Infrastructure

Hazelcast IMDG uses staged event-driven architecture (SEDA) for message processing. During execution, a message passes through
several thread pools (stages), each optimized for a specific type of workload. We now describe stages that exist in Hazelcast.

### 1.1 IO Pool

Hazelcast uses a dedicated thread pool for message send and receive, which will be referred to as **IO Pool** in this paper.
Each thread from the IO pool maintains a subset of connections to remote members. Consider that we have a sender member (S)
and a receiver member (R). The typical execution flow is organized as follows:
1. The message is added to the queue of a single IO thread, and the thread is notified
1. The sender IO thread wakes up and sends the message over the network
1. A receiver IO thread is notified by the operating system on receive
1. The receiver IO thread wakes up, determines the next execution stage, adds the message to the stage's queue and notifies the
stage
1. The next execution stage processes the message

*Snippet 1: Message execution flow*
```
Stage(S)                 IO(S)        IO(R)                Stage(R)
   |----enqueue/notify->--|            |                      |
   |                      |----send->--|                      |
   |                      |            |----enqueue/notify->--|
```

We now discuss the organization of different execution stages.

### 1.2 Partition Pool

A message may have a logical **partition**, which is a positive integer number. Messages with defined partition are routed to
a special thread pool, which we refer to as **partition pool**. The pool has several threads. Every thread has a dedicated task
queue. Partition of the message is used to determine the exact thread which will process the message:
`threadIndex = partition % threadCount`.

The partition pool has the following advantages:
1. Only one thread processes messages with the given partition so that processing logic may use less synchronization
1. Dedicated thread queues reduce contention on enqueue/deque operations

The partition pool has the following disadvantage:
1. There is no balancing between threads: a single long-running task may delay other tasks from the same partition
indefinitely; likewise, an imbalance between partitions may cause resource underutilization

The partition pool is thus most suitable for small tasks that operate on independent physical resources, and that are
distributed equally between logical partitions. An example is `IMap` operations, which operate on separate physical
partitions, such as `GET` and `PUT`.

Since the partition is a logical notion, it is possible to multiplex tasks from different components to a single partition pool.
For example, CP Subsystem schedules tasks, all with the same partition, to the partition pool to ensure total processing order.

### 1.3 Generic Pool

If a message doesn't have a logical partition, it is submitted to the **generic pool**. This is a conventional thread pool with
a shared blocking queue. It has inherent balancing capabilities. But at the same time, this pool may demonstrate less than
optimal throughput when a lot of small tasks are submitted due to contention on the queue.

### 1.4 Hazelcast Jet Pool

Hazelcast Jet uses its own cooperative pool to execute Jet jobs. Every thread has its own queue of jobs that are executed
cooperatively. There is no balancing: once the job is submitted to a specific thread, it is always executed in that thread.

IO pool doesn't notify the Jet pool about new data batch ("push"). Instead, the message is just enqueued, and the Jet thread
checks the queue periodically ("poll").

## 2 Design

We now define the requirements to Hazelcast Mustang threading model, analyze them concerning existing infrastructure, and
define the design.

### 2.1 Requirements

The requirements are thread safety, load balancing, and ordered message processing.

First, the infrastructure must guarantee that operator execution is thread-safe. That is, the stateful operator should not be
executed by multiple threads simultaneously. This simplifies operator implementations and makes them more performant.
Hazelcast Jet follows this principle, as only one thread may execute a particular job. However, Hazelcast Jet pool doesn't
satisfy the load balancing requirement discussed below.

Second, the execution environment must support load balancing. Query execution may take a long time to complete. If several query
fragments have been assigned to a single execution thread, it should be possible to reassign them to idle threads dynamically.
Hence neither partition pool nor Hazelcast Jet pool designs are applicable to Hazelcast Mustang because they lack balancing
capabilities.

Third, it should be possible to execute some tasks in order. That is, if task `A` is received before task `B`, then it
should be executed before `B`. It is always possible to implement an ordering guarantee with the help of additional
synchronization primitives, but it increases complexity and typically reduces performance. So we prefer to have a threading
infrastructure with ordering guarantees, such as in the partition pool. Examples of tasks requiring ordered processing:
1. Query cancel should be executed after query start to minimize resource leaks
1. The N-th batch from the stream should be processed before the (N+1)-th batch, as described in [[1]] (p. 1.3)

### 2.1 General Design

We define the taxonomy of tasks related to query execution.

First, the engine must execute query fragments, i.e., advance Volcano-style operators, as explained in [[2]] (p. 3). Fragment
execution is initiated in response to query start or data messages. Fragment execution may take significant time to complete.

Second, the engine must process query operations described in [[1]] (p. 3), such as query start, query cancel, batch arrival,
and flow control. These operations take short time to complete. Moreover, query start operation may trigger the parallel
execution of several fragments.

Given the different nature of query operations and fragment execution, we split them into independent stages, called
**operation pool** and **fragment pool**. The former executes query operations, and the latter executes fragments. This design
provides a clear separation of concerns and allows us to optimize stages for their tasks as described below, which improves
performance. On the other hand, this design introduces an additional thread notification, as shown in the snippet below, which
may negatively affect performance. Nevertheless, we think that the advantages of this approach outweigh the disadvantages.

*Snippet 2: Query start flow (receiver only)*
```
IO               Operation pool         Fragment pool
|----enqueue/notify-->-|                      |
|                      |----enqueue/notify-->-|
```

### 2.2 Operation Pool

Every network message received by the IO pool is first submitted to the operation pool.

The pool is organized as a fixed pool of threads with dedicated per-thread queues. Dedicated queues reduce contention and
increase the throughput, which is important given that processing of a single message takes little time.

Every message may define an optional logical partition. If the partition is not defined, a random thread is picked for message
processing. If the partition is defined, the index of executing thread is defined as `threadIndex = partition % threadCount`.
Two messages with the same partition are guaranteed to be processed by the same thread, thus providing ordering guarantees.

Some messages may trigger query fragment execution, as described below. In this case, a fragment execution task is created and
submitted for execution to the fragment pool.
1. `execute` - triggers the execution of one or more fragments
1. `batch` and `flow_control` - trigger execution of one fragment defined by query ID and edge ID, as described in [[1]]

### 2.3 Fragment Pool

The fragment pool is responsible for the execution of individual query fragments. Fragment execution is always initiated by
a task from the operation pool.

Unlike operations, fragment execution may take arbitrary time depending on the query structure. It is, therefore, important to
guarantee high throughput for short fragments, while still providing load balancing for long fragments. The ideal candidate
is a thread pool with dedicated per-thread queues and work-stealing. For this reason, we choose JDK's `ForkJoinPool` as a
backbone of the fragment pool. We create a separate instance of the `ForkJoinPool` rather than using `ForkJoinPool.commonPool()`
to avoid interference with user workloads.

We may decide to introduce multiple fragment pools for better resource management in the future. This will help limit the
maximum number of CPU cores dedicated to a particular workload. An example of this approach is **Resource Pools** in MemSQL [[3]].

## 3 Alternative Approaches

Several other approaches were considered but then rejected. This section explains these approaches and the rejection
reasons.

### 3.1 Use Single Pool

It is possible to use a single pool for both operation and fragment processing and remove the additional notifications for
fragment execution. The problem is that an operation task is short and requires fast response, while fragment execution is
potentially a long-running task. Consider two examples that demonstrate when this approach doesn't work well.

**Example 1**:

Consider that long-running fragments occupied all threads in the pool. Then a `cancel` message arrives, but it cannot be
processed because all threads are busy. To fix it, we would have to introduce a separate priority queue, which should be
checked periodically by fragment tasks. This may lead to wasted CPU cycles and frequent interrupts to fragment tasks.

**Example 2**

Consider a fragment which occupied the thread `A`. Then a `batch` message arrives, which should resume execution of another
fragment. Given that the processing of `batch` message should be ordered, it has logical partition defined. It may happen that
the thread chosen for `batch` operation processing will also be the thread `A`, that is currently busy. So we either delay
the resume of the fragment associated with the batch, reducing the throughput, or we introduce a priority queue and interrupt
the running fragment, wasting CPU cycles.

### 3.2 Use IO Pool

Another approach is to use the IO pool to process short operations, such as `batch` processing, to eliminate an additional
notification. Hazelcast Jet uses this approach for batch processing, but with two important traits:
1. The "poll" approach without thread notifications is used. This is inappropriate for SQL where low latency is required
1. The batch destination is encoded inside the network `Packet` at known positions, and therefore no expensive deserialization
is needed. We expect SQL protocol to change frequently during first releases, so it is not desirable to have an additional
contract on message structure at the moment

### 3.3 Use Partition Pool

The partition pool cannot be used for fragment execution, because it may significantly delay other operations, such as `IMap`
'GET' and 'PUT'. However, it is possible to use the partition pool instead of the operation pool. The partition pool and query
operation pool have a very similar design. By merging them, we may decrease the total number of threads and possibly reduce the
number of context switches. But with this design, query operations will interfere with other workloads, that are potentially
long-running (e.g., `EntryProcessor`) or skewed (e.g., bad data distribution), causing unpredictable side effects.

We may revisit this decision at any time in the future because it doesn't affect the network protocol.

### 3.4 Use Generic Pool

It is possible to execute query fragments in the generic pool. But if we do so, we would have to restrict the execution of
queries from any tasks that may potentially be executed in the generic pool.

For example, it will be impossible to execute SQL from `IScheduledExecutorService`. Otherwise, thread starvation is possible as
described below:
1. User starts many tasks from the `IScheduledExecutorService`, and they occupy all generic threads
1. Every task starts execution of the query
1. But query execution cannot complete because it needs free generic threads, resulting in a distributed deadlock

[1]: 03-network-protocol.md "SQL Network Protocol"
[2]: 02-operator-interface.md "SQL Operator Interface"
[3]: https://docs.memsql.com/v6.8/guides/cluster-management/operations/setting-resource-limits/ "MemSQL: Setting Resource Limits"
