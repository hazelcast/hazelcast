# SQL Parallel Execution

## Overview

The Hazelcast Mustang engine executes queries in parallel. This document describes the design of the parallel execution
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

Hazelcast uses a dedicated thread pool for a message send and receive, which will be referred to as **IO Pool** in this paper.
Each thread from the IO pool maintains a subset of connections to remote members. Consider that we have a sender member (S)
and a receiver member (R). The typical execution flow is organized as follows:
1. The message is added to the queue of a single IO thread, and the thread is notified.
1. The sender IO thread wakes up and sends the message over the network.
1. A receiver IO thread is notified by the operating system on receive.
1. The receiver IO thread wakes up, determines the next execution stage, adds the message to the stage's queue and notifies the
   stage.
1. The next execution stage processes the message.

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
1. Only one thread processes messages with the given partition so that processing logic may use less synchronization.
1. Dedicated thread queues reduce contention on enqueue/dequeue operations.

However, there is no load balancing in the partition pool: a single long-running task may delay other tasks from the same 
partition indefinitely. An imbalance between partitions may cause low resource utilization.

The partition pool is thus suitable for small tasks that operate on independent physical resources, and that are
distributed equally between logical partitions. An example is `IMap` operations, which operate on separate physical
partitions, such as `GET` and `PUT`.

Since the partition is a logical notion, it is possible to multiplex tasks from different components to a single partition pool.
For example, CP Subsystem schedules tasks, all with the same partition, to the partition pool to ensure total processing order.

### 1.3 Generic Pool

If a message doesn't have a logical partition, it is submitted to the **generic pool**. This is a conventional thread pool with
a shared blocking queue. It has inherent balancing capabilities. At the same time, this pool may demonstrate less than
optimal throughput when a lot of small tasks are submitted due to contention on the queue.

### 1.4 Hazelcast Jet Pool

Hazelcast Jet uses its own cooperative thread pool to execute Jet jobs. A job is split into "tasklets", each vertex in the DAG is
backed by one or more tasklets on each cluster member. Every tasklet is assigned to one cooperative thread, the thread then
executes the tasklets in a loop. There is no balancing: once a tasklet is submitted to a specific thread, it is always executed in
that thread. Non-cooperative tasklets (those handling a blocking API such as JDBC) run on dedicated threads.

IO pool doesn't notify the Jet pool about new data batch ("push"). Instead, the message is just enqueued, and the Jet thread
checks the queue periodically ("poll").

## 2 Design

We now define the requirements to Hazelcast Mustang threading model, analyze them concerning existing infrastructure, and
define the design.

### 2.1 Requirements

The requirements are thread safety, load balancing, and ordered processing.

First, the infrastructure must guarantee that operator execution is thread-safe. That is, the stateful operator should not be
executed by multiple threads simultaneously. This simplifies operator implementations and makes them more performant.
Hazelcast Jet follows this principle, as only one thread may execute a particular tasklet. However, Hazelcast Jet pool doesn't
satisfy the load balancing requirement discussed below.

Second, the execution environment must support load balancing. Query execution may take a long time to complete. If several query
fragments have been assigned to a single execution thread, it should be possible to reassign them to idle threads dynamically.
Neither partition pool nor Hazelcast Jet pool designs are applicable to Hazelcast Mustang because they lack balancing
capabilities.

Third, it should be possible to execute some messages in order. For example, for the ordered stream, the N-th batch should be 
processed before the (N+1)-th batch, as described in [[1]] (p. 1.3)

### 2.1 General Design

We define the taxonomy of tasks related to query execution.

First, the engine must execute query fragments, i.e., advance Volcano-style operators, as explained in [[2]] (p. 3). Fragment
execution is initiated in response to query start or data messages. Fragment execution may take significant time to complete.

Second, the engine must execute system operations, such as query cancel and query check. These operations take short time to 
complete and must be executed as soon as possible. Moreover, query start operation may trigger the parallel
execution of several fragments.

Given the different nature of fragment and system operations, we split execution into two independent pools:
1. **fragment pool** - executes fragments.
1. **system pool** - executes system operations.
   
### 2.2 Fragment Pool

The fragment pool is responsible for the execution of individual query fragments.

Fragment execution may take arbitrary time depending on the query structure. It is, therefore, important to
guarantee high throughput for short fragments, while still providing load balancing for long fragments. The ideal candidate
is a thread pool with dedicated per-thread queues and work-stealing. For this reason, we choose JDK's `ForkJoinPool` as a
backbone of the fragment pool. We create a separate instance of the `ForkJoinPool` rather than using `ForkJoinPool.commonPool()`
to avoid interference with user workloads.

We may decide to introduce multiple fragment pools for better resource management in the future. This will help limit the
maximum number of CPU cores dedicated to a particular workload. An example of this approach is **Resource Pools** in MemSQL [[3]].

### 2.3 System Pool

The system pool is a pool dedicated for `cancel` and `check` operations execution. With a separate pool, if the fragment pool 
is fully occupied with long-running queries, the `cancel` and `check` operations would be executed in a timely manner still.

## 3 Rejected Alternatives

Several other approaches were considered but then rejected:
1. It is possible to use a single pool for both fragment and system operation processing. The problem is that a system operation 
   is short and requires fast response, while fragment execution is potentially a long-running task.
1. The partition pool cannot be used for fragment execution, because it lacks load balancing and may significantly delay other 
   operations, such as `IMap.GET/PUT`
1. The generic pool cannot be used for fragment execution either, because (1) long-running queries may stall concurrent
   operation; (2) we would not be able to initiate queries from the other tasks that are executed in the generic pool, such as 
   `IScheduledExecutorService` because it is prone to starvation.

[1]: 03-network-protocol.md "SQL Network Protocol"
[2]: 02-operator-interface.md "SQL Operator Interface"
[3]: https://docs.memsql.com/v6.8/guides/cluster-management/operations/setting-resource-limits/ "MemSQL: Setting Resource Limits"
