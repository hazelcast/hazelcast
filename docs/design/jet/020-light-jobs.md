---
title: 020 - Light Jobs
description: Faster lifecycle for short jobs
---

## Summary

Jet was originally designed with unbounded jobs and large batch
processing in mind. Not much attention was paid to the deployment
performance. Over time, a need to efficiently run very small batches
arose, in which case the startup and cleanup overhead became
significant. With the decision to run all SQL queries on Jet, it became
a requirement.

## Main Causes of the Slowness

Depending on job design and network roundtrip latency, job
initialization may easily take a few milliseconds. We identified several
main causes of this:

- execution lifecycle is managed with 3 operations: `init`, `start`,
  `complete`, which run sequentially (i.e. after all members respond to
  the previous operation, the next operation starts)

- we store the job state in IMaps: multiple IMap writes and reads are
  performed for each execution

- Jet provides multiple features that are not needed in small jobs:
  especially fault tolerance, code deployment and attached files

## Implemented solution

To simplify the solution, we introduce a new _light job_ - a job which
supports only limited features. Most notably it lacks fault tolerance.
Such a job can only be submitted and then joined or cancelled.

It allowed us to:

- reduce the number of lifecycle operations to 1: execution will start
  right after the `init` operation and will be cleaned up right after it
  completes. We don't need to take snapshot phases into account, nor can
  the execution be restarted or suspended.

- avoid storing any metadata in IMaps - job metadata is stored only in
  the coordinator node's memory. If the coordinator fails, the job
  fails.

- enable any member to become a coordinator, not just the master (the
  oldest) member in the cluster. We don't need to transfer the
  coordination to another member after a failure.

We removed the `complete` operation even from normal jobs, keeping only
`init` and `start`. The execution is cleaned up immediately after it
completes locally, not waiting for the `complete` operation.

## Implementation challenges

As a consequence of these changes, multiple new race conditions have
become possible.

### Job receives data before it is initialized

We addressed this race by moving the received packet buffer to
`ExecutionContext`. The `ExecutionContext` is created if one is not
found when data arrives. When later the `init` operation arrives, we
create the processors and they will process the accumulated data.

### Job receives data, but is never initialized

This can happen e.g. if the coordinator dies or the operation is lost.
We clean up the accumulated data in [Light Job
Checker](#light-job-checker).

### Job receives data after the execution completed

It's the same as the previous race - because we delete all job data
after the execution terminates, when more data arrives, we don't know if
it's a new job or a job that already completed. The solution is the
same: the [Light Job Checker](#light-job-checker).

Note that this is not possible in normal conditions because execution
completes after receiving a `DONE_ITEM` for all edges, and there must be
no data after `DONE_ITEM`. But it is possible if the job fails or is
cancelled: members don't cancel at the same time.

### Snapshot operation received after execution terminated locally

This affects normal jobs due to the removal of the `complete` operation.
When a member receives a snapshot operation (the
`SnapshotPhase1Operation` or `SnapshotPhase2Operation`), it doesn't know
if the execution terminated successfully or not. If successfully, it
should respond with an empty response. If it failed, it should respond
with failure.

To address this, we made the operations throw
`ExecutionNotFoundException`. If the coordinator receives it, it has to
look at the response of the `start` operation. If the execution failed,
make the snapshot fail too. If it completed normally, ignore it.

## Light Job Checker

The Light Job Checker looks for two things at regular intervals:

1. Uninitialized executions: executions for which no `init` operation
   was received. An uninitialized execution can be a result of the races
   mentioned above. None of its processors are running, but it may hold
   some unprocessed data packets. We delete such executions after 5
   minutes. The reason for such a long timeout is that if the execution
   was eventually initialized later, we will lose data and it will go
   undetected. The amount of data held isn't high because it's limited
   by the backpressure mechanism - receivers haven't acknowledged any
   data yet.

2. Initialized executions: we send a `CheckLightJobsOperation` with the
   list of all initialized executions to their coordinators. The
   coordinator replies with those executions from the list which it
   doesn't know. This cleans up executions that were, for example,
   initialized after termination.

This approach is simple and robust. Any execution leak will be
eventually removed. A hard-coded interval is 1 second. No operation is
sent if no light job is running.

We also considered to piggy-back on the flow-control mechanism to check
light jobs, but this feature isn't performance-critical and it's simpler
to do it separately.

## Visibility of Light Jobs in Management Center

Management Center accesses the internal IMaps to display job data. For
light jobs we will have to implement a new `GetLightJobsOperation` that
will be sent to all members and they will reply with a list of light
jobs they coordinate.

## Graceful Shutdown

When a Jet member is shut down, it first gracefully terminates all
executions. That means, fault tolerant jobs will save a snapshot and
process no data after it, so even sinks with at-least-once guarantee
don't produce duplicates.

However, this mechanism is not needed for non-fault-tolerant jobs, which
includes all light jobs. Even though we terminate non-fault-tolerant
normal jobs during graceful shutdown, we didn't implement the shutdown
of light jobs. Those will be terminated abruptly after the member shuts
down.

## Java Serialization of Processors

After the job roundtrip got to microsecond scale, the serialization time
for Jet processors became non-negligible, though not major. It might be
worthwhile to convert the common processors to use
`IdentifiedDataSerializable`.
