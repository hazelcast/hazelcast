---
title: Fault Tolerance
description: Fault tolerance guarantees and options provided by Jet.
---

Hazelcast Jet doesn't delegate its cluster management and fault
tolerance concerns to an outside system like ZooKeeper. It reuses the
groundwork implemented for the IMDG: cluster management and the `IMap`,
and adds its own implementation of Chandy-Lamport distributed snapshots.
If a cluster member fails, Jet will restart the job on the remaining
members, restore the state of processing from the last snapshot, and
then seamlessly continue from that point.

## Processing Guarantee is a Shared Concern

When you configure the processing guarantee for your job as
*exactly-once* or *at-least-once*, Hazelcast Jet uses the distributed
snapshotting feature to store all the **internal** computation state to
an `IMap`. However, this on its own isn't enough to provide the
processing guarantee because the snapshot must cover the entire
pipeline, including the external changes performed by sources and sinks.
Hazelcast Jet requires certain guarantees from sources and sinks in a
fault-tolerant data pipeline.

When the job is restarting after a node failure, Jet resets the whole
data pipeline to the state of the last snapshot. More technically,
processors can save arbitrary data to the snapshot and Jet will present
that same data to the processors after a restart. Jet performs such
snapshots in regular intervals. Sources can cooperate with the job in
multiple ways:

- **Replayable sources:** a replayable source can seek to certain
  position and re-read events from that positions multiple times. An
  example is Apache Kafka or an IMap Journal. Such source saves the
  offset(s) to the snapshot and in case of restart it continues from the
  saved position.

- **Acknowledging sources:** such sources acknowledge messages after
  fully processing them. Typical example is a JMS queue. Unacknowledged
  messages are delivered again in case the job fails. Such sources need to
  do two things: (1) acknowledge messages only after a next snapshot is
  completed and (2) save message IDs for deduplication to snapshot in case
  the job fails after a snapshot is completed but before it manages to
  acknowledge the consumption. Those IDs are used to drop re-delivered
  messages after a restart.

Sinks can cooperate in different ways:

- **Transactional sinks:** such sinks write their output using a
  transaction and they commit it only after the snapshot is completed.
  Since there are multiple parallel workers writing the data, each with
  its own transaction, Jet employs two-phase commit to ensure that either
  all participants commit or all roll back. An example is JMS, JDBC or
  Kafka sinks.

- **Idempotent writes:** Idempotent operation is an operation that, if
  performed multiple times, has the same effect as if performed once. An
  example is writing to an IMap: `map.put("key", "value")` has the same
  effect if you execute it once or twice. Such sinks only need to ensure
  that all in-flight operations are finished before each snapshot is
  performed. That is they need to wait for async operations to finish or
  fsync writes to files. But it's not enough to just use such a sink: you
  also need to ensure that the keys are stable. For example if you use
  random UUID for the key, it won't work, the job must produce identical
  keys after a restart. Also if you process the journal for a map, the
  journal will contain the update event multiple times.

## Distributed Snapshot

The technique Jet uses to achieve fault tolerance is called a
“distributed snapshot”, described in a [paper by Chandy and
Lamport](http://lamport.azurewebsites.net/pubs/chandy.pdf). At regular
intervals, Jet raises a global flag that says "it’s time for another
snapshot". All processors belonging to source vertices observe the flag,
save their state, emit a barrier item to the downstream processors and
resume processing.

As the barrier item reaches a processor, it stops what it’s doing and
saves its state to the snapshot storage. Once complete, it forwards the
barrier item to its downstream processors and resumes. The same story
repeats in the downstream processors, eventually reaching the sink
processors. When they complete, the snapshot is done.

This is the basic story, but due to parallelism, in most cases a
processor receives data from more than one upstream processor. It will
receive the barrier item from each of them at separate times, but it
must start taking a snapshot at a single point in time. There are two
approaches it can take, as explained below.

### Exactly-Once

With exactly-once configured, as soon as the processor gets a barrier
item in any input stream (from any upstream processor), it must stop
consuming it until it gets the same barrier item in all the streams:

<img src="/docs/assets/exactly-once-1.png"
     alt="Exactly-once processing: received one of two barrier items"
     width="70%">

1. Stream X is at the barrier, Y not yet. The processor must not accept
   any more X items.

<img src="/docs/assets/exactly-once-2.png"
     alt="Exactly-once processing: received both barrier items"
     width="70%">

2. At the barrier in both streams, taking a snapshot.

<img src="/docs/assets/exactly-once-3.png"
     alt="Exactly-once processing: forwarding the barrier"
     width="70%">

3. Snapshot done, barrier forwarded. Processor resumes consuming all
   streams.

### At-Least-Once

With at-least-once configured, the processor can keep consuming all the
streams until it gets all the barriers, at which point it stops to take
the snapshot:

<img src="/docs/assets/at-least-once-1.png"
     alt="At-Least-once processing: received one barrier"
     width="70%">

1. Stream X is at the barrier, Y not yet. Carry on consuming all streams.

<img src="/docs/assets/at-least-once-2.png"
     alt="At-Least-once processing: received both barriers"
     width="70%">

2. At the barrier in both streams, already consumed x1 and x2. Taking a snapshot.

<img src="/docs/assets/at-least-once-3.png"
     alt="At-Least-once processing: forward the barrier"
     width="70%">

3. Snapshot done, barrier forwarded.

Even though `x1` and `x2` occur after the barrier, the processor
consumed and processed them before processing the barrier, updating its
state accordingly. If the computation job stops and restarts, this state
will be restored from the snapshot and then the source will replay `x1`
and `x2`. The processor will think it got two new items.

## Data Safety

### In-Memory Snapshot Storage

Jet backs up the state to its own `IMap` objects. `IMap` is a replicated
in-memory data structure, storing each key-value pair on a configurable
number of cluster members. By default it makes a single backup copy,
resulting in a system that tolerates the failure of a single member at a
time. The cluster recovers its safety level by re-establishing all the
missing backups, and when this is done, another node can fail without
data loss. You can set the backup count in the configuration, for
example:

```yaml
hazelcast-jet:
  instance:
    backup-count: 2
```

If multiple members fail simultaneously, some data from the backing
`IMap`s can be lost. Jet detects this by counting the entries in the
snapshot `IMap` and it won't run a job with missing data.

## Split-Brain Protection

There is a special kind of cluster failure, popularly called the "Split
Brain". It occurs due to a complex network failure (a network
*partition*) where the graph of live connections among cluster nodes
falls apart into two islands. In each island it seems like all the other
nodes failed, so the remaining cluster should self-heal and continue
working. Now you have two Jet clusters working in parallel, each running
all the jobs on all the data.

Hazelcast Jet offers a mechanism to mitigate this risk: split-brain
protection. It works by ensuring that a job can be restarted only in a
cluster whose size is more than half of what it ever was. Enable
split-brain protection like this:

```java
jobConfig.setSplitBrainProtection(true);
```

If there’s an even number of members in your cluster, this may mean the
job will not be able to restart at all if the cluster splits into two
equally-sized parts. We recommend having an odd number of members.

Note also that you should ensure there is no split-brain condition at
the moment you are introducing new members to the cluster. If that
happens, both sub-clusters may grow to more than half of the previous
size, circumventing the split-brain protection.

<!-- ### Disk Snapshot Storage -->

<!-- In-memory Snapshot Storage doesn’t cover the case when the entire
cluster must shut down. -->

<!-- The Lossless Cluster Restart allows you to gracefully shut down the
cluster at any time and have the snapshot data of all the jobs
preserved. After you restart the cluster, Jet automatically restores the
data and resumes the jobs. -->

<!-- Since the Hot Restart data is saved locally on each member, all the
members must be present after the restart for Jet to be able to reload
the data. Beyond that, there’s no special action to take: as soon as the
cluster re-forms, it will automatically reload the persisted snapshots
and resume the jobs. -->

<!-- ## Exported Snapshots -->

<!-- In addition to regular snapshots, you can create exported
snapshots. The lifecycle of the exported snapshot is controlled by
the user: it's created upon user request and is stored in the cluster
until the user decides do remove it. -->

<!--
Exported snapshots are mainly used to update the job: job is cancelled
with a snapshot and a new job is submitted that will use the saved
snapshot for initial state.  -->
