---
title: Processing Guarantees for Stateful Computation
description: How Jet reacts to failures and the processing guaranties it can offer.
id: version-4.4-processing-guarantees
original_id: processing-guarantees
---

With unbounded stream processing comes the challenge of forever
maintaining the continuity of the output, even in the face of changing
cluster topology. A Jet node may leave the cluster due to an internal
error, loss of networking, or deliberate shutdown for maintenance. In
this case Jet must suspend the computation, re-plan it for the smaller
cluster, and then resume in such a way that the state of computation
remains intact. When you add a node to the cluster, Jet must follow the
same procedure in order to start using it.

## Stateless vs. Stateful Computation

A given computation step can be either
[stateless](../api/stateless-transforms.md) or
[stateful](../api/stateful-transforms.md). Basic
examples of the former are
[`map`](../api/stateless-transforms.md#map),
[`filter`](../api/stateless-transforms.md#filter) and
[`flatMap`](../api/stateless-transforms.md#flatmap). Jet doesn't have to
take any special measures to make stateless transforms work in a
fault-tolerant data pipeline because they are pure functions and always
give the same output for the same input.

## Exactly-Once and At-Least-Once Guarantees

The challenges come with stateful computation, and most real-world
pipelines contain at least one such transform. For example, if you have
a `counting` [windowed aggregation](../tutorials/windowing.md) step, for
the count to remain correct the aggregating task must see each item
exactly once. The technical term for this is the **exactly-once
processing guarantee** and Jet supports it. It also supports a lesser
guarantee, **at-least-once**, in which an item can be observed more than
once. It allows more performance for those pipelines that can gracefully
deal with duplicates.

To appreciate the challenges involved, here are some of the techniques
Hazelcast Jet employs to optimize the throughput and latency of a
processing pipeline:

- **Batching.** Jet takes a batch of items at once from a data source.
- **Pipelining.** All processing stages work on data all the time,
  concurrently. That means that, while the first stage works on batch
  *N*, later stages are still working on batches *N - 1*, *N - 2* etc.
- **Volatile State.** Jet stores aggregation state in plain `HashMap`s
  and only occasionally backs them up to the resilient
  [`IMap`](/javadoc/4.4/com/hazelcast/map/IMap.html)
  storage.
- **Load Balancing.** Jet often reshapes batches to better match the
  capacity of each stage.

These techniques mean that it is impossible to reconstruct the state as
it was exactly at the time of a failure. Every stage is at a different
point in the processing of the stream, so the first challenge is just
making the backups represent a consistent point in the stream. You can
read more about Jet's techniques that deal with this in the
[Architecture](/docs/architecture/fault-tolerance) section.

When a job restarts, the state is restored from `IMap` backups which
happened at an earlier time, and then all the data that came after that
point must be replayed. To keep the *exactly-once* guarantee, all the
components of the system must behave as if the data was observed only
once.

## Requirements on Outside Resources

Upholding a processing guarantee requires support not just from Jet, but
from all the participants in the computation, including data sources,
sinks, and side-inputs. Exactly-once processing is a cross-cutting
concern that needs careful systems design.

Ideally, the data sources will be *replayable:* observing an item
doesn't consume it and you can ask for it again later, after restarting
the pipeline. Jet can also make do with *transactional* sources that
forget the items you consumed, but allow you to consume them inside a
transaction that you can roll back.

We have a symmetrical requirements on the data sink: ideally it is
*idempotent*, allowing duplicate submission of the same data item
without resulting in duplication, but Jet can also work with
*transactional* sinks.
