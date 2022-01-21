---
title: Building Pipelines
description: How to build processing pipelines in Jet.
---

The general shape of any data processing pipeline is `readFromSource ->
transform -> writeToSink` and the natural way to build it is from source
to sink. The `Pipeline` API follows this
pattern. For example,

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("the", "quick", "brown", "fox"))
 .map(item -> item.toUpperCase())
 .writeTo(Sinks.logger());
```

In each step, such as `readFrom` or `writeTo`, you create a pipeline
_stage_. The stage resulting from a `writeTo` operation is called a
_sink stage_ and you can't attach more stages to it. All others are
called _compute stages_ and expect you to attach further stages to them.

## StreamStage and BatchStage

A pipeline stage is one of two basic kinds: a stream stage or a batch
stage. The data passing through a stream stage never reaches the end,
while only a finite amount of data passes through a batch stage. This
split is visible in the Java types: there is a
[StreamStage](/javadoc/{jet-version}/com/hazelcast/jet/pipeline/StreamStage.html)
and a
[BatchStage](/javadoc/{jet-version}/com/hazelcast/jet/pipeline/BatchStage.html),
each offering the operations appropriate to its kind.

The kind of a stage is usually determined from its upstream stage,
ultimately reaching the source where you explicitly choose to use a
`StreamSource` or a `BatchSource`.

You can also use a batch source to simulate an unbounded stream by using
the `addTimestamps` transform.

In this section we'll often use batch stages in the code snippets for
simplicity, but the API of operations common to both kinds is identical.
Jet internally treats a batch as a bounded stream. We'll explain later
on how to apply windowing, which is necessary to aggregate over
unbounded streams.

### Add Timestamps to a Stream

The Pipeline API guides you to set up the timestamp policy right after
you obtain a source stage. `pipeline.readFrom(aStreamSource)` returns
a `SourceStreamStage` which offers just these methods:

- `withNativeTimestamps()`
  declares that the stream will use source's native timestamps. This
  typically refers to the timestamps that the external source system
  sets on each event.

- `withTimestamps(timestampFn)`
  provides a function to the source that determines the timestamp of
  each event.

- `withIngestionTimestamps()`
  declares that the source will assign the time of ingestion as the
  event timestamp. System-time semantics will be used.

- `withoutTimestamps()`
  declares that the source stage has no timestamps. Use this if you
  don't need them (i.e., your pipeline won't perform windowed
  aggregation or stateful mapping).

You may also have to start without timestamps, then perform a
transformation, and only then use `addTimestamps(timestampFn)` to
instruct Jet where to find them in the transformed events. Some examples
include an enrichment stage that retrieves the timestamps from a side
input or flat-mapping the stream to unpack a series of events from each
original item. If you do this, however, you will remove the watermarking
responsibility from the source.

#### Issue with Sparse Events

If the time is extracted from the events, time progresses only when
newer events arrive. If the events are sparse, the time will effectively
stop between two events. This causes high latency for time-sensitive
operations (such as window aggregation). The time is also tracked for
every source partition separately and if just one partition has sparse
events, time progress in the whole job is hindered.

To overcome this you can either ensure there's a consistent influx of
events in every partition, or you can use `withIngestionTimestamps()`
which doesn't have this issue because it's based on system clock on the
member machines.

#### Prefer Assigning Timestamps at the Source

In some source implementations, especially partitioned ones like Kafka,
there is a risk of high [event time skew](../concepts/event-time)
occurring across partitions as the Jet processor pulls the data from
them. This problem is especially pronounced when Jet recovers from a
failure and restarts your job. In this case Jet must catch up with all
the events that arrived since taking the last snapshot. It will receive
these events at the maximum system throughput and thus each partition
will have a lot of data each time Jet polls it. This means that the
interleaving of data from different partitions will become much more
coarse-grained and there will be sudden jumps in event time at the
points of transition from one partition to the next. The jumps can
easily exceed the configured `allowedLag` and cause Jet to drop whole
swaths of events as late.

In order to mitigate this issue, Jet has special logic in its
partitioned source implementations that keeps separate track of the
timestamps within each partition and knows how to reconcile the
temporary differences that occur between them.

This feature works only if you set the timestamping policy in the source
using `withTimestamps()` or `withNativeTimestamps()`.

## Event Reordering and How To Prevent It

By default Jet prefers parallel throughput over strict event ordering.
Many transforms aren't sensitive to the exact order of events. This
includes the stateless transforms, as well as aggregate operations.
There are also transforms, especially `mapStateful`, where it's much
easier to write the logic if you can rely on the strict order of events.

A common example of this is recognizing patterns in the event sequence
and other tasks commonly done in the discipline of Complex Event
Processing. Also, external services that a pipeline interacts with can
be stateful, and their state can also be order dependent.

For those cases, `Pipeline` has a property named `preserveOrder`. If you
enable it, Jet will keep the order of events with the same partitioning
key at the expense of less flexible balancing of parallel processing
tasks. You can enable it this way:

```java
Pipeline p = Pipeline.create();
p.setPreserveOrder(true);
```

Note that a given pipeline may still reorder events. This happens
whenever you change the partitioning key along a data path. For example,
if you receive data from a partitioned source like Kafka, but then use
a `groupingKey` which doesn't match the Kafka partitioning key, it
means you have changed the partitioning key and the original order is
lost.

There's a discussion of the underlying mechanisms of this feature in the
[Pipeline Execution Model
section](/docs/architecture/distributed-computing#introduction)

## Multiple Inputs

Your pipeline can consist of multiple sources, each starting its own
pipeline branch, and you are allowed to mix both kinds of stages in the
same pipeline. You can merge the branches with joining transforms such
as [hash-join](stateless-transforms.md#hashjoin),
[co-group](stateful-transforms.md#co-group--join) or
[merge](stateless-transforms.md#merge).

As an example, you can merge two stages into one by using the `merge`
operator:

```java
Pipeline p = Pipeline.create();

BatchSource<String> leftSource = TestSources.items("the", "quick", "brown", "fox");
BatchSource<String> rightSource = TestSources.items("jumps", "over", "the", "lazy", "dog");

BatchStage<String> left = p.readFrom(leftSource);
BatchStage<String> right = p.readFrom(rightSource);

left.merge(right)
    .writeTo(Sinks.logger());
```

## Multiple outputs

Symmetrically, you can fork the output of a stage and send it to more
than one destination:

```java
Pipeline p = Pipeline.create();
BatchStage<String> src = p.readFrom(TestSources.items("the", "quick", "brown", "fox"));
src.map(String::toUpperCase)
   .writeTo(Sinks.files("uppercase"));
src.map(String::toLowerCase)
   .writeTo(Sinks.files("lowercase"));
```

## Pipeline lifecycle

The pipeline itself is a reusable object which can be passed around and
submitted several times to the cluster. To execute a job, you need the
following steps:

1. Create an empty pipeline definition
1. Start with sources, add transforms and then finally write to a sink.
   A pipeline without any sinks is not valid.
1. Create or obtain a `JetInstance` (using either embedded instance,
   bootstrapped or a client)
1. Using `JetInstance.newJob(Pipeline)` submit it to the cluster
1. Wait for it complete (for batch jobs) using `Job.join()` or just let
   it run on the cluster indefinitely, for streaming jobs.

## Types of Transforms

Besides sources and sinks, Jet offers several transforms which can be used
to process data. We can divide these into roughly the following categories:

1. [Stateless transforms](stateless-transforms.md): These transforms do
 not have any notion of _state_ meaning that all items must be processed
 independently of any previous items. Examples:
 [map](stateless-transforms.md#map),
 [filter](stateless-transforms.md#filter),
 [flatMap](stateless-transforms.md#flatmap),
 [hashJoin](stateful-transforms.md#)
1. [Stateful transforms](stateful-transforms.md): These transforms
 accumulate data and the output depends on previously encountered items.
 Examples: [aggregate](stateful-transforms.md#aggregate),
 [rollingAggregate](stateful-transforms.md#rollingaggregate),
 [distinct](stateful-transforms.md#distinct),
 [window](stateful-transforms.md#window)

This distinction is important because any stateful computation requires
the state to be saved for fault-tolerance and this has big implications
in terms of operational design.
