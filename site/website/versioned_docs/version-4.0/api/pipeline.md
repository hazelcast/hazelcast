---
title: Building Pipelines
description: How to build processing pipelines in Jet.
id: version-4.0-pipeline
original_id: pipeline
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

## Batch vs Stream

The API differentiates between batch (bounded) and stream (unbounded)
sources and this is reflected in the naming: there is a
[BatchStage](/javadoc/4.0/com/hazelcast/jet/pipeline/BatchStage.html)
and a
[StreamStage](/javadoc/4.0/com/hazelcast/jet/pipeline/StreamStage.html)
, each offering the operations appropriate to its kind.
Depending on the data source, your pipeline will end up starting with a
batch or streaming stage. A batch source still can be used to simulate
a streaming source using the `addTimestamps` method, which will
convert it into a `StreamStage`.

In this section we'll mostly use batch stages, for simplicity, but the
API of operations common to both kinds is identical. Jet internally
treats batches as a bounded stream. We'll explain later on how to apply
windowing, which is necessary to aggregate over unbounded streams.

## Multiple Inputs

Your pipeline can consist of multiple sources, each starting its own
pipeline branch, and you are allowed to mix both kinds of stages in the
same pipeline. You can merge the branches with joining transforms such
as [hash-join](stateless-transforms.md#hashjoin),
[co-group](stateful-transforms.md#co-group--join) or
[merge](stateless-transforms.md#merge)
.

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
