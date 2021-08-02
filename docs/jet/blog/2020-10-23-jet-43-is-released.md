---
title: Jet 4.3 Released
author: Marko Topolnik
authorURL: https://twitter.com/mtopolnik
authorImageURL: https://pbs.twimg.com/profile_images/922726943614783488/Pb5DDGWF_400x400.jpg
---

Today we're releasing Hazelcast Jet 4.3, our fourth release in 2020!

We took part in Google Summer of Code that ended just a few weeks ago,
and this release already brings a productionized piece of work by our
student, [Mohamed Mandouh](https://github.com/MohamedMandouh):
distributed in-memory sorting. Mohamed's primary focus was research into
the feasibility of integrating RocksDB or a similar DB library as a
disk-based state backend for Jet's hash join, aggregation and sorting,
and we plan to continue with this work for some more time.

Here are the main improvements in this release:

## Benchmarking and Tuning for Low Latency

Continuing the story from the previous release, we [benchmarked and
fine-tuned Jet](/blog/2020/08/05/gc-tuning-for-jet) with a focus on
low-latency processing. Jet can now give you a 99.99th percentile
latency of less than 10 milliseconds at a pipeline throughput of 60M
items/second! Based on this work we significantly expanded the
Operations Guide section on [Garbage
Collection](/docs/operations/gc-concerns) with many new
latency-squashing tricks.

## BatchStage.sort()

As mentioned, this is the work coming out of this year's GSoC. You can
now sort the data coming out of a batch pipeline stage. For example,
this starts from an asceding sequence 0..9,999, sorts it in descending
order, and prints the result:

```java
var pipeline = Pipeline.create();
var integerSequence = TestSources.items(IntStream.range(0, 10_000)
        .boxed().toArray(Integer[]::new));
pipeline.readFrom(integerSequence)
        .sort(ComparatorEx.comparing(i -> -i))
        .writeTo(Sinks.logger());
try {
    Jet.bootstrappedInstance().newJob(pipeline).join();
} finally {
    Jet.shutdownAll();
}
```

Jet's current execution model allows reordering, e.g., when maximizing
parallel throughput in stateless transform stages, which means you may
easily lose the sort order. In the next release we'll add the ability to
set limits on these optimizations so that the ordering survives.

## TestSources.longStream()

Our community contributor [Guenter
Hesse](https://github.com/guenter-hesse) took the ad-hoc work we did for
our low latency GC benchmarks and productionized it to be included in
our library. If you want to benchmark Hazelcast Jet in a way that
doesn't depend on the specifics of an actual data source, you can use
this distributed event generator that produces a timestamped sequence of
`Long` numbers. You can then transform the sequence numbers to whichever
mock events you are using for the benchmark:

```java
StreamStage<String> trades = pipeline
        .readFrom(TestSources.longStream(1_000_000, 25))
        .withNativeTimestamps(0)
        .map(i -> String.format("Trade %09d", i));
```

This stage will generate a steady stream of a million events per second,
keeping the latency of emiting any given event at a minimum. If you run
it in a cluster, every cluster node will generate its share of the
events.

## Preserve Job State on Exception

Jet's default behavior (and so far the only choice) is to cancel and
dispose of a job that throws an exception from any part of the pipeline.
This is usually user code, but it could also be IO errors while
contacting outside services. We are now introducing an option that
applies to jobs with enabled fault tolerance: Jet can now keep the job
in a suspended state, with the latest snapshot attached to it. Once you
remove the cause of the exception, you can resume the job and it will
continue executing without data loss.

As a part of these improvements, we added a whole new section on [error
handling](/docs/api/error-handling) in the Programming Guide.

## Make Continuous Progress in Pipelines Based on Ingestion Time

Hazelcast Jet is primarily built to respect the original event
timestamps instead of just noting the time it received them. Time
advances in the pipeline when events with fresh timestamps arrive. This
system has an Achilles' heel for the case where the event stream is very
sparse: without events, time doesn't pass. When you have a partitioned
data source, each partition has its own event time, and Jet must
consolidate them into a single global event time. For this to work out
without losses, time advances according to the "slowest" partition, with
the lowest local event time. So all it takes is a single partition out
of potential hundreds or thousands, to experience very low traffic, and
your entire pipeline experiences stalls.

In this release we bring a partial solution to this general problem:
if you happen to work with a pipeline based on ingestion time instead of
event time, Jet can be certain that the time advances in any partition
with or without events. We use this to improve our watermark emission
logic and make progress regardless of actual events coming in. We expect
to invest more effort into heuristic approaches that will improve the
progress of event time-based pipelines as well.

## Full Release Notes

Hazelcast Jet 4.3 is based on IMDG version 4.0.3. Check out its Release
Notes [here](https://docs.hazelcast.org/docs/rn/index.html#4-0-3) and,
for the Enterprise Edition,
[here](https://docs.hazelcast.org/docs/ern/index.html#4-0-3).

Members of the open source community that appear in these release notes:

- @caioguedes
- @guenter-hesse
- @MohamedMandouh

Thank you for your valuable contributions!

### New Features

- [pipeline-api] [014] @MohamedMandouh implemented distributed sorting:
  `BatchStage.sort()` (#2469, #2544)
- [core] [012] Added `JobConfig.suspendOnFailure`: suspend a job on
  exception instead of cancelling it (#2411)
- [cdc] Improved the consistency of reconnect behaviour across CDC
  sources, new uniform API to configure the reconnect strategy (#2419)

### Enhancements

- [core] @guenter-hesse contributed a test source to benchmark Jet's
  throughput and latency (#2382)
- [core] [013] Improved watermark semantics that prevent low event rate
  from stalling an ingestion time-based pipeline (#2485, #2514)
- [cdc] Exposed the sequence number in the CDC `ChangeRecord` that
  orders the events (#2390)
- [core] Two new DAG edge types: `distributeToOne` (sending all data to
  one member) and `ordered` (maintaining the sort order) (#2394, #2469,
  #2544)
- [core] Disabled access to external XML entities when parsing XML
  config, this was a potential XXE attack vector (#2528)

### Fixes

- [core] Fixed error handling during job startup that could result in
  inconsistent job state (#2383)
- [core] Fixed an internal exception that leaked out of Observable
  (#2313, #2389)
- [core] Prevented Observable from processing in-flight items after
  cancellation (#2415, #2418)
- [cli] @caioguedes fixed an issue with `--targets` option in CLI where
  it would overwrite other settings (#2373, #2421)
- [metrics] Fixed a problem where an internally added DAG vertex would
  show up as a source instead of the actual source vertex (#2475, #2476)
- [core] Fixed a race that could cause `getJobStatus()` to throw an
  exception if called right after `newJob()` (#2481, #2484)
- [core] Fix a race between snapshotting and restarting (#2487, #2503)
- [core] Fixed a race where `getJobStatus()` would report `RUNNING` even
  though it was actually `COMPLETING`. (#2507)
- [core] Fixed an issue where a `DONE_ITEM` could get lost due to
  connection failure, preventing the job from completing (#2158, #2532)
- [core] Fixed a job failure related to the coordinator member failing
  (#2461, #2546)
- [core] Fixed a job failure related to a member reconnecting (#2542,
  #2547)
- [core] Improved robustness related to Jet's internal `IMap` operations
  (#2533, #2550)
- [cdc] Upgraded Jackson jr dep, solving a null handling issue (#2459)
- [cdc] Upgraded the Debezium dep, solving a Postgress issue resulting
  in data loss when snapshotting (#2406)
- [core] Fixed a bug where a non-keyed aggregating stage would produce
  no output when no input (#2560, #2567)

### Breaking Changes

- [pipeline-api] Breaking signature change to
  `Sources.streamFromProcessorWithWatermarks()`
- [pipeline-api] Deprecated `Pipeline.toDag()`, made `Pipeline` and all
  its components `Serializable`.
- [core-api] Breaking signature change to `StreamEventJournalP`, methods
  `streamRemoteMapSupplier()` and `streamRemoteCacheSupplier`

_If you enjoyed reading this post, check out Jet at
[GitHub](https://github.com/hazelcast/hazelcast-jet) and give us a
star!_
