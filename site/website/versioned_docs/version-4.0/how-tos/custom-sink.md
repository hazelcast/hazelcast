---
title: Create a Sink
description: How to create a custom sinks for Jet.
id: version-4.0-custom-sink
original_id: custom-sink
---

In the [Custom Sources and Sinks](../api/sources-sinks.md#custom-sources-and-sinks)
section of our [Sources and Sinks](../api/sources-sinks.md) programming
guide we have seen some basic examples of user-defined sources and
sinks. Let us now examine more examples which cover some of the
trickier aspects of writing our own sinks.

## 1. Define Sink

Let's write a sink that functions like a **file logger**. You set it up
with a filename and it will write one line for each input it gets into
that file. The lines will be composed of a **timestamp** and then the
**`toString()`** form of whatever input object produced the line.
Here's a sample:

```text
1583309377078,SimpleEvent(timestamp=10:09:37.000, sequence=2900)
1583309377177,SimpleEvent(timestamp=10:09:37.100, sequence=2901)
1583309377277,SimpleEvent(timestamp=10:09:37.200, sequence=2902)
1583309377376,SimpleEvent(timestamp=10:09:37.300, sequence=2903)
```

The definition of such a sink could be like this:

```java
class Sinks {

    static Sink<Object> buildLogSink() {
        return SinkBuilder.sinkBuilder(
            "log-sink", pctx -> new PrintWriter("data." + pctx.globalProcessorIndex() + ".csv"))
            .receiveFn((writer, item) -> {
                writer.println(String.format("%d,%s", System.currentTimeMillis(), item.toString()));
                writer.flush();
            })
            .destroyFn(writer -> writer.close())
            .build();
    }

}
```

Using it in a pipeline happens just as with built-in sinks:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(10))
 .withoutTimestamps()
 .writeTo(Sinks.buildLogSink());
```

## 2. Add Batching

Our sink uses a `PrintWriter` which has internal buffering we could use
to make it more efficient. Jet allows us to make buffering a first-class
concern and deal with it explicitly by taking an optional `flushFn`
which it will call at regular intervals.

To apply this to our example we need to update our sink definition like
this:

```java
SinkBuilder.sinkBuilder(
    "log-sink", pctx -> new PrintWriter("data." + pctx.globalProcessorIndex() + ".csv"))
    .receiveFn((writer, item) -> {
        writer.println(String.format("%d,%s", System.currentTimeMillis(), item.toString()));
    })
    .flushFn(writer -> writer.flush())
    .destroyFn(writer -> writer.close())
    .build();
```

## 3. Increase Parallelism

Jet builds the sink to be distributed by default: each member of the Jet
cluster has a processor running it. You can configure how many parallel
processors there are on each member (the **local parallelism**) by
calling `SinkBuilder.preferredLocalParallelism()`. By default there will
be one processor per member.

The overall job output consists of the contents of all the files
written by all processor instances put together.

Let's increase the local parallelism from the default value of 1 to 2:

```java
SinkBuilder.sinkBuilder(
    "log-sink", pctx -> new PrintWriter("data." + pctx.globalProcessorIndex() + ".csv"))
    .receiveFn((writer, item) -> {
        writer.println(String.format("%d,%s", System.currentTimeMillis(), item.toString()));
    })
    .flushFn(writer -> writer.flush())
    .destroyFn(writer -> writer.close())
    .preferredLocalParallelism(2)
    .build();
```

The behavioral change we can notice now is that there will be two output
files, `data.0.csv` and `data.1.csv`, each containing half of the output
data.

> Note: we could add a second member to the Jet cluster now. At that
> point we would have two members, both with local parallelism of 2.
> There would be 4 output files. You would notice however that all
> the data is in the files written by the processors of a single Jet
> member.
>
> The other members don't get any data, because on one hand our pipeline
> doesn't contain any operation that would generate distributed edges
> (ones that carry data from one member to another) and on the other
> hand the test source we have used only creates one instance globally,
> regardless of the number of members we have in the cluster. The member
> containing the test source instance will process all the data in this
> case. Real sources don't usually have this limitation.

## 4. Make it Fault Tolerant

Sinks built via `SinkBuilder` don’t participate in the fault tolerance
protocol. You can’t preserve any internal state if a job fails and gets
restarted. In a job with snapshotting enabled your sink will still
receive every item at least once. If you ensure that after the `flushFn`
is called all the previous items are persistently stored, your sink
provides an at-least-once guarantee. If you don't (like our first
example without the flushFn), your sink can also miss items. If the
system you’re storing the data into is idempotent (i.e. writing the same
thing multiple times has the exact same effect as writing it a single
time - obviously not the case with our example), then your sink will
have an exactly-once guarantee.
