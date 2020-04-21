---
title: Directed Acyclic Graph (DAG)
description: How streaming computation is modelled as a Direct Acyclic Graph (DAG) in Jet.
---

Hazelcast Jet models computation as a network of tasks connected with
data pipes. The pipes are one-way: results of one task are the input of
the next task. Since the dataflow must not go in circles, the structure
of the network corresponds to the notion of a Directed Acyclic Graph
&ndash; DAG.

## The Word Count Task

Let's take one specific problem, the Word Count, and explain how to
model it as a DAG. In this task we analyze the input consisting of lines
of text and derive a histogram of word frequencies. Let's start from the
single-threaded Java code that solves the problem for a basic data
structure such as an `ArrayList`:

```java
List<String> lines = someExistingList();
Map<String, Long> counts = new HashMap<>();
for (String line : lines) {
    for (String word : line.toLowerCase().split("\\W+")) {
        if (!word.isEmpty()) {
            counts.merge(word, 1L, (count, one) -> count + one);
        }
    }
}
```

## Separate the Steps

Basically, this code does everything in one nested loop. This works for
the local `ArrayList`, very efficiently so, but in this form it is not
amenable to auto-parallelization. We need a *reactive* coding style that
uses lambda functions to specify what to do with the data once it's
there while the framework takes care of passing it to the lambdas. This
leads us to the Pipeline API expression:

```java
Pipeline p = Pipeline.create();
p.readFrom(textSource())
 .flatMap(e -> traverseArray(e.getValue().toLowerCase().split("\\W+")))
 .filter(word -> !word.isEmpty())
 .groupingKey(wholeItem())
 .aggregate(AggregateOperations.counting())
 .writeTo(someSink());
```

In this form we can clearly identify individual steps taken by the
computation:

1. read lines from the text source
2. flat-map lines into words
3. filter out empty words
4. group by word, aggregate by counting
5. write to the sink

## Each Step Becomes a Concurrent Task

Now Jet can set up several concurrent tasks, each receiving the data
from the previous task and emitting the results to the next one, and
apply our lambda functions as plug-ins:

```java
// Source task
for (String line : textSource.readLines()) {
    emit(line);
}
```

```java
// Flat-mapping task
for (String line : receive()) {
    for (String word : flatMapFn.apply(line)) {
        emit(word);
    }
}
```

```java
// Filtering task
for (String word : receive()) {
    if (filterFn.test(word)) {
        emit(word);
    }
}
```

```java
// Aggregating task
Map<String, Long> counts = new HashMap<>();
for (String word : receive()) {
    String key = keyFn.apply(word);
    counts.merge(key, 1L, (count, one) -> count + one);
}
// finally, when done receiving:
for (Entry<String, Long> keyAndCount : counts.entrySet()) {
    emit(keyAndCount);
}
```

```java
// Sink task
for (Entry<String, Long> e: receive()) {
    sink.write(e);
}
```

## Tasks are Connected Into a DAG

The tasks are connected into a cascade, forming the following DAG
(`flatMap` and `filter` steps get automatically fused into a single
task):

![Word Count DAG](assets/dag.svg)

With the computation in this shape, Jet can now easily parallelize it by
starting more than one parallel task for a given step. It can also
introduce a network connection between tasks, sending the data from one
cluster node to the other. This is the basic principle behind Jet's
auto-parallelization and distribution.

## Jet Replicates the DAG on Each Cluster Member

When you run a job on the Jet cluster, every cluster instantiates the
same DAG. Therefore every DAG vertex runs on every cluster member. Also,
every vertex expands to several parallel tasks, one for each CPU core by
default. That means an edge in the DAG represents many point-to-point
connections between the parallel tasks.

## Group-and-Aggregate Transform Needs Data Partitioning

When you split the stream by, for example, user ID and aggregate every
user's events independently, you should send all the events with the
same user ID to the same task, the one holding that user's state.
Otherwise all the tasks will end up with storage for all the IDs and no
task will have the full picture. The technique to achieve this
separation is *data partitioning*: Jet uses a function that maps any
user ID to an integer from a predefined range and then assigns the
integers to tasks:

![Data Partitioning](assets/dag-partitioning.svg)

This brings us to the following picture of the DAG instantiated on two
cluster members:

![Word Count DAG &mdash; exploded view](assets/dag-exploded.svg)

Note that the data can flow mostly within the same machine, except when
it reaches the partitioned edge. Jet additionally optimizes for
throughput by splitting the `aggregate` vertex into two, called
`accumulate` and `combine`:

![Two-Stage Aggregation](assets/dag-twostage-aggregation.svg)

Here the edge coming into `accumulate` is also partitioned, but only
locally: every cluster member has all the partitions, but the
aggregation results are only partial. Once the `accumulate` step has
seen all the items, it sends its partial result to `combine` which
combines the partial results from all cluster members. Since there is
much less data after aggregation than before it, this reduces the amount
of data exchanged between servers at the cost of using more RAM.

## Tasks Concurrency is Cooperative

Hazelcast Jet avoids starting a heavyweight system thread for each
concurrent task of the DAG. Instead it uses a *cooperative
multithreading* model. This has high-level implications as well: all the
lambdas you write in the Pipeline API must cooperate by not calling
blocking methods that may take unpredictably long to complete. If that
happens, all the tasklets scheduled on the same thread will be blocked
as well.

Since sometimes you can't avoid making blocking calls, Jet provides
dedicated support for such cases. You should use the `mapUsingService`
transform that allows you to declare your code as "non-cooperative". Jet
will adapt to this by running the code in a dedicated thread.

However, whenever you have a choice, you should go for non-blocking,
asynchronous calls and use `mapUsingServiceAsync`.
