---
title: Cooperative Multithreading
description: Jet's efficient threading model.
id: version-4.2-execution-engine
original_id: execution-engine
---

Hazelcast Jet doesn't start a new thread for each concurrent task.
Instead it uses a design similar to
[coroutines](https://github.com/Kotlin/KEEP/blob/master/proposals/coroutines.md):
the execution of a task can be suspended purely on the Java level. The
underlying thread just goes on executing, returning control to the
framework code that manages many coroutines on a single worker thread.
We use this design in order to maximize CPU utilization. Two key factors
contribute to this:

- The overhead of context switching is much lower since the operating
  system’s thread scheduler is not involved.
- The worker thread stays on the same core for longer periods,
  preserving the CPU cache lines.

## Tasklet

The key component is the `Tasklet` interface:

```java
interface Tasklet {
    ProgressState call();
    ...
}
```

The execution engine repeatedly invokes `call()`, which is supposed to
return in no more than 1 millisecond. The `ProgressState` result is a
pair of booleans: `(madeProgress, isDone)`. The former is used for CPU
load control (to prevent a hot idle loop) and the latter signals the
completion of the task. As long as the task keeps reporting "not done",
Jet will call it again. This is a simplified loop that Jet runs:

```java
while (true) {
    boolean madeProgress = false;
    for (Iterator<Tasklet> it = tasklets.iterator(); it.hasNext();) {
        ProgressState ps = it.next().call();
        if (ps.isDone) {
            it.remove();
        }
        madeProgress |= ps.madeProgress;
    }
    if (!madeProgress) {
        backOff();
    }
}
```

In the default setup Jet runs the above loop on every CPU core.

## Processor

The non-blocking API contract spreads to the `Processor` which
implements the logic of a given DAG vertex:

```java
interface Processor {
    void process(int ordinal, Inbox inbox);
    ...
}
```

`ordinal` identifies the input edge and `inbox` contains a batch of
input data. The tasklet keeps calling this method until it has consumed
all the items from the inbox and then refills the inbox with more data
(possibly from a different input edge).

The processor emits the data to the `Outbox`:

```java
interface Outbox {
    boolean offer(int ordinal, @Nonnull Object item);
    ...
}
```

`offer()` is non-blocking, but will fail when the outbox is full,
returning `false`. The processor will react to this by returning from
its `process()` method, and then the tasklet returns from `call()`. The
processor must preserve its state of computation so that it can resume
where it left off the next time it's called.

## Traverser

In many cases the processor satisfies the non-blocking contract by
creating a lazy sequence from the input and attaching transformation
steps to it (akin to [Kotlin
sequences](https://kotlinlang.org/docs/reference/sequences.html)). Jet
defines the `Traverser<T>` type for this purpose, an iterator-like
object with just a single abstract method:

```java
interface Traverser<T> {
    T next();
    ...
}
```

This lightweight contract allows us to implement `Traverser` with just a
lambda expression. If you look at the source code of Jet processor,
you may encounter quite complex code inside `Traverser` transforms. A
good example is the
[`SlidingWindowP`](https://github.com/hazelcast/hazelcast-jet/blob/v4.2/hazelcast-jet-core/src/main/java/com/hazelcast/jet/impl/processor/SlidingWindowP.java#L207)
processor.

At the
[`ProcessorTasklet`](https://github.com/hazelcast/hazelcast-jet/blob/v4.2/hazelcast-jet-core/src/main/java/com/hazelcast/jet/impl/execution/ProcessorTasklet.java#L259)
level we needed a full state machine implementation, basically
implementing the [CPS
transformation](https://github.com/Kotlin/KEEP/blob/master/proposals/coroutines.md#implementation-details)
by hand.
