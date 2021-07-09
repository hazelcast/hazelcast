---
title: 014 - BatchStage.sort()
description: Distributed sorting for batch workloads
---

*Since*: 4.3

Currently, Jet can sort the data only indirectly, through a special
aggregate operation, resulting in a single output item that is the whole
sorted list. This design implements a transform that affects just the
order and not the type of the stream items (`T` vs. `List<T>`), but only
for a batch workload. We gain the following methods:

```java
BatchStage<T> BatchStage.sort()
BatchStage<T> BatchStage.sort(Comparator<? super T>)
```

Example:

```java
Pipeline p = Pipeline.create();
BatchStage<String> strings = p.readFrom(stringSource);
BatchStage<String> sortedStrings = p.sort();
```

Distributed sorting happens in two phases:

1. Sort the data on every cluster node locally, using a single `SortP`
  processor per node
2. Receive the partially sorted data on a single node, using a new type
  of edge, `ordered(Comparator)`

`SortP` is a very simple processor, it just puts all the received data
into a `PriorityQueue` with a user-supplied `Comparator`. In the
completion phase it drains the priority queue into the output. This is
the entire implementation:

```java
public class SortP<T> extends AbstractProcessor {
    private final PriorityQueue<T> priorityQueue;
    private final Traverser<T> resultTraverser;

    public SortP(@Nullable Comparator<T> comparator) {
        this.priorityQueue = new PriorityQueue<>(comparator);
        this.resultTraverser = priorityQueue::poll;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        priorityQueue.add((T) item);
        return true;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(resultTraverser);
    }
}
```

The `ordered(Comparator)` edge ensures that it always receives the least
item available from all the incoming data streams (one from each node).
Even though this is trivially simple high-level logic, implementing the
monotonic reception with cooperative multithreading in mind is the most
complex part of this work. It is implemented inside
`ConcurrentInboundEdgeStream.OrderedDrain`.

## Limitations

Apart from the mentioned limitation that this is only for batch stages,
a big caveat is that Jet may reorder the data between stages through
parallel processing, removing the sort order. There is work in progress
that will prevent Jet from reordering the stream items where it would
break correctness.
