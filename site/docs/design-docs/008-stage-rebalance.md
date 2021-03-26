---
title: 008 - Rebalance Data on a Pipeline Stage
description: Adds a stage.rebalance() method that results in a distributed DAG edge from this stage to the next one.
---

*Since*: 4.2

## Goal

Allow the user to request a distributed round-robin DAG edge between
two stages.

## Background

By default, Jet uses local DAG edges, which means that the data that
originated on a given cluster member stays within it. This has the
advantage of avoiding the latency of network hops and congesting the
links between members. The disadvantage is that the data may be
imbalanced across members, especially when using a non-distributed data
source. We are therefore introducing a Pipeline API method,
`stage.rebalance()`, that allows the user to decide where the data
should be distributed across all members.

There are two ways of balancing the data on an edge: using a
self-balancing round-robin scheme and using a partitioning key that
makes for each data item a fixed decision on its target processor. We
provide both methods.

## Semantics

```java
stage.rebalance().map(Person::getName)
```

Jet uses a round-robin distributed edge from `stage` to `map`.

```java
stage.rebalance(Person::getName).map(Person::getAge)
```

Jet uses a distributed edge partitioned by `Person::getName`.

```java
stage.map(Person::getName)
     .rebalance()
     .flatMap(s -> traverseArray(s.toCharArray()))
```

Jet doesn't fuse `map` and `flatMap`, uses a distributed round-robin
edge between them. The processing in `map` will execute locally and the
items passed to `flatMap` will be rebalanced.

```java
stage0 = ...
stage1 = src.rebalance();
stage0.merge(stage1);
```

For merging, Jet independently rebalances the stages as requested.

```java
stage.rebalance().aggregate(...)
or
stage.rebalance(Person::getName).aggregate(...)
```

Global aggregation results in a single value, which means the second
stage is non-parallelizable. Rebalancing the first stage makes sense as
it parallelizes the load of accumulating the value.

```java
stage.rebalance().groupingKey(Person:getAge).aggregate(...)
```

Keyed aggregation computes many independent values. By default, Jet uses
two-stage aggregation so that it accumulates the running value locally
and sends the partial results for distributed combining only after
having received all the data.

Applying rebalancing removes the first local stage and uses single-stage
aggregation. Jet partitions the edge with the grouping key.

```java
stage.rebalance(Person::getName).groupingKey(Person:getAge)
```

For all transforms except aggregation, rebalancing has no effect here
since `groupingKey()` by itself results in a distributed-partitioned
edge.

Rebalancing changes keyed aggregation to single-stage, so it has some
effect but still the rebalancing key is ignored. The grouping key must
be used for semantic correctness.

```java
stage0 = stage.groupingKey(Person:getAge)
stage1 = stage.rebalance().groupingKey(Person:getAge)

stage0.aggregate2(stage1, ...)
```

If any of the joined stages are rebalanced, Jet uses single-stage
aggregation.

```java
stage0 = p.readFrom(src0).rebalance();
enrichingStage1 = p.readFrom(src1);
joinedStage = stage0.hashJoin(enrichingStage1, joinMapEntries());
```

Hash join is just a stateless mapping transform of `stage0`, so this
uses a round-robin distributed edge from `stage0` to `joinedStage`.

```java
stage0 = p.readFrom(src0);
enrichingStage1 = p.readFrom(src1).rebalance();
enrichingStage2 = p.readFrom(src2);
stage0.hashJoin(enrichingStage1, joinMapEntries(), enrichingStage2, joinMapEntries())
```

Enriching stages in a hash join use a distributed-broadcast edge anyway.
Rebalancing has no effect.

```java
stage.rebalance().xStateful()
or
stage.rebalance().rollingAggregate()
```

Stateful transforms are non-parallelizable and rebalancing does nothing.
We should probably throw an error.

## Implementation

We introduce these fields to `AbstractTransform`:

```java
public abstract class AbstractTransform implements Transform {

    // ...

    private final boolean[] upstreamRebalancingFlags;
    private final FunctionEx<?, ?>[] upstreamPartitionKeyFns;

    // ...

    @Override
    public boolean shouldRebalanceInput(int ordinal) {
        return upstreamRebalancingFlags[ordinal];
    }

    @Override
    public FunctionEx<?, ?> partitionKeyFnForInput(int ordinal) {
        return upstreamPartitionKeyFns[ordinal];
    }

    // ...
}
```

We use them in the `Planner`:

```java
public class Planner {
    // ...
    public void addEdges(Transform transform, Vertex toVertex, ObjIntConsumer<Edge> configureEdgeFn) {
        int destOrdinal = 0;
        for (Transform fromTransform : transform.upstream()) {
            PlannerVertex fromPv = xform2vertex.get(fromTransform);
            Edge edge = from(fromPv.v, fromPv.nextAvailableOrdinal()).to(toVertex, destOrdinal);
            applyRebalancing(edge, transform);
            dag.edge(edge);
            configureEdgeFn.accept(edge, destOrdinal);
            destOrdinal++;
        }
    }

    public static void applyRebalancing(Edge edge, Transform toTransform) {
        int destOrdinal = edge.getDestOrdinal();
        if (!toTransform.shouldRebalanceInput(destOrdinal)) {
            return;
        }
        edge.distributed();
        FunctionEx<?, ?> keyFn = toTransform.partitionKeyFnForInput(destOrdinal);
        if (keyFn != null) {
            edge.partitioned(keyFn);
        }
    }
    // ...
}
```

This mostly eliminates the need to propagate the rebalancing concern
into concrete `Transform` implementations. The custom `configureEdgeFn`
can still override the edge configuration. The only concrete transform
that must apply it explicitly is `HashJoinTransform` because it doesn't
use `Planner.addEdges()`.

Data rebalancing affects the transform fusing logic: it interrupts the
chain of fusable stateless transforms. Therefore `Planner.findFusableChain`
needed rework:

```java
public class Planner {
    // ...
    private static List<Transform> findFusableChain(
            @Nonnull Transform transform,
            @Nonnull Map<Transform, List<Transform>> adjacencyMap
    ) {
        ArrayList<Transform> chain = new ArrayList<>();
        for (;;) {
            if (!(transform instanceof MapTransform || transform instanceof FlatMapTransform)) {
                break;
            }
            chain.add(transform);
            List<Transform> downstream = adjacencyMap.get(transform);
            if (downstream.size() != 1) {
                break;
            }
            Transform nextTransform = downstream.get(0);
            if (nextTransform.localParallelism() != transform.localParallelism()
                    || nextTransform.shouldRebalanceInput(0)
            ) {
                break;
            }
            transform = nextTransform;
        }
        return chain.size() > 1 ? chain : null;
    }
    // ...
}
```

This completes the fundamental changes done to the pipeline code. The
rest of the code changes in this TDD serve the concern of setting the
rebalancing fields when creating the transforms. This affects classes
such as `ComputeStageImplBase` and `HashJoinBuilder`.
