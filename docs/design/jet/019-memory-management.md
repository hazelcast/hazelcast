---
title: 019 - Memory Management
description: Provide means to limit memory consumption of certain runtime operations
---

*Since*: 5.0

## Introduction

The memory management is used for the operations that accumulate a
(potentially large) number of records:

* sort
* group/aggregate
* join
* stateful transform
* distinct

Without means to control memory consumption, these operations could
lead to `OutOfMemoryError`s and destabilization of entire cluster.

## Possible Solutions

### 1. Fine-grained memory management

Control very precisely (with byte level granularity) how much memory
each operator uses at any given time. All member's operators share
a memory pool they acquire from and release to.

#### Pros

* accurate

#### Cons

* requires substantial effort to implement and might be tricky to get
  right
* might potentially affect performance

### 2. Coarse-grained memory management

Control number of records `Processor`s can hold on to. The record might
mean different things for different operations - i.e. for sorting it's
an individual item, for grouping it's a distinct key. The limit applies
to each `Processor` instance separately, hence the effective limit of
records accumulated by each cluster member is influenced by the
vertex's `localParallelism` and the number of jobs in the cluster.

#### Pros

* simple and easy to implement
* effect on performance is negligible

#### Cons

* not accurate
* might still lead to `OutOfMemoryError`s due to variable object sizes

## Design

We've chosen the second option since it's simple. We might reconsider
it if it turns out that current solution is not enough.

## Implementation

Allow configuring `maxProcessorAccumulatedRecords` for the member:

<!--DOCUSAURUS_CODE_TABS-->

<!--Java-->

```java
public class InstanceConfig {

      public void setMaxProcessorAccumulatedRecords(long maxProcessorAccumulatedRecords) {
        checkPositive(maxProcessorAccumulatedRecords, "maxProcessorAccumulatedRecords must be a positive number");
        this.maxProcessorAccumulatedRecords = maxProcessorAccumulatedRecords;
    }
}
```

<!--YAML-->

```yaml
  jet:
    instance:
      max-processor-accumulated-records: 1000000000
```

<!--XML-->

```xml
  <jet>
    <max-processor-accumulated-records>1000000000</max-processor-accumulated-records>
  </jet>
```

<!--END_DOCUSAURUS_CODE_TABS-->

as well as for the job:

```java
public class JobConfig implements IdentifiedDataSerializable {

    public JobConfig setMaxProcessorAccumulatedRecords(long maxProcessorAccumulatedRecords) {
        checkTrue(maxProcessorAccumulatedRecords > 0 || maxProcessorAccumulatedRecords == -1,
              "maxProcessorAccumulatedRecords must be a positive number or -1");
        this.maxProcessorAccumulatedRecords = maxProcessorAccumulatedRecords;
        return this;
    }
}
```

To not break backward compatibility the default value of
`maxProcessorAccumulatedRecords` for `JobConfig` is `-1` and for
`InstanceConfig` it is `Long.MAX_VALUE`. `JobConfig`'s value, if set,
has precedence over `InstanceConfig`'s one.

`maxProcessorAccumulatedRecords` is accessible for `Processor`s via
`ProcessorMetaSupplier.Context.maxProcessorAccumulatedRecords()`.

`Processor`s track number of accumulated records individually and throw
`com.hazelcast.jet.impl.memory.AccumulationLimitExceededException` if
limit is exceeded which fails the job.
