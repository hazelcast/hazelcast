# Jet job partition pruning

|||
|---|---|
|Related Jira|[HZ-1605](https://hazelcast.atlassian.net/browse/HZ-1605)|
|Document Status / Completeness|IN PROGRESS|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|Isaac Sumner|
|Technical Reviewers|TBD|
|Version|5.4|

## Background

Before Hazelcast Platform 5.4, Jet job was always deployed to all data members, specifically all non-lite members.
If some DAG vertex is not using all members, it creates no-op processors on others and Jet still creates queues
to/from those vertices and starts the processors, even though it completes immediately and the queues are closed with a `DONE_ITEM`.
If some member is not used at all, the DAG is still deployed to it, and the coordinator has to send `InitExecutionOperation` to it and wait for
the completion. Even though the processors are no-op, or have no data to process, it is an unnecessary overhead,
which becomes noticeable in very small batch jobs and/or large clusters.

## Terminology

- Member pruning - prevent cluster members which to not own requested data from being involved in job execution
- Processor pruning - eliminate redundant stage processor creation
- Scan partition pruning - extract partition condition and select only required partition to be read by scanning processors (`ReadMapOrCacheP`, `MapIndexScanP`)

## Goals

The goals of that initiative is corresponding with items enumerated in 'Terminology' section:

- deploy a job only on members which contain required partitions,
- limit number of partitions involved in IMap scans.

Non-goals are:

- support any kind of pruning for streaming jobs. It may be considered to do later, but now it is not a case.
- special support migration-tolerance for member and processor pruning. If the migration happens when the job is starting,
  it will be running suboptimally, because it may fetch data from other members - same behavior as we have currently.
- support local index scan. Index scan is not supported in pure Jet, only SQL has dedicated processor for that.

## Technical Design

Let's split member pruning, processor pruning and partition pruning as separate stages, which can complement each other,
but on their own, each kind of pruning is still a good optimization.

### Member pruning

Member pruning by its own, may have pretty simple solution. To implement it, we need to support that solution
in `ExecutionPlanBuilder` (execution plan creation phase). Previous `ProcessorMetaSupplier#get` contract was a reason
for strict assumptions in execution plan creation algorithm, but that has changed.

#### Use cases for member pruning

**Important note**: by default, the basic unit on example pictures will be DAG vertices, but sometimes we will
use processors as basic unit instead. We will explicitly declare it.

##### Member pruning for simple map scan

Prune all members except Member 1 for scanning job. We want to highlight this simple case, because we assume that
the bigger part of the submitted queries looks like `SELECT * FROM map WHERE ...`. So, if you may hint the optimizer
with partitionKey, the submitted job could be even local.

```
- SCAN[map2, partitionKey=1]
```

##### Member pruning for Scan -> Hash Join in big cluster

Prune members without any connections, so, only Member 1 and Member K left in cluster.

```
- HASH JOIN
    - SCAN[map1, partitionKey=1]
    - SCAN[map2, partitionKey=K]
```

![Scan + Scan -> HashJoin](https://svgshare.com/i/tfk.svg)

##### Member pruning for Scan -> Transform -> Aggregate

```
- AGGREGATION
  - TRANSFORM
    - SCAN[map, partitionKey=K]
```

![Scan + Scan -> HashJoin](https://svgshare.com/i/tTL.svg)

On the picture above the basic units are processors, since the actual graph looks like `Scan -> Transform -> Aggregate`.
So, here we can eliminate whole Member 2, since according to partition key condition only Member 1 contains required data
and final destination point `Aggregate`. Moreover, it is a good example for further processor pruning optimization.

#### Solution design details

There are two different approaches were considered, which differ from each other
in the stage that we understand that some member may be pruned from job execution.
The _generic_ approach tries to define the default behavior and applicable to DAGs from any source (SQL/Jet),
whereas the _SQL-oriented_ approach focuses on hinting `ExecutionPlanBuilder` with
additional meta-information from `JobConfig` and DAG constructed by SQL optimizer.

##### Generic approach

The core idea here is that `ExecutionPlanBuilder` would perfrom a detailed analysis of the received DAG and possibly
try to optimize it before creating execution plans. The requirements to prune a member are:

- each vertex in the DAG may work without an input;
- DAG does not contain `distributed-broadcast` edges;

This approach generifies member pruning for SQL, Pipeline API and any connector.
However, it brings high complexity in problem analysis, long development time
and big list of corner cases, which are discussed in the chapter below.

##### SQL-oriented approach

Unlike the way described above, we can think that member pruning would be beneficial
mostly for small jobs. The overwhelming majority of such jobs are generated by SQL,
and our team effort is to replace Predicate API, where `PartitionPredicate` is available
for partitioned data querying. For that, we will use SQL optimization phase also to determine
partitions and for all relations. Then, we extract required partitions set to run to `JobConfig`
baked by `__sql.requiredPartitions` argument. This indicates that `ExecutionPlanBuilder` will
try to choose only the required members.

### Scan partition pruning

Partition pruning for IMap scan supposed to be a pretty simple optimization: we should extract 
the partition key condition(s) during SQL opt phase, pass it to a specialized processor meta supplier 
which will spawn `ReadMapOrCacheP` with only required partitions to scan. We designed this solution with 
an additional steps to improve solution architecture, which are described below.

#### Implementation details
First of all, for all prunable `FullScan`-s in resulting plan we want to have a **precise** partition set to scan. 
To make it possible, and also isolate the implementation for each specific connector, we move the computational process
to `SqlConnector`. We extended `fullScanReader` method in `SqlConnector` interface to accept extracted all 
partition pruning candidates as a parameter and calculate it in connector-specific way:
```
    @Override
    @Nonnull
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates, // <-- new parameter
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider)
```

The new parameter `partitionPruningCandidates` is a list of maps, where for each column present in the filter 
the column name maps to the extracted comparison expression. The connector-specific implementation should
filter predicates for columns where partitioning strategy is applicable, and check if partitioning strategy 
is applicable generally, and in case of success transform the input to the **list of inner list of expressions**. 
Each inner list of expressions contains comparison expressions, and may be **one-sized**, 
if the partitioning strategy key is **simple**, and **multi-sized**, if the partitioning strategy key is **composite**. 
If we have more than one prunable filter predicate, outer list will be multi-sized. 

**Currently, it is implemented only for IMap connector, where all expressions are available.**

For better imagination we prepared an example below.

#### Successful case example 
Let's assume we have an IMap `map` with composite key {comp1, comp2, comp3} and applied partitioning strategy for
`comp1` and `comp2`. Let's have the following synthetic query, where filter matches the partitioning strategy:
```
SELECT * FROM map WHERE __key.comp1 = 1 AND __key.comp2 = 2
```
IMap-specific `fullScanReader` receives the following list of maps as a parameter:
```
[{"__key.comp1" = Expression(`__key.comp1 = 1`)}, {"__key.comp2" = Expression(`__key.comp2 = 2`)}]
```

After the described computation above, `fullScanReader` implementation should return the following list of expressions:
```
[[Expression(`__key.comp1 = 2`], [Expression(`__key.comp1 = 2`]]
```

#### Not successful case example

For same `map` as above, let's have the following synthetic query, where filter does not match the partitioning strategy:
```
SELECT * FROM map WHERE __key.comp1 = 1 AND __key.comp3 = 3
```

IMap-specific `fullScanReader` receives the following list of maps as a parameter:
```
[{"__key.comp1" = Expression(`__key.comp1 = 1`)}, {"__key.comp2" = Expression(`__key.comp3 = 3)}]
```

but, instead of example below, it will return an empty list, because partition pruning is not applicable due to presence
of `__key.comp3` in the filter, which was not declared as part of partitioning strategy.

**Important note : simple `__key` is always participates in default partitioning strategy, since IMap is partitioned
by __key**.

#### IMap-specific prunable meta supplier implementation details

`SpecificPartitionsImapReaderPms` is a new meta supplier to perform IMap scans. It was designed with ability 
to self-sufficiently calculate exact set of partitions to scan, similarly to `lazyForceTotalParallelismOne` 
described below. As a parameter, it accepts an optional list of lists of expressions (see previous section), 
which triggers the code path for partition pruning. Also, `SpecificPartitionsImapReaderPms` has a possibility not to do
any partitions' calculation, if partitioning strategy is not available for the given IMap.

Regarding partition pruning case, the related calculation is performed by evaluating received list of expressions in 
`init` method, where partition assignment is available, and as a result, we receive available partition set. Later,
in `get` method we compute required partitions subset per member (stored in `ProcessorSupplier`), and then each
`ProcessorSupplier` spawns `ReadMapOrCacheP` with only required partitions to scan.

### `lazyForceTotalParallelismOne`

To complement the existing `forceTotalParallelismOne` with PMS with partition pruning support we decided
to add a new partition-aware `lazyForceTotalParallelismOne` PMS builder which is dynamically calculating 
members to reduce total parallelism to one, using provided partition key expressions. Also, it does not cache
calculated member address to prevent the wrong usage of cached plan.

## Rejected opportunities

### Processor pruning

We would like to separate processor pruning into two categories: **intra-member** and **inter-member**.

#### Use cases for processor pruning

##### Processor pruning for Scan -> Transform -> Aggregate

1. Single-staged aggregation.

![Scan + Scan -> HashJoin](https://svgshare.com/i/tTM.svg)

On the picture above we can see optimized example from member pruning case. We can go further and just don't create
processors which are not participating in data processing by tuning local parallelism parameter with
partition involvement knowledge:

![Scan + Scan -> HashJoin](https://svgshare.com/i/tUD.svg)

So, it's a good target for **intra-member** processor pruning.

2. Double-staged aggregation.

```
- COMBINE
  - ACCUMULATE
    - TRANSFORM
      - SCAN[map, partitionKey=K]
```

![Scan + Scan -> HashJoin](https://jet-start.sh/docs/assets/arch-dag-4.svg)

Two-stage aggregation might be a good target for **inter-member** processor pruning. However,
processor logic is opaque to Jet and changing it requires a lot of changes, which may jeopardize
the correctness of execution plans. Potentially, it may be done in another way: we can translate
two-staged into single-staged on SQL opt phase and then eliminate member(s) via member pruning,
namely scan and flatmap nodes.

#### Solution design details

##### Intra-member processor pruning

To control processor creation and parallelism within one member, we would like to use various processor suppliers
(`ProcessorMetaSupplier` or `ProcessorSupplier`). The correctness of this method will totally rely on how DAG is constructed.
Since Partition Pruning initiative was introduced to align PredicateAPI and SQL functionality and performance, we will
rely on SQL optimizer input and DAG construction phase in `CreateDagVisitor`.
This approach was tried, but it was **rejected** due to **small performance difference for increased code complexity**.

##### Inter-member processor pruning

We decided NOT to support this kind of processor pruning, because it

- is relatively ineffective since most DAGs with broadcast edges were created with algorithm correctness in mind,
- is hard to implement for most use cases, and
- doesn't fit the goal of general effort.

## Final scope

We decided to implement **SQL-oriented** approach for member pruning and Scan processor partition pruning.

Processor pruning was considered as non-universal, complex and  **rejected**.
