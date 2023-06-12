# Jet job member pruning

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
If some DAG vertex isn’t using all members, it creates no-op processors on the rest, but Jet still creates queues 
to/from those vertices and starts the processors, even though it completes immediately and the queues are closed with a `DONE_ITEM`. 
If some member isn’t used at all, the DAG is still deployed to it, and the coordinator has to send `InitExecutionOperation` to it and wait for 
the completion. Even though the processors are no-op, or have no data to process, it’s an unnecessary overhead, 
which becomes noticeable in very small batch jobs and/or large clusters due to serial transmission.

## Terminology

- Member pruning - prevent cluster members without requested data on board from involving in job execution;
- Processor pruning - eliminate redundant stage processor creation;
- (IMap) Partition pruning - extract partition condition and assign only required partition to be read by `ReadMapOrCacheP`;

## Goals

The goals of that initiative is corresponding with items enumerated in 'Terminology' section : 

- deploy a job only on members which contain required partitions.
- prune processors which are not required for job execution - e.g, their input processor doesn't produce data and 
the processor itself can work only with input.
- limit number of partitions involved in IMap scan.

Non-goals are:

- support any kind of pruning for streaming jobs. It may be considered to do later, but now it is not a case.
- support migration-tolerance for member and processor pruning. 
- support local index scan.

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

To support this behaviour, we need to extend the API.

We propose to change the contract of `ProcessorMetaSupplier#get` return function so that it will be
allowed to return `null` for addresses for which it does not want to deploy processors. But this will not be enough
to completely eliminate a member from the execution.

Next, we need to determine exact scope of the change : there are two main approaches were considered, 
to shorten their meaning we mark them as 'generic' and 'SQL-oriented'. The difference between these two approaches 
is an exact stage when we understand that some member may be pruned from job execution: 'generic' approach tries 
to define the default behaviour and accept DAG from any source, when 'SQL-oriented' approach focuses on hinting 
`ExecutionPlanBuilder` with additional meta-information from JobConfig and DAG constructed by SQL optimizer. 

##### Generic approach

The core idea here is that `ExecutionPlanBuilder` would have detailed analysis of the  received DAG and possibly 
try to optimize it before creating execution plans. The requirements to prune a member are:
- each vertex in the DAG may work without an input;
- DAG does not contain `distributed-broadcast` edges;

This approach generifies member pruning for SQL, Pipeline API and any connector.
However, it brings high complexity in problem analysis, long development time
and big list of corner cases, which are discussed in the chapter below.

##### SQL-oriented approach

Unlike a way described above, we can think that member pruning would be beneficial mostly for small jobs.
The overwhelming majority of such jobs are generated by SQL, and our team effort is to replace Predicate API,
where `PartitionPredicate` is available for partitioned data querying.
For that, we can use SQL optimization phase also for determining partitions and for all relations: we extract information
about required partitions to run to `JobConfig`, and then mark DAG as 'ableToPruneMembers'. This mark is an indicator
for `ExecutionPlanBuilder` to choose code path with member pruning during execution plans creation.

### Processor pruning

For processor pruning we would like to separate two kinds of pruning: **intra-member** processor pruning, which
is applicable only for processor creation elimination inside one member, and **inter-member** processor pruning.

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

For 2-stage aggregation, honestly, we don't see any processor pruning possibilities. Potentially,
it may be done in another way: we can translate 2-staged into single-staged on SQL opt phase and
then eliminate member(s) via member pruning, namely scan and flatmap nodes.
In practice, processor logic is opaque to Jet and may have side effects which may break the job. 
This example illustrates **inter-member** processor pruning.

#### Solution design details

##### Intra-member processor pruning
To control processor creation and parallelism within one member, we would like to use various processor suppliers
(`ProcessorMetaSupplier` or `ProcessorSupplier`). The correctness of this method will totally rely on how DAG is constructed.
Since Partition Pruning initiative was introduced to align PredicateAPI and SQL functionality and performance, we will
rely on SQL optimizer input and DAG construction phase in `CreateDagVisitor`. 
This approach was tried, but it was **rejected** due to **small performance difference for increased code complexity**.
##### Inter-member processor pruning

After long discussions, we decided NOT to support this kind of processor pruning, because it
- is relatively ineffective since most DAGs with broadcast edges were created with algorithm correctness in mind,
- is hard to implement for most use cases, and
- doesn't fit the goal of general effort.

### Scan processor partition pruning

Partition pruning is a pretty simple optimization: we will extract partition key condition during SQL opt phase,
pass it to specialized processor supplier which will spawn `ReadMapOrCacheP` with only required partitions to scan.

## Final decision
After long discussions, we decided to implement **SQL-oriented** approach for member pruning and Scan processor partition pruning.

Processor pruning was considered as non-universal, complex and  **rejected**. As a side effect from this research, 
we decided to add  new `lazyForceTotalParallelismOne` PMS builder which does not cache member address to prevent 
wrong usage of cached plan.


### Acceptance Criteria