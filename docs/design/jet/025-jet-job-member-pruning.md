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

Before Hazelcast Platform 5.4, Jet job was always deployed to all data members (specifically - all not-lite members). 
If some DAG vertex isn’t using all members, it creates no-op processors on the rest, but Jet still creates queues 
to/from those vertices and starts the processors, even though it completes immediately and the queues are closed with a `DONE_ITEM`. 
If some member isn’t used at all, the DAG is still deployed to it, and the master has to send it to it and wait for 
the completion. Even though the processors are no-op, or have no data to process, it’s an unnecessary overhead, 
which becomes noticeable in very small batch jobs.

## Terminology

- Member pruning - prevention of cluster members without requested data on board to be involved in job execution;
- Processor pruning - elimination of redundant (it can work only with input) stage processor creation;
- (IMap) Partition pruning - extract partition condition and assigning only required partition to read by `ReadMapOrCacheP`;

## Goals

The goals of that initiative is corresponding with items enumerated in 'Terminology' section : 

- deploy a job only on members with required partitions.
- prune processors which are not required for job execution - e.g, their input processor doesn't produce data and 
the processor itself can work only with input.
- limit count partitions involved in IMap scan.

Non-goals are :
- support any kind of pruning for streaming jobs. It may be considered to do later, but now it is not a case.
- support migration-tolerance for member and processor pruning.

## Technical Design

Let's split member pruning, processor pruning and partition pruning as separate stages, which can complement each other, 
but on their own, each kind of pruning is still a good optimization.

### Member pruning

Member pruning by its own, may have pretty simple solution. To implement it, we need to support that solution 
in `ExecutionPlanBuilder` (execution plan creation phase).  Previous `ProcessorMetaSupplier#get` contract was a reason 
for strict assumptions in execution plan creation algorithm, but that has changed.

#### Use cases for member pruning

**Important note**: by default, the basic unit on example pictures will be DAG vertices, but sometimes we will 
use processors as basic unit instead. We will explicitly declare it.

##### Member pruning for simple map scan

Prune all members except Member 1 for scanning job. We want to highlight this simple case, because we assume that 
the bigger part of the submitted queries looks like `SELECT * FROM map WHERE ...`. So, if you may hint the optimizer
with partitionKey, the submitted job would be even local.

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
and final destination point `Aggregate`. Moreover, it is a good example for further processor pruning optimization :

#### Solution design details

To support this behaviour, we need to extend the API. One option is add a method to PMS:
```java
interface ProcessorMetaSupplier extends IdentifiedDataSerializable {
    // ...
    default boolean doesWorkWithoutInput() {
        return true;
    }
    // ...
}
```
The default return value is `true` for backwards compatibility. This means that all current and new processors that
do no work without an input will have to override this method.

Also,  propose to change the contract of `ProcessorMetaSupplier#get` return function so that it will be
allowed to return `null` for addresses for which it does not want to deploy processors. But this will not be enough
to completely eliminate a member from the execution.

Next, we need to determine exact scope of the change : there are two main approaches were considered, 
to shorten their meaning we mark them as 'heavyweight' and 'lightweight'. The difference between these two approaches 
is an exact stage when we understand that some member may be pruned from job execution: 'heavyweight' approach tries 
to be more generic and define it for any accept DAG, when 'lightweight' approach focus on hinting `ExecutionPlanBuilder`
with additional meta-information from DAG and JobConfig constructed by SQL optimizer.

##### Heavyweight approach

The core idea here is that  `ExecutionPlanBuilder` would have detailed analysis of received DAG before execution plan
creation and then, possibly, trying to optimize DAG and only then apply member pruning based. The requirements
to prune member are :
- each vertex in the DAG may work without an input;
- DAG does not contain `distributed-broadcast` edges;

As a pros, using this approach generify member pruning usage for both SQL, Pipeline API; allow usage for any connectors.
As a cons, this approach brings high complexity in problem analysis, long development and big list of corner cases, which
are discussed in chapter below.

##### Lightweight approach

Unlike a way described above, we can think that member pruning would be beneficial mostly for small jobs.
The overwhelming majority of such jobs are generated by SQL, and our team effort is to replace Predicate API,
where `PartitionPredicate` is available for partitioned data querying.
For that, we can use SQL optimization phase also for determining partitions and for all relations: we extract information
about required partitions to run to JobConfig, and then mark DAG as 'ableToPruneMembers'. This mark is an indicator
for `ExecutionPlanBuilder` to choose code path with member pruning during execution plans creation. 
As a helpful API for SQL's `CreateDagVisitor`, we introduced new wrapper for both `ProcessorMetaSupplier` and 
`ProcessorSupplier` - `memberPruningProcessorMetaSupplier`, where `doesWorkWithoutInput` method returns `false`. This PMS
make easier and more precise choosing members to prune.

### Processor pruning

#### Use cases for processor pruning

##### Processor pruning for Scan -> Transform -> Aggregate

![Scan + Scan -> HashJoin](https://svgshare.com/i/tTM.svg)

On the picture above we can see optimized example from member pruning case. We can go further and just don't create 
processors which are not participating in data processing: 

![Scan + Scan -> HashJoin](https://svgshare.com/i/tUD.svg)

#### Solution design details

For processor pruning we would like to separate two kinds of pruning: **intra-member** processor pruning, which 
is applicable only for processor creation elimination inside one member, and **inter-member** processor pruning.

##### Intra-member processor pruning
[//]: # (REPHRASE SOMEHOW)
To control processor creation and parallelism within one member, we would like to use various processor suppliers
(`ProcessorMetaSupplier` or `ProcessorSupplier`). The correctness of this method will totally rely on how DAG constructor. 
Since Partition Pruning initiative was introduced to align PredicateAPI and SQL functionality and performance, we will
rely on SQL optimizer input and DAG construction phase in `CreateDagVisitor`. Here we describe Jet API to support processor
pruning : 
- new `lazyForceTotalParallelismOne` PMS which does not cache member address to prevent wrong usage of cached plan.
- new smart `` PMS wrapper which controls 

##### Inter-member processor pruning

After long discussions, we decided NOT support this kind of processor pruning, because it 
- relatively ineffective, because most of DAGs with broadcast edges were created with algorithm correctness in mind
- hard to implement solution which fits most use cases;
- doesn't fit the goal of general effort.

### Scan processor partition pruning

Partition pruning is a pretty simple optimization : we will extract partition key condition from during SQL opt phase,
pass it to specialized processor supplier which will spawn `ReadMapOrCacheP` with only required partitions to scan.

### Notes

### Acceptance Criteria


































