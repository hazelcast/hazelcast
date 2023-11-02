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
- Processor pruning - eliminate redundant stage processors creation.
- Scan partition pruning - extract partition condition and select only required partition to be read
  by scanning processor meta suppliers (`SpecificPartitionsImapReaderPms` which spawns `ReadMapOrCacheP`).

## Goals

The goals of that initiative is corresponding with items enumerated in 'Terminology' section:

- deploy a job only on members which contain required partitions,
- limit number of partitions involved in IMap scans.

Non-goals are:

- support any kind of pruning for streaming jobs. It may be considered to do later, but now it is not a case.
- special support migration-tolerance for member and processor pruning. If the migration happens when the job is starting,
  it will be running suboptimally, because it may fetch data from other members - same behavior as we have currently.
- support partitioned index scan. Index scan is not supported in pure Jet, only SQL has dedicated processor for that.

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

There are three different approaches were considered, which differ from each other
in the stage that we understand that some member may be pruned from job execution.
The _generic_ approach tries to define the default behavior and applicable to DAGs
from any source (SQL/Jet), whereas the _SQL-oriented_ approach focuses on hinting
`ExecutionPlanBuilder` with additional meta-information from `JobConfig` and DAG
constructed by SQL optimizer. The third, _mixed_ approach suggests to use the
_SQL-oriented_ approach idea, but also analyze DAG to seek additional meta-information
to unlock support of operations that requires partitioning edges, like joins or aggregations.

##### Generic approach

The core idea here is that `ExecutionPlanBuilder` would perform a detailed analysis
of the received DAG and possibly try to optimize it before creating execution plans.
The only requirement to prune a member is that source(s) have defined partitioning attribute strategy.

This approach generifies member pruning for both SQL and Pipeline API, for any connector. However,
it brings high complexity in problem analysis, long development time and big list of corner cases,
which are discussed in the chapter below.

##### SQL-oriented approach

Unlike the way described above, we can think that member pruning would be beneficial
mostly for small jobs. The overwhelming majority of such jobs are generated by SQL,
and our team effort is to replace Predicate API, where `PartitionPredicate` is available
for partitioned data querying. For that, we will use SQL optimization phase also to determine
partitions and for all relations. Then, we extract required partitions set to run to `JobConfig`
baked by `__sql.requiredPartitions` argument. This indicates that `ExecutionPlanBuilder` will
try to choose only the required members.

##### Mixed approach

In addition to SQL-oriented approach, described above, we can attach the DAG analysis in
`ExecutionPlanBuilder` during required members calculation to quickly determine required members to run the job.
We are interested on it for two cases:

- support `allToOne` edge : all edges are directed to one member and partitioned by single key.
  These partitions and members (if multiple) are also a subjects to be included into the DAG result.
  Main motivation here is that most of the queries highly likely will have `allToOne` to forward
  scanned data to the coordinator, and we want to calculate it in proper place to include the coordinator
  and include appropriate partition in the job.

- general `partitioned` edge support to unlock support of operations like aggregations. DAG analysis result
  will include boolean flag, which determines if we need preserve all partitions in the cluster.

Based on described above, the result of DAG analysis may influence on partitions assignment: if DAG has
`distributed-partitioned` edge, non-used partitions will be assigned to all required members to run the job.
The reader may think that it would make performance worse, but mixed member pruning is coupled with
scan processors partition pruning, which closes this potential performance gap for `partitioned` edge case.

Overall, the algorithm looks like:

1. If we don't receive required partitions, just assign partitions to all members in cluster.
2. If we have received, perform DAG analysis.
3. Find all members that are owners of partitions with data required for the job (`dataPartitions`).
4. Add members that have explicit routing (`Edge.distributeTo`) if not yet added. Members found after
   this step are all members that are needed to execute the job (`required members`).
5. Assign additional partitions, which do not store data but are needed for (mainly routing) reasons to required members.

### Scan partition pruning

Key principle of scan partition pruning is that specialized processor meta supplier which will spawn scan processors
should be able to self-sufficiently calculate exact set of partitions to scan. To achieve it, we designed a convenient
API on a SQL side to be able to pass a minimum required information to PMS constructor.

#### IMap-specific prunable scan processor meta supplier design and implementation details

`SpecificPartitionsImapReaderPms` is a new meta supplier to perform IMap scans. It was designed with ability
to self-sufficiently calculate exact set of partitions to scan and have a possibility not to do any partitions'
calculation, if partitioning strategy is not available for the given IMap.

In case of partition prunability: when partition assignment is available, PMS should calculate an available
partition set by evaluating accepted filtering expressions. During `ProcessorSupplier` spawn process PMS computes
precise partitions subset per member (stored in `ProcessorSupplier`), and then each `ProcessorSupplier` spawns
`ReadMapOrCacheP` with only required partitions to scan.

Scan partition pruning will work in cases when member pruning is not possible. E.g, we have UNION for two tables,
and only one scan is prunable. Member pruning is not applicable here, but we can apply scan partition pruning
locally for prunable scan.

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

##### Intra-member && Inter-member processor pruning

We decided NOT to support this kind of processor pruning, because it

- is relatively ineffective since most DAGs with broadcast edges were created with algorithm correctness in mind,
- is hard to implement for most use cases, and
- doesn't fit the goal of general effort.

Also, its worth mentioning, that `DagNodeUtil` effectively implements inter-member processor pruning,
but it doesn't rely on partition pruning meta-information.

## Final scope

Firstly, we decided to implement only SQL-oriented approach, but after a couple of iterations,
we switched to the **Mixed** approach for member pruning and scan processor partition pruning.

Processor pruning was considered as non-universal, complex and was **rejected**.
