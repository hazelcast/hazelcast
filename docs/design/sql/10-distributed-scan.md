# Distributed Scan

## Overview

The Hazelcast Mustang is a distributed query engine. When an SQL string is submitted for execution, it is converted into a 
query plan, that contains the tree of relational operators, such as `scan`, `project` and `filter`. Leaf operators of the plan 
act as data sources for the parent operators. The `scan` operator iterates over the `IMap` data structure. This document 
explains how the distributed scan is performed.

In this document, we describe the main design points of the scan operators: access path selection, local execution semantics,
and reaction to cluster reconfiguration.

## 1 Background

The Hazelcast IMDG is a distributed in-memory key-value storage. Data is stored in distributed objects, such as an `IMap`.
The cluster has a predefined number of partitions, 271 by default. Each partition is stored on a single member, and may have 
zero, one or more backups on other members. Every distributed object is split across one or more partitions. Partitions may
migrate between members due to topology change events, such as member leave or join.

The `IMap` is a distributed map. It could be accessed either directly, or through a secondary index. The secondary index is 
a distributed data structure. Every member stores part of the index that is built for the local entries of the member. 

The goal of the Hazelcast Mustang engine is to pick the proper access method - direct scan or index scan, start execution of 
the scan on members, and collect the results. In the above sections we describe how it is implemented.   

## 2 Access Path Selection

There are two ways to scan the `IMap` - iterate over the record store directly, or use one of the secondary indexes. During the
planning stage the proper access method is chosen. 

If there are no indexes on the table, the direct scan is chosen and no further optimization is performed.    

If there are indexes on the table, we analyze the predicate stored in the table of the `TableScan` operator. The entry point
is `IndexResolver.createIndexScans`. 

First, we split the predicate into conjunctive normal form (CNF). For example, the predicate `a=1 AND b=2` is split into 
`a=1` and `b=2`, while the predicate `a=1 OR b=2` remains unchanged.

Second, for every sub-predicate we find those that could be used by some index. Assume that `col` is a simple column
expression, and `exp` is an expression that has only constants or parameters at leaves (i.e. it doesn't refer to other columns). 
We use the following rules:
1. `col = exp` can be used with `SORTED` and `HASH` indexes
1. `col [comparison] exp` can be used with `SORTED` indexes
1. `col IS NULL/TRUE/FALSE` can be treated as equality expression (with slightly different semantics for `NULL` values)
1. `col = exp1 OR col = exp2` can be treated as union of two equality predicates 

The result of this step is a map from column to candidate expressions that could be used with indexes. For example, 
`a=1 AND b>2 AND b<4` is returned as: 
```
a -> [=1] 
b -> [>2], [<4]
```

Next, we iterate over every index, and try to bind candidates to the index based on the index columns and index type.
General rules are:
1. `SORTED` index may use equality and comparison conditions, while `HASH` index may use only equality conditions
1. Only a continuous prefix of index columns can be bound. E.g. for the index `(a, b, c)` and the condition `a=1 AND c=2` we 
can bind only `a=1`, while for `a=1 AND b=2` we may bind both `a=1` and `b=2`
1. All expressions in the prefix except for the last one must be equality conditions. E.g. for the index `(a, b)` and the 
condition `a>1 AND b>2`, we can use only `a>1`   

The result of this step is a map from index to the filter that should be used. For example, for the expression 
`a=1 AND b>2 AND b<4`, and indexes on `(a, b)`, `(a, c)` and `(b)`, the result would be:
```
index(a, b) -> [a=1, b>2 AND b<4]
index(a, c) -> [a=1, NULL]
index(b) -> [b>2 AND b<4]
```

Once the index filter is built, we calculate the remainder filter, such that `indexFilter AND remainderFilter` is equivalent
to the original filter. 

For every proposed index, we create a `MapIndexScanPhysicalRel` operator that is added to the planner search space. 
Currently, we add all usable indexes to the search space. This might become a problem in future releases, when we have 
joins and multiple tables, because there will be too many alternatives to consider. The solution could be to not add certain
indexes to the search space based on some heuristics. 

The cost model works as follows:
1. Get the expected number of rows to scan (`SCANNED_ROWS`). For the direct scan it equals to the number of rows in the map. For 
the index scan this is `mapRowCount * selectivity(indexFilter)`.
1. Get the expected number of returned rows (`RETURNED_ROWS`), that depend on the selectivity of the original filter.
1. Assign a weight to the scanned rows, based on the access method (`SCAN_MULTIPLIER`). We assume that a direct scan is cheaper 
than an index scan, because the latter requires indirection. Also, we assume that a scan of the `HASH` index is cheaper that 
a scan of the `SORTED` index, because it requires less CPU for the lookup.
1. The final formula is `COST = SCANNED_ROWS * SCAN_MULTIPLIER + RETURNED_ROWS * PROJECTION_COST`. It ensures, that a direct
scan is picked when the filter has poor selectivity, and that an index scan is picked otherwise, giving a priority to `HASH`
index.

## 3 Local Execution

Scan operators consist of three parts:
1. Access path (direct scan, index scan)
1. Optional filter
1. Column list

The execution proceeds as follows. First, an iterator over the underlying data is opened (record stores, index). Then every 
returned record is evaluated against the optional filter. If record is not filtered out, it is converted to a `Row` instance
based on the column list. The `Row` is added to the internal batch. When the batch reaches a certain size, or there are no
more records, the batch of rows is returned to the parent operator. 

For the direct map scan (`MapScanExec`), the iterator scans all local partitions one by one. For the index scan 
(`MapIndexScanExec`), the iterator is opened against the index, based on the optional index condition. In both cases,
the full result set of the scan is never materialized, unlike the legacy predicate engine.

An entry might be changed concurrently during execution of the query. Since the engine doesn't have transactions, it is
possible that the updated entry will be observed zero, one or several times, due to entry relocation within the underlying
data structure. To solve this problem, we would need a transactional engine, but we do not have one. Note that the described 
behavior is possible in many databases, including transactional ones. Examples are: Redis [[1]], MongoDB [[2]], Microsoft 
SQL Server with `READ_COMMITTED` isolation (the default behavior) [[3]]. 

## 4 Cluster Reconfiguration

Cluster configuration may change during query execution: partitions may migrate between members, distributed objects may be 
created and destroyed. This section describes the behavior of scan operators in the case of concurrent cluster reconfiguration.

### 4.1 Partition Migration

If the query is executed concurrently with partition migration, it is possible that some partitions will be scanned several 
times, or missed completely. To avoid that we need a mechanics to ensure that the engine observed every partition exactly
once.  

When the plan is created, we create the so-called **partition map**, that maps members to partitions. The partition map is saved
in the plan. If the partition distribution changes due to member join/leave, the plan is invalidated.  

When the query is about to start, we explicitly assign partitions to every participant. For example:
```
member1 -> [1, 2]
member2 -> [3, 4]
```  

When the scan operation is started on the member, we first check whether it contains expected partitions. If the expected and 
actually owned partitions do not match, an exception is thrown. For example, if the member is expected to have partitions 
`[1, 2]`, then:
```
[1] - error, because we may miss the partition [2]
[1, 2] - ok
[1, 2, 3] - error, because we may return duplicates for the partition [3]
```

The check before the query start is not sufficient to guarantee the correctness of the results. The partition may migrate to the 
member during execution of the query, leading to duplicates. Conversely, the partition may be removed from the member 
concurrently, leading to missed entries. To avoid inconsistent results we **re-check** the partition distribution before any
result is returned from the scan operator. 

Partition check logic depends on the operator type. For the direct map scan, we use the migration stamps 
(`MapService.validateMigrationStamp`). For the index scans, we use index partition stamps 
(`InternalIndex.validatePartitionStamp`).

A better solution would hide the partitioning problems from the user completely. This, however, is difficult to achieve 
in practice. To avoid duplicates and missed entries we would have to track which part of the operator input is processed, and 
re-schedule scans to other members at runtime. Therefore, the current design ensures that the user does not see an inconsistent
result, but forces the user to re-run the query manually if the result correctness cannot be guaranteed by the engine. In the
long term we would like to avoid, or at least minimize, the number of cases when the error is thrown.

### 4.2 Concurrent Schema Changes

It may happen, that the referenced object (`IMap`, index) that was present during the query planning, no longer exists on the 
local member. For example, because the map has been destroyed concurrently. Therefore, when the scan operation is about to
start, the existence of the required objects is checked. If the required object is not found, an exception is thrown, and the
plan is invalidated. 

The same check is also performed before the next result batch is returned to the parent operator, to avoid reading stale 
record stores of a destroyed map.

[1]: https://redis.io/commands/scan "Redis: SCAN command"
[2]: https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/#read-uncommitted-and-multiple-document-write "MongoDB: Read Isolation, Consistency, and Recency"
[3]: https://sqlperformance.com/2014/04/t-sql-queries/the-read-committed-isolation-level "Microsoft SQL Server: The Read Committed Isolation Level"
