# Plan Caching

## Overview

Optimization of a query may take considerable time. In many applications, the set of queries is fixed. Therefore, the result of 
query optimization could be cached and reused. This document explains the design of the plan cache in the Hazelcast Mustang SQL
engine.

In section 1 we discuss the high-level requirements to the plan cache. In section 2 we describe the design. 

## 1. Requirements

We assume that the result of query optimization is deterministic. That is, the same query plan is produced for the same set of 
inputs. The inputs of the query optimizer are:
1. Catalog (schemas, tables, indexes)
1. The original query: query string, current schema, and parameters 
1. Metadata

### 1.1 Catalog

A  catalog is a set of objects that may participate in query execution. The catalog is often referred to as "schema" in 
the literature. We use the term "catalog" to disambiguate from the logical object containers, which are also called "schemas".
 
The catalog has three types of objects. *Schema* is a logical container for other objects. *Table* is a relation backed
by some physical storage, such as an `IMap`. *Index* is an additional data structure of a table that speeds up the execution 
of queries. The catalog is used to resolve objects mentioned in the query and choose the proper access method. 

If the catalog is changed, the plan created earlier might become invalid. For example, if the table is dropped, the execution 
of the plan will produce an error. If a new index is added, the optimizer may pick a better access path.

The plan cache must be able to find and remove plans that have become invalid after changing the catalog. 

### 1.2 Query

The query consists of the query string, the current schema, and parameters. Each of them may influence the optimization result.

The current schema affects object resolution. For example, the query `SELECT * FROM map` may refer to `IMap` or `ReplicatedMap`
depending on the current schema (`partitioned` or `replicated`). 

Parameter values may alter statistics derivation and access path selection. For example, `SELECT ... FROM sales WHERE region=?`
may have different optimal plans for regions `EMEA` and `APAC`.  
 
The plan cache must use query content properly to ensure the correctness and efficiency of the query execution.  
 
### 1.3 Metadata
 
In the query optimization theory, metadata is external information that is used for optimization. Examples are 
statistics, column uniqueness, data distribution, etc.

In this document, we consider only partition distribution because this is the only metadata we use in our optimizer, 
that is not part of the catalog, and that could change across query runs.

Every plan is built for a specific partition distribution. That is, the distribution and participating members are saved 
in the plan.

The plan cache must be able to find and remove plans with obsolete partition distribution. 
 
## 2. Design

### 2.1 Plan Key

The plan key is a key used to locate the cache plan. It should be possible to derive the key from the query before the 
optimization phase. 

We use the following key:
```
PlanKey {
    List<List<String>> searchPaths;
    String sql;
}
```
`searchPaths` is the list of schemas that are used to resolve non-fully qualified objects during query parsing. Search paths
are created based on configured table resolvers, and the current schema. `sql` is a query string.   

For the same catalog, two queries with the same search paths, and the same query string will always resolve the same objects. 
On the contrary, the same query string may resolve different objects for different search paths, as shown in section 1.2. 
Therefore, search paths must be part of the key.  

### 2.2 Data Structure

We use `ConcurrentHashMap` to store cached plans. `PlanKey` is a key, the plan is a value.
 
### 2.3 Maximum Size and Plan Eviction

The maximum size of the cache is required to prevent out-of-memory if too many distinct queries are submitted.

When a plan is added to the map, the map size is checked. If the map size is greater than the maximum size, some plans are 
evicted. 

Eviction is synchronous because the asynchronous variant is prone to out-of-memory. We assume that for most workloads 
evictions should be rare. 

We use the LRU (least recently used) approach to find the plans to evict. Whenever a plan is accessed, its `lastUsed` field is 
updated with the current time. During the eviction, plans are sorted by their `lastUsed` values, and the least recently used
plans are removed.   

### 2.4 Reacting to Catalog and Partition Distribution Changes

If a catalog or partition distribution is changed, some plans must be invalidated. There are two different ways to achieve 
this: `push` and `pull`.

With the `push` approach, the plan cache is notified about a change, from the relevant component. E.g., if an index is created,
then the map service notifies the plan cache about the change. The advantage of this solution is that any change is reflected 
in the plan cache immediately. However, this approach increases coupling, because many components (map service, 
replicated map service, partition service) now have to be aware of the SQL subsystem. This approach also requires complex
synchronization between query optimizer, plan cache, and dependent components, to ensure that no stale plan is ever cached.

With the `pull` approach, the SQL subsystem queries other components periodically, collects the changes, and invalidates 
affected plans. The advantage of this approach is simplicity. No synchronization or changes to other components are needed. 
Invalid plans are guaranteed to be removed eventually. The downside is that invalid plans might be active for some time after
the change has occurred.

We choose the `pull` approach due to simplicity and sufficient guarantees. The background worker reconstructs the catalog 
periodically, and verifies that existing plans are compatible with the current catalog and partition distribution. 

To counter the problem with outdated plans, we add a special `invalidatePlan` flag to `QueryException`. If an invalid plan
is used, an exception with this flag will be thrown at some point. When the initiator member receives an exception with 
this flag, the plan is invalidated. 

Note that currently, users will have to re-execute the query in this case. In future versions, we will add a transparent
query retry, so that invalid plans will not be visible to users.    

### 2.5 No Use of Parameters

In other databases, parameters are used for plan caching, because the same queries with different parameters may have different 
optimal plans.

We do not use parameters at the moment, because we do not have statistics. We may change this decision in the future when 
statistics are available.
