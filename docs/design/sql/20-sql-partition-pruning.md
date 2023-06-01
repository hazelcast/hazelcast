# Design document template

### Table of Contents

+ [Background](#background)
  - [Description](#description)
  - [Terminology](#terminology)
  - [Actors and Scenarios](#actors-and-scenarios)
+ [Functional Design](#functional-design)
  * [Summary of Functionality](#summary-of-functionality)
  * [Additional Functional Design Topics](#additional-functional-design-topics)
    + [Notes/Questions/Issues](#notesquestionsissues)
+ [User Interaction](#user-interaction)
  - [API design and/or Prototypes](#api-design-andor-prototypes)
+ [Client Related Changes](#client-related-changes)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)
+ [Other Artifacts](#other-artifacts)



|                                |                                                           |
|--------------------------------|-----------------------------------------------------------|
| Related Jira                   | [HZ-1605](https://hazelcast.atlassian.net/browse/HZ-1605) |
| Related Github issues          | _GH issue list_                                           |
| Document Status / Completeness | IN PROGRESS                                               |
| Requirement owner              | Sandeep Akhouri                                           |
| Developer(s)                   | Krzystof Jamróz                                           |
| Quality Engineer               | Isaac Sumner                                              |
| Support Engineer               | _Support Engineer_                                        |
| Technical Reviewers            | _Technical Reviewers_                                     |
| Simulator or Soak Test PR(s)   | _Link to Simulator or Soak test PR_                       |

### Background
#### Description

In practice many queries for large data sets do not need to scan all data.
Very often data is partitioned and only limited subset of partitions needs to accessed during the query.
Also some other operations like joins or aggregations can benefit from the knowledge about data partitioning.
Using such knowledge makes it possible to run a query faster and using less resources (network, IO).
This will make SQL queries eligible for partition pruning comparable in performance to 
Predicate API queries using `PartitionPredicate`.

Knowing which partitions are needed allows also to eliminate some members completely from the query execution.
Such members do not have any data related to the query.




#### Terminology

| Term             | Definition                                       |
|------------------|--------------------------------------------------|
| item, entry, row | Single element of data which is treated as whole |
|                  |                                                  |


*Definition: Partitioning column*

Partitioning column is a column/attribute that impacts partition to which given item is assigned.

*Definition: Partitioning key*

Partitioning key is a minimum (in terms of inclusion) set of partitioning columns that are sufficient
to determine to which partition given item belongs.

*Definition: Set of partitioning keys*

Set of partitioning keys is defined as a set containing:
- partitioning key defined by means of `AttributePartitioningStrategy` (if any)
- `__key`

_Clarification:_
For IMap, partitioning columns may be attributes of IMap key. 
If IMap uses `AttributePartitioningStrategy`, partitioning columns are functionally dependent on entire `__key`. 
However for the sake of simplicity in the SQL optimizer we currently do not track functional dependencies
and assume that both `__key` and `AttributePartitioningStrategy` define a partitioning key.
Partition id calculation will take that into account and use a correct strategy for given IMap.

*Definition: Partition-complete expression*

Expression `E` is *partition-complete* when there exists partitioning key
`PRK = {partColumn1, ..., partColumnM}`
for which the expression can be transformed to form:
`E = partKeyExp1 OR partKeyExp2 OR ... OR partKeyExpN`
where `partKeyExpN` is in form 
(there must be sub-expression for each of partitioning columns forming `PRK`):
`partColumn1Expr AND partColumn1Expr AND ... AND partColumnMExpr AND residualFilter`
where `partColumnMExpr` is in form
`partColumnM <operator> <arguments>`
where `operator` is one of the following operators:
- `=`
- `SEARCH` with `Sarg`
- `BETWEEN` for integer types (TODO: if we want to support this in the first version, makes sense only for reasonably small range)
- TODO: `IS NULL`?

and operator `arguments` are:
- literal
- query parameter
- complex expression using only the above `arguments` and any deterministic functions
  (in particular references to columns are not allowed)

and `residualFilter` is any expression, in particular it can be `TRUE`.  `residualFilter` may reference any columns,
also the partitioning columns.

Clarifications:
- range partitioning (eg. `order_date between day1 and day1`) currently is not supported,
  only hash/equality based partitioning is supported.
- each `OR` branch in `E` might use different partitioning key. Currently, such case is not supported.
- `partCol1 = partCol2 AND partCol2 = constantX` is not partition-complete, but can be transformed to such form
  by inlining constant: `partCol1 = constantX AND partCol2 = constantX`. Such transformations are out-of-scope
  for this TDD, but may be performed now or in future by the SQL optimizer independently.

#### Goals

- Improve performance of queries using IMap with `AttributePartitioningStrategy` which use attributes defined in it
- Improve performance of queries using IMap `__key` which are not eligible to be converted to `IMapSelectPlan`
  and have to create a Jet job

#### Non-goals

- Support partition pruning for other sources than IMap (can be considered in the future)
- Complex expression transformations of filters to extract partitioning information
  (for complex cases users will have to write their predicates in supported form)
- Tracking of functional dependencies between attributes
- Building a `__key` from constituent attributes, eg. if `__key` has 2 attributes, we could convert
  `__key.a1=X and __key.a2=Y` to `__key=(X,Y)`. 
  TODO: maybe we already do that?

### Functional Design
#### Summary of Functionality

Provide a list of functions user(s) can perform.

#### Additional Functional Design Topics

Provide functional design details that are pertinent to this specific design spec. Use the h3 heading tag to distinguish topic titles.

##### Notes/Questions/Issues

- Document notes, questions, and issues identified with this functional design topic.
- List drawbacks - why should we *not* do this? If applicable, list mitigating factors that may make each drawback acceptable. Investigate the consequences of the proposed change onto other areas
  of Hazelcast. If other features are impacted, especially UX, list this impact as a reason not to do the change. If possible, also investigate and suggest mitigating actions that would reduce the impact. You can for example consider additional validation testing, additional documentation or doc changes, new user research, etc.
- What parts of the design do you expect to resolve through the design process before this gets merged?
- What parts of the design do you expect to resolve through the implementation of this feature before stabilization?
- What related issues do you consider out of scope for this document that could be addressed in the future independently of the solution that comes out of this change?
- How does this functionality impact/augment our Viridian Cloud offering? 
- What changes, if any, are needed in Viridian to explose this functionality? 

Use the ⚠️ or ❓icon to indicate an outstanding issue or question, and use the ✅ or ℹ️ icon to indicate a resolved issue or question.

### User Interaction
#### API design and/or Prototypes

Listing of associated prototypes (latest versions) and any API design samples. How do we teach this?

Explain the proposal as if it was already included in the project and you were teaching it to an end-user, or a Hazelcast team member in a different project area.

Consider the following writing prompts:
- Which new concepts have been introduced to end-users? Can you provide examples for each?
- How would end-users change their apps or thinking to use the change?
- Are there new error messages introduced? Can you provide examples?
- Are there new deprecation warnings? Can you provide examples?
- How are clusters affected that were created before this change? Are there migrations to consider?
- How can this be leveraged in Viridian Cloud? 

#### Client Related Changes
Please identify if any client code change is required. If so, please provide a list of client code changes.
The changes may include API changes, serialization changes or other client related code changes.

Please notify the APIs team if any change is documented in this section. 
The changes may need to be handled for non-Java clients as well.

### Technical Design

#### Assumptions

##### Equi-partitions

For IMap we support equality-based partitions which is inline with IMap being a hash table.
Range-partitioning (eg. for date ranges) will be not supported.

#### Partitioning metadata available to SQL optimizer

Calcite SQL optimizer need to know how IMap is partitioned.

List of columns comprising `AttributePartitioningStrategy` of the IMap will be available in `PartitionedMapTable`.
(note that in this case entire `__key` also determines to which partition the entry belongs,
but it must be evaluated using `AttributePartitioningStrategy` not general hash strategy).

TODO: we want to benefit from the pruning also for `__key` in case `AttributePartitioningStrategy`.

For IMaps without `AttributePartitioningStrategy` the de-facto partitioning column is `__key`,
which will be treated as a partitioning key.
Helper methods (`isPartitioned` - TODO: each IMap is partitioned, method returning partition column indexes etc.) will be added as needed - this is private API.

At current stage there are no plans to add this information to general `Table` (TBD: could be useful for other sources) 

TBD: if we need to differentiate `AttributePartitioningStrategy` from `__key`?

#### Information if RelNode is partitioned and how

For some/all RelNodes we will be using information about how its input(s) and output(s) (???) are partitioned:
- if the input is partitioned
- which fields (indexes in input/output row) define partitioning key
- expression that generates list of partitioning key values for each partitioning key.
  Note that the expression may reference query parameters (`RexDynamicParam`),
  use special operators (`SEARCH` and `Sarg`).

This information may be calculated eagerly when RelNode is created or on-demand (TBD).
TBD: Expression may be complicated in some cases - maybe in such cases we should forget about them?
TBD: is it easy to derive the information for output? may be complex, is not strictly necessary for simple cases, but may be useful for joins and aggregations

#### Partition information in EXPLAIN PLAN

Explain plan should contain 2 sets of predicates in scan operations:
- partition columns (if any), without expressions
- filters (expressions used for partition selection should be reported separately, but this may be implemented later)

```sql
select count(*), sum(amount), priority from orders WHERE customerId='C2' group by priority
```

Current plan:

```
CalcPhysicalRel(expr#0..2=[{inputs}], EXPR$0=[$t1], EXPR$1=[$t2], priority=[$t0])
  AggregateCombineByKeyPhysicalRel(group=[{0}], EXPR$0=[COUNT()], EXPR$1=[SUM($1)])
    AggregateAccumulateByKeyPhysicalRel(group=[{0}])
      FullScanPhysicalRel(table=[[hazelcast, public, orders[projects=[$7, $4], filter==($1, _UTF-16LE'C2')]]], discriminator=[0])
```

Desired plan:

TODO: partitioning info not only on scans?

```
CalcPhysicalRel(expr#0..2=[{inputs}], EXPR$0=[$t1], EXPR$1=[$t2], priority=[$t0])
  AggregateCombineByKeyPhysicalRel(group=[{0}], EXPR$0=[COUNT()], EXPR$1=[SUM($1)])
    AggregateAccumulateByKeyPhysicalRel(group=[{0}])
      FullScanPhysicalRel(table=[[hazelcast, public, orders[projects=[$7, $4], filter==($1, _UTF-16LE'C2')]]], partitionedBy=[$1], discriminator=[0])
```


#### Rel node rules

Below rules apply only to rel nodes operating on IMaps. 
For other connectors similar rules may be defined in the future.

##### ValuesPhysicalRel

Input: N/A
Output: Not partitioned.

Should be executed only members that are needed because of other considerations.

##### InsertPhysicalRel

Input: 
- partitioned in the same way as target IMap
- partition key expressions determined by the input Rel. In some cases (eg. `ValuesPhysicalRel`) it may be possible
  to statically determine to which partitions inserts will be needed.

Output: N/A

Can drive partitioning of `ValuesPhysicalRel` which does not care how it is partitioned.

##### SinkPhysicalRel

Same as `InsertPhysicalRel`. We assume that `__key` columns cannot be updated, so the entry never changes partition to which it belongs.

##### UpdatePhysicalRel and DeletePhysicalRel

Same as `SinkPhysicalRel`.

##### FullScanPhysicalRel

Input: N/A
Output:

Full scan is eligible for partition pruning if the predicate is *Complete partitioning expression*.

Full scan should be executed only on members that contain needed partitions
and access only those partitions.

### Testing Criteria

#### Execution plan tests

Unit tests will be implemented ensuring that partition pruning generates expected
information about members and partitions needed for query execution.

#### Performance
Performance will compared for the same cluster topology (in particular with more that 1 member, ideally 3-5),
same IMap with the same data and data layout (ie. the same partitioning strategy).
The same queries will be issued with and without partition pruning optimization. (TODO: feature flag? hint?)
Throughput and latency will be compared.

TDB: is it possible to measure impact for query optimization time?


#### Soak tests

Soak tests for SQL queries should include some test cases with queries eligible for partition pruning
to test the stability of them (eg. in presence of concurrent partition migrations).

### Other Artifacts

Links to additional artifacts go here.
