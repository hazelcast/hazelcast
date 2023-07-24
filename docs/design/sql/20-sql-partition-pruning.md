# SQL Optimization for Partition Pruning

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

### Technical Design

#### Assumptions

##### Equi-partitions

For IMap we support equality-based partitions which is inline with IMap being a hash table.
Range-partitioning (eg. for date ranges) will be not supported.

#### Partitioning metadata available to SQL optimizer

Calcite SQL optimizer need to know how IMap is partitioned.

List of columns comprising `AttributePartitioningStrategy` of the IMap will be available in `PartitionedMapTable`.
(note that `__key` is a special case that should be handled with either an extension to `AttributePartitioningStrategy`
or in a separate specialized Partitioning Strategy that will explicitly ignore case when whole key object implements 
`PartitionAware` interface).

#### Information if RelNode is partitioned and how

For some/all RelNodes we will be using information about how its input(s) are partitioned:
- if the input is partitioned
- which fields (indexes in input/output row) define partitioning key
- expression that generates list of partitioning key values for each partitioning key.
  Note that the expression may reference query parameters (`RexDynamicParam`),
  use special operators (`SEARCH` and `Sarg`).

This information may be calculated eagerly when RelNode is created or on-demand (TBD).

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

#### Filter Analysis and Transformation for Partition Pruning
Filter Analysis and Transformation refers to the process of transforming and analyzing input Filter for determining
whether filter inherently limits the query to a finite number of partitions, after which point parts of the filter
can be extracted and transformed into form that allows other SQL execution logic to product concrete partition IDs.

##### Notice
This chapter refers to the future functionality, the MVP for Member Pruning should only include support for 
basic filters with `AttributePartitioningStrategy` and no support for Aggregations, Joins, Unions and limited support
for conjunctive (series of expressions in a single `AND`) filters. This chapter describes proposed design of the 
full implementation of the Filter Analysis, however. 

##### Filter Analysis Terminology
- **Branch** - a result of branching condition (e.g. OR, IN, BETWEEN, >, < and other operators which may match multiple rows). 
E.g. (comp1 IN (1,2,3)) has three branches comp1 = 1, comp1 = 2 and comp1 = 3. 
- **Variant/Filtering Variant** - filter or a series of conditions (joined by AND or otherwise unambiguously 
producing single match) that produce a single match e.g. comp1 = 1 will produce one variant.
- **FA** - Filter Analysis, here and after colloquial name for the entire process of analyzing input filter for partition boundness
and extracting Variants from it. The process itself may not be executed in this exact order however (transformation
will be performed before analysis for ease of analysis).
- **Partition Boundess** - characteristic of SQL filters to limit underlying query to a finite number of partitions, 
  it's based on the assumption that a finite number of keys will produce a finite number of partitions. Therefore, 
  to determine whether a filter is bounded partitions-wise we must analyze it for the number 
  of discrete keys it will pass (filter in).

##### Goals and non-goals of Filter Analysis and Transformation

**Goals**
Transform input filter into partition-complete filter pairs
First step is to normalize into a series of disjunctions/conjunctions around key components (either __key or components 
extracted from attribute strategy config). (a BETWEEN 1 AND 2 AND b BETWEEN 3 AND 4 should become a cartesian product 
of inputs e.g. (A=1,B=1), (A=1,B=2), (A=2,B=1), (A=2,B=2) - note that A and B are positional in the produced tuples, 
based off what’s specified in the strategy. Alternative approach might be choosing Number ranges as the basis and
therefore using BETWEEN as the basis operator instead of EQUALS. 

**Non-goals:**
- "Unwrap" function calls. Functions like floor, to_lower, ceiling have open-ended input-sets that are hard to determine 
or impractical to iterate over e.g. floor(__key) = 1.0 has virtually infinite number of possible concrete __key values.
- Reduce overlapping/negating expressions e.g. a IN (1,2,3,4,5) AND a > 2 should automatically eliminate 1 as possible 
variant of a.

##### Role of Data Types in Filter Analysis
Data Types play a big role in partition-boundness:
- Integer number ranges are finite by nature (unless the upper/lower bound for a range is explicitly stated 
as Infinity), therefore any closed range will produce closed range of partitions. Bigger ranges however reduce 
the probability of producing anything less than a full partition table (by default we should most likely limit it 
to <271 - if more than 271 keys are affected by filter, it will effectively cover the entire cluster, unless the end user 
has set the number of partitions higher). 
- For Floating Point numbers partition-boundness analysis is possible, but most likely prone to bugs and 
errors - FP numbers are imprecise and 1.0 could as well end up being 1.0000000096 which while will have 
entirely different binary representation, given that there’s no special handling for FP numbers in partitioning logic,
1.000000096 might produce entirely different partition ID compared to 1.00000095. 
- For DateTime types analysis is similar to Integer types since internal representation should be in integers, 
except precise microsecond, albeit it likely requires more advanced analysis logic than Integer types.
- For String types analysis is possible for finite strings and possibly finite String patterns 
(e.g. regex “^tes\w{1,1}$”). Additionally more advanced regex (unbounded ones) can be analyzed if 
they’re present with some form of String size limit operator e.g. LEN(__key) < 9 AND __key LIKE “test[0-9]est” 
should produce 10 keys at most (e.g. “test0est”, “test1est” … “test9est”). 

##### Role of Operators in Filter Analysis
- AND - each AND instance may either produce a complete variant or start a variant. 
- OR - start of a branch that may in the end produce multiple variants if all sub-conditions are bounded or form a bounded condition together with a higher level condition.
- IN - similar to OR, effectively can be transformed into a series of ORs. 
- BETWEEN - same as IN, effectively a series of ORs merged into one operator. 
- \> (GreaterThan) - unbounded by itself, but may produce a bounded condition if joined with a matching LesserThan, 
but only for Integer Data Types. Even FP number representation issues aside, a range of > 1.0 AND < 2.0 
is technically infinite. 
- < (LesserThan) - same as GreaterThan, with which it can form a bounding condition
- = (Equals) - bounding condition for any type, except FP types due to the nature of FP numbers.
- LIKE - may be bounded if the underlying filter is bounded, analysis most likely requires complex logic. 

##### Possible general algorithm of Filter Analysis
1. Denormalize filters into a disjunction of conjunctions, e.g. from a = 1 AND b IN (1,2) into (a = 1 AND b =1) 
OR (a = 1 AND b = 2)
2. Analyze each conjunctive expression for Partition Boundness - if the filter limits every partition-mandatory 
component of the key (this can be either full key or some of its components used according to 
AttributePartitioningStrategy), then it’s considered a Partition-Bound filter. 
3. If all the conjunctive expressions are Partition Bounded - transform them into a List of 
RexInputRef/RexDynamicParam expressions joined with table and key/component name. 
4. Transform each resulting Tuple3<TableName,ColumnName,RexNode> into a series of PartitionIds and pass 
to the Jet in arguments. 

##### Possible general Filter Denormalization rules
- Any expression in the filter may produce from 0 to N variants depending on whether it contains key columns. 
- OR should form a disjunctive, a top level OR should be considered as a series of filters essentially.
- AND forms a single conjunctive group that will be expanded into 1+ group after denormalization. 
In that group each member should be traversed and for each member we should produce a number of Variants. 
Once that process is done, a list of Combinations should be created from each variant inside every member group.
E.g. for a filter a = 1 AND b IN (1,2), the processing should yield a[1] and b[1,2] as variants, therefore combinations
we can produce from them will be a1b1 and a1b2.
- Any nested OR or an OR-like condition, a series of variants (EQUALS based) should be produced e.g. b IN (1,2,3) 
should produce b = 1, b = 2, b = 3.
- Sublevel conditions should produce partial variants that can then be joined with higher level conditions. 
E.g. a = 1 AND ((b = 1) AND (c IN (1,2))) should produce c = 1, c = 2 at lowest level, which then produce 
combinations of (b = 1 AND c = 1) and (b = 1 AND c = 2), which will in turn be combined with a = 1. 
- Any AND-d conditions that do not involve partitioning key columns should be excluded from the resulting 
transformation. E.g. a = 1 AND b = 2 AND someCol = 3 - in this case someCol = 3 can be safely discarded 
(from Variants list) since it's irrelevant.
- If a key condition has another column reference - this sub-condition should be thrown away as invalid 
as well e.g. a = 1 AND b = someCol is an unbounded filter
- Any OR’d sub-condition should also invalidate the branch e.g. a = 1 AND (b = 1 OR someCol = 2) - 
in this case someCol invalidates b1 as a valid key filter. And since at the top level there is an analysis 
of completeness, (b = 1 OR someCol = 2) will produce zero sub-variants for b and therefore there will be 
no complete a-b variants at the top level too. 
- A possible transformation could be merging > and < operators at one given conjunctive  level (AND) into 
a OR-like condition e.g. b > 20 AND b < 23 could become b = 21 OR b = 22. This however limits the possibility 
of optimization for queries like b > 20 AND (b < 25 OR b > 21). 
- An alternative approach to handling > and < and other unbounded operators is to use ranges as the 
product of extraction instead of EQUALS conditions and merge corresponding ranges according to rules 
of the underlying conditions. For AND e.g. a > 20 AND a < 30 would be merging [20, inf) and (-inf, 30] into b = [20,30]. 

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
