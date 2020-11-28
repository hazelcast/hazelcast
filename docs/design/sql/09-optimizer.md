# SQL Optimizer 

## Overview

When a query string is submitted for execution, it is first parsed, converted to a tree of relational operators, and then 
passed to the optimizer in order to find the best execution path.

In this document, we describe the design of the Hazelcast Mustang query optimizer that is based on Apache Calcite.

## 1 Theory

In this section we describe the theoretical aspects of query optimization that form the basis of the Hazelcast Mustang query 
optimizer.

### 1.1 Definitions

First, we define several entities from the relational algebra. The definitions are not necessarily precise from the 
relational algebra standpoint, but are sufficient for the purpose of this document. 

A **tuple** is an ordered collection of triplets `[name, type, value]`. A **relation** is an unordered collection of 
tuples. A relational **operator** is a function that takes arbitrary arguments (possibly, other operators) and produces a 
relation. 

A **plan** is a tree of relational operators. Two plans are said to be **equivalent** if they produce the same relation for 
all possible inputs. 

### 1.2 Cost

Typically, a query could be executed in different ways. For example, the query `SELECT * FROM table` could be executed either
through a direct table scan, or through a scan of an index on that table. 

The goal of the query optimizer is to find the **best** plan among one or more alternative equivalent plans. The definition of 
the **best** is context-dependent. Usually, the goal is to find the plan that consumes the least amount of system resources,
that could be CPU, memory, network. The optimization goal could be also defined in terms of performance characteristics, such 
as "find the plan with the minimal latency".

To formalize the optimization goal, every operator is assigned a **cost** - an abstract comparable value, that depends on the 
operator type, and operator arguments. The plan with the least cost is said to be the best plan for the given query.     

### 1.3 Properties

A **property** is an arbitrary value assigned to an operator, that is used during the optimization process. The query optimizer
may **enforce** a certain property on the operator. If the operator satisfies the required property, it is left unchanged. 
Otherwise, an **enforcer** operator is applied to the original operator to meet the optimizer requirements.

In the query optimization literature, the **collation** (aka sort order) is represented as a property. The **Sort** operator is 
a typical enforcer operator.

Consider the class `Person {id:Integer, name:String}` and an `IMap<Integer, Person>` with two entries `[{1, "John"}, {2, "Jane}]`.
Then the query below produces a relation with two tuples:
```sql
SELECT id, name FROM person ORDER BY id
``` 
```
Sort[$0 ASC]
  Project[$0=id, $1=name]
    Scan[person]
```
```
{id:INT:1, name:VARCHAR:"John"}
{id:INT:2, name:VARCHAR:"Jane"}
```

Since the relation is defined as an **unordered** collection of tuples, the plan of the query below is **equivalent** to the 
plan of the query above, even though the order of rows is different:  
```sql
SELECT id, name FROM person ORDER BY id DESC
``` 
```
Sort[$0 DESC]
  Project[$0=id, $1=name]
    Scan[person]
```
```
{id:INT:2, name:VARCHAR:"Jane"}
{id:INT:1, name:VARCHAR:"John"}
```

While these two plans are equivalent in the relational theory, they produce different results from the user standpoint. 
The difference comes from the different values of the collation property: the first query has collation `[a ASC]`, while
the second query has collation `[a DESC]`.

### 1.4 MEMO

During the optimization, quite a few alternative plans could be created (thousands, millions, etc). Therefore, it is important
to encode the search space efficiently to prevent out-of-memory conditions. The so-called **MEMO** data structure is often used
for this purpose. 

A **group** is a collection of equivalent operators. When a plan is submitted for optimization, its operators are copied and 
put into groups. During copying, operator inputs are replaced with groups. Then, the search for equivalent operators is 
performed. When an equivalent operator is found, it is put into the same equivalence group as the original operator, thus
eliminating the need to copy the whole operator tree.

Consider a simple query that performs a table scan:
```
SELECT name FROM person
```  
If there is an index on the table, then there are two access paths (table scan, index scan), and two equivalent plans. Without
the MEMO, we would have to create two separate plans:
```
Project[name]
  TableScan[person]
```   
```
Project[name]
  IndexScan[person, index]
```
With the MEMO, the plan is first copied into the search space:
```
G1: {Project[name]}
  G2: {TableScan[person]}
```
Then, when an alternative scan path is found, it is added to the group:
```
G1: {Project[name]}
  G2: {TableScan[person], IndexScan[person, index]}
```  

### 1.5 Logical and Physical Operators

In query optimization literature, there are typically two types of operators. **Logical operators** are operators that define 
data transformations. **Physical operators** are specific algorithms that implement particular transformations. Below are 
several examples of logical operators and their physical counterparts.

*Table 1: Logical and Physical Operators*

| Logical Operator | Physical Operator |
|---|---|
| `Scan` | `TableScan`, `IndexScan` |
| `Join` | `HashJoin`, `MergeJoin` |
| `Agg` | `HashAgg`, `StreamingAgg` |

The ultimate goal of the optimizer is to find the best physical plan for the initial logical plan.

### 1.6 Extensible Query Optimizers

Modern query optimizers are extensible. They have a core algorithm, and a set of user-provided interfaces:
- Operators
- Rules - code that create new operators based on some pattern and other conditions
- Metadata - additional information that is used by rules, such as cardinality, column uniqueness, etc

Now we discuss two concrete algorithms that are relevant to this document. 

#### 1.6.1 EXODUS

The EXODUS was a research project to design an extensible database system. As a part of this project, an extensible query 
optimizer was developed [[1]].

In the original paper, the logical and physical operators are named **operators** and **methods** respectively. 
The rules applied to logical operators are named **transformation rules**, while the rules applied to physical operators
are named **implementation rules**.

Initially, the optimizer accepts an operator tree and a set of transformation rules. For every rule, a pattern
matching is performed for the available operators. If a rule matches a part of the given operator tree, an
instance of the rule is placed into a priority queue called **OPEN**. This way the initial queue of rule instances
is formed.

Then the optimizer takes rule instances from the queue and applies them to the operator tree. The result of rule
execution is zero, one or more new logical operators. For every new logical operator, matching implementation rules
are executed, possibly producing new physical operators. Newly created operators are stored in a MEMO-like data 
structure called **MESH**. 

When a new operator is created, its cost is estimated. Since it may have better cost than previously known operators
of the same equivalence group, it is necessary to recalculate the costs of parent operators. This step is called **reanalyzing**,
and is performed by re-execution of the implementation rules on parents. In addition to this, if a new logical operator was 
created, there might be new possibilities for further transformations. Therefore, the transformation rule instances are 
scheduled for execution on parent operators (i.e. added to OPEN). This step is called **rematching**.
   
The algorithm proceeds until OPEN is empty.

The EXODUS optimizer doesn't have a strict order of rule execution. One may assign weights to rule instance to make them
fire earlier, but generally the search is not guided - every rule instance fires independently of others. As a result, the
same rule may fire multiple times during reanalyzing/rematching, which makes the engine inefficient.

Consider the following operator initial plan and two rules - one changes the join order, and the other attempts 
to remove the sort operator if the input is already sorted:
```
MEMO:
G1: [Sort]
G2:   [Join_AB]

OPEN (pending):
1. JOIN_COMMUTE[Join_AB]
2. SORT_REMOVE[Sort, Join_AB]
```
Once the `JOIN_COMMUTE` rule is fired, a new operator Join_BA operator is created, and a new rule instance is scheduled:
```
MEMO:
G1: [Sort]
G2:   [Join_AB, Join_BA]

OPEN (pending):
2. SORT_REMOVE[Sort, Join_AB]
3. SORT_REMOVE[Sort, Join_BA]

OPEN (retired):
1. JOIN_COMMUTE[Join_AB]
```
Notice how we have to schedule the same rule `SORT_REMOVE` twice to not miss the optimization opportunity.

#### 1.6.2 Volcano/Cascades

The same research group attempted to find more efficient optimization algorithm, what led to the development of Volcano [[2]] 
and Cascades [[3]] optimizers.

The main difference that is that Volcano/Cascades uses a **guided top-down search** strategy. Operators are optimized only
if requested explicitly by parents. Therefore, the optimizer is free to define any optimization logic it finds useful - it may 
prune some nodes completely, perform partial optimization of a node, etc. Since the search is guided, many redundant rule calls
can be avoided, because reanalyzing/rematching is no longer needed.

The guided search can be implemented either as a recursive function calls, or as a queue of tasks. In EXODUS,
the task is a rule instance, in the Cascades the queue contains optimization tasks, such as "transform this operator".

Consider a similar query plan, now optimized with the Cascades approach:

```
MEMO:
G1: [Sort]
G2:   [Join_AB]

STACK: 
-- OPTIMIZE[G1->SORT_REMOVE]
```

Then during optimization of the sort we recognize that we need to optimize the join:
```
MEMO:
G1: [Sort]
G2:   [Join_AB]

STACK:
-- OPTIMIZE[G2->JOIN_COMMUTE] 
-- OPTIMIZE[G1->SORT_REMOVE]
``` 

During join optimization, the join commute rule is fired:
```
MEMO:
G1: [Sort]
G2:   [Join_AB, Join_BA]

STACK: 
-- OPTIMIZE[G1->SORT_REMOVE]
``` 

Last, the sort elimination rule is fired on top of the already optimized `G2`.

Notice how we avoid the excessive pattern matching and rule execution due to a guided search. 

The Cascades design clearly separates logical optimization (exploration) and physical optimization (implementation).
When an unoptimized group is reached, matching transformation rules are scheduled. Then the optimization proceeds
to group inputs, and only after that the implementation rules for the group are fired. A careful guided interleaving of 
transformation rules, input optimizations, and of implementation rules ensures that a single physical plan is found as early as
possible. Once the first (sub)plan is found, the cost of the group can be calculated. Then this cost can be used to prune 
less efficient alternatives, the technique known as **branch-and-bound** pruning.
  
The Volcano/Cascades top-down guided search is widely considered superior to EXODUS and earlier bottom-up optimization 
strategies, because it allows for flexible optimization of only parts of the search space. 

Cascades-like query optimization is used in SQL Server, Pivotal Greenplum and CockroachDB, to name a few.

## 2 Apache Calcite

The initial analysis showed that a number of Java-based data management projects use Apache Calcite for query optimization. 
Apache Calcite is a framework to build data processing engines. It consists of SQL parser, query optimizer, query execution 
engine, and JDBC driver. 

During the research phase, we integrated the Apache Calcite, and at the same time researched what will it take us to implement 
our own optimizer. We revealed a significant problem with Apache Calcite optimization algorithm that could be solved to some
extent at the cost of poor optimizer performance (discussed below). At the same time we realized that the implementation of our
own optimizer will take enormous time. Therefore, the final decision was to proceed with Apache Calcite as a basis for our 
optimizer.

Apache Calcite has two optimizers - heuristic (`HepPlanner`) and cost-based (`VolcanoPlanner`). Since the heuristic
optimizer cannot guarantee the optimal plan, we use cost-based `VolcanoPlanner`. Below we discuss the design of the latter.

### 2.1 Operators and Rules

The operator abstraction is defined in the `RelNode` interface. The operator may have zero or more inputs and a set of 
properties encoded in the `RelTraitSet` data structure:
```java
interface RelNode {
    List<RelNode> getInputs(); // Inputs
    RelTraitSet getTraitSet(); // Properties
}
```
The rule abstraction is defined in the `RelOptRule` abstract class. Its constructor accepts the pattern. Users 
must implement the method `onMatch`, where the actual transformation is performed.
```java
abstract class RelNode {
    void onMatch(RelOptRuleCall call); 
}
```

### 2.2 Traits

The operator may have a custom property, defined by the `RelTrait` interface. Example property is collation (sort order).
Every `RelTrait` has a relevant `RelTraitDef` instance, that defines whether two traits of the same type satisfy one 
another. For example, `[a ASC, b ASC]` satisfies `[a ASC]`, but not vice versa.

Apache Calcite comes with two built-in traits:
- `RelCollation` - collation
- `Convention` - an opaque marker, that describes the application-specific type of the node

We will use the terms `property` and `trait` interchangeably.

#### 2.2.1 Convention

Convention is a special trait in Apache Calcite, that describes the type of the operator (`RelNode`).  

After the parsing, all operators are assigned the `Convention.NONE`, meaning they are abstract. By default, an operator 
with `NONE` convention has an infinite cost, and hence cannot be part of a valid plan. One of the goals of the planning process 
is to find nodes with non-`NONE` conventions. The method `VolcanoPlanner.setNoneConventionHasInfiniteCost` could be used to 
assign non-infinite costs to `NONE` nodes. 

Apache Calcite is meant to execute federated queries, such as `SELECT * FROM cassandra.table1 JOIN druid.table2`.
In addition, Apache Calcite comes with a couple of execution backends, `Enumerable` (interpreter) and `Bindable` (compiler).
Therefore, the original motivation for the `Convention` trait was to define the execution backend for the operator. For example,
for the query `SELECT * FROM cassandra.table1 JOIN druid.table2 WHERE table1.field = 1`, the plan after parsing will look like
```
Filter[NONE](table1.field = 1)
  Join[NONE]
    Table[NONE](cassandra.table1)
    Table[NONE](druid.table2)
```

Then we can delegate table scans to the respective backend databases, and then perform the filter and join locally using one the 
Calcite backends. Assuming we did a filter pushdown, the plan could look like:
```
Join[BINDABLE]
  Filter[BINDABLE](table1.field = 1)
    Table[CASSANDRA](table1)
  Table[DRUID](table2)
```

Or we may try to push the filter down to the Cassandra database, thus reducing the number of rows returned to the local process. 
To do this, we set the `CASSANDRA` convention to the filter node, so that the executor understands that it should be passed to 
the database, rather than be processed locally: 
```
Join[BINDABLE]
  Filter[CASSANDRA](table1.field = 1)
    Table[CASSANDRA](table1)
  Table[DRUID](table2)
```

Note that the `Convention` is merely an opaque marker that is used during the planning process. It is up to the execution
backend to decide how to deal with the marker.

Many products that integrated Apache Calcite use the `Convention` to distinguish between logical and physical operators. 

### 2.3 Memoization

The search space is organized in a collection of groups of equivalent operators, called `RelSet`. Within the `RelSet`
operators are further grouped by their physical properties into one or more `RelSubset`.  

For example, the join equivalence group might look like this:
```
RelSet#1 {
    RelSubset#1: [convention=LOGICAL, collation=NONE] -> LogicalJoin(AxB), LogicalJoin(BxA)
    RelSubset#2: [convention=PHYSICAL, collation=NONE] -> HashJoin(AxB), HashJoin(BxA)
    RelSubset#3: [convention=PHYSICAL, collation={A.a ASC}] -> MergeJoin(AxB)
}
```
  
When the plan is submitted for optimization, it's operators are copied into the MEMO. Concrete operator inputs are
replaced with the relevant RelSubset-s. E.g.:
```
BEFORE:
Sort(Scan)
  Scan

AFTER:
RelSet#1 {
    RelSubset#1: [convention=LOGICAL] -> Sort(RelSubset#2)
}
RelSet#2 {
    RelSubset#2: [convention=LOGICAL] -> Scan
}
```
  
Every operator has a signature string that uniquely identifies it. Two operators with the same signature are placed into
the same equivalence group. For example, for the query `SELECT * FROM t JOIN t`, two scans will end up in the 
same subset:
```
RelSet#1 {
    RelSubset#1: [convention=LOGICAL] -> Join(RelSubset#2, RelSubset#2)
}
RelSet#2 {
    RelSubset#2: [convention=LOGICAL] -> Scan(t)
}
``` 
  
When a rule is being executed a new operator might be created. This operator is added to the search space through 
the `RelOptRuleCall.transformTo(RelNode newOperator)` call. The `RelOptRuleCall` knows the original operator it was
called for, and hence it adds the `newOperator` to the same `RelSet` as the original operator. For example, when an index
scan is applicable for the given table scan, it will be added to the same group:
```
BEFORE:
RelSet#1 {
    RelSubset#1: [convention=LOGICAL] -> Scan(t)
}

AFTER:
RelSet#1 {
    RelSubset#1: [convention=PHYSICAL] -> Scan(table)
    RelSubset#2: [convention=PHYSICAL, collation={a ASC}] -> IndexScan(table, index(a))
}
```
  
### 2.4 Enforcers  
  
It is possible to enforce a certain trait on the operator. This is done through a `RelOptRule.convert` call. When the 
conversion is requested, the optimizer creates a special `AbstractConverter` operator. When a converter is created, an 
instance of the `AbstractConverter.ExpandConversionRule` is scheduled to expand it. This rule compares the properties 
of original operator, and the requested properties. If the original properties do not satisfy the requested ones, the
relevant `RelTraitDef` is invoked to enforce the required property. The result of the enforcement could be either a 
new operator with the desired property, or `null`, which means that the conversion is not possible.

For example, consider the following MEMO:
```
RelSet#1: [LogicalAgg(a)]
RelSet#2: [LogicalScan]
```    
There could be a rule to produce a non-blocking streaming physical aggregate. Such an aggregate requires the input to be 
sorted on the group key `a`. Therefore, the rule may enforce the conversion:    
```
RelSet#1: [LogicalAgg(a)]
RelSet#2: [LogicalScan, AbstractConverter(LogicalScan, a ASC)]
```

Later, the `AbstractConverter.ExpandConversionRule` instance expands the converter. It calls the `RelTraitDef` of the 
collation property, that adds a `LogicalSort` operator on top of the scan:
```
RelSet#1: [LogicalAgg(a)]
RelSet#2: [LogicalScan, AbstractConverter(LogicalScan, a ASC), LogicalSort(LogicalScan, a ASC)]
``` 

### 2.5 Metadata

Many optimization rules and operator cost functions require access to external metadata, such as column cardinality,
column uniqueness, etc. Apache Calcite ships with an extensible framework for metadata management. 

Every type of metadata is represented as a concrete class extending the `MetadataHandler` interface. For every operator
type, a separate method with a predefined name is created in this class.  

Then the metadata class is wired up into the `RelMetadataQuery` class by means of Janino-based code generation. An 
instance of the `RelMetadataQuery` class is passed to the rule invocation and operator cost contexts, thus providing
a centralized access point to all required metadata.

This approach is convenient and extensible, but has several performance problems:
1. Compilation with Janino may take significant time to complete, increasing planning time to seconds on a fresh JVM
1. Metadata is not cached at `RelSet`/`RelSubset` levels, and re-calculated on every call. This is not optimal,
because metadata calculation typically requires recursive dives into inputs of the operator. 

### 2.6 Execution

The `VolcanoPlanner` employs EXODUS-like approach to query optimization. It uses rules to find alternative plans. However, it 
doesn't employ the guided top-down search strategy. Instead, the optimizer organizes rule instances in a queue, and fire them
until the queue is empty. The word `Volcano` in the name is misleading, because the optimizer doesn't actually follow the 
main ideas from the Volcano/Cascades papers.

First, an instance of the `VolcanoPlanner` is created and initialized with:
1. The operator tree to be optimized (`RelNode`)
1. The list of rules to use (`RelOptRule`)
1. The list of property types that will be considered during optimization (`RelTraitDef`)
1. The desired properties of the result

Then the optimization process is started through a call to the `VolcanoOptimizer.findBestExp` method. The result is the 
optimized operator tree (`RelNode`) with the desired properties, or an exception if the desired properties cannot be 
satisfied through the given set of rules.

The optimization process is split into three main phases:
1. Copy-in
2. Optimization
3. Copy-out

#### 2.6.1 Copy-in Phase

The goal of the **copy-in** phase is to prepare the initial MEMO and rule instance queue. For every operator the following steps 
are performed:
1. A copy of the operator is created using `RelNode.copy` method, with inputs replaced with the relevant `RelSubset` instances
1. An operator is added to the relevant `RelSet` and `RelSubset` instances based on an operator's signature (`RelNode.explain`)
1. A cost of the operator is calculated and set as the `best` cost of the current `RelSubset`
1. For every rule that matches the given operator a rule instance (`VolcanoRuleMatch`) is added to the rule queue 
(`VolcanoPlanner.ruleQueue`) 

The copy-in is performed via `VolcanoPlanner.setRoot` method.

#### 2.6.2 Optimization Phase

The optimization phase proceeds as follows. The next rule instance is taken from the queue and executed. For every new 
operator created during rule invocation:
1. Add the operator to MEMO
1. Update the cost of the operator's `RelSubset` and parents, if needed
1. Schedule new rule instances for the operator and parents, if needed

The process continues until the queue is empty.

#### 2.6.3 Copy-out Phase

Once the optimization is finished, it is necessary to produce the resulting plan from the MEMO. To do this, a recursive 
top-bottom dive from the top `RelSubset` is performed. All `RelSubset` instances already have a node with the best cost at 
this point (aka "winner"). To construct the final plan, the optimizer recursively finds winners bottom-up.

## 3 Hazelcast Mustang Optimizer

We now discuss how the query optimization is organized in the Hazelcast Mustang. The process is split into the following
phases:

*Table 2: Optimization Phases*

| # | Name | Input | Output | Description |
|---|---|---|---|---|
| 1 | Parsing | `String` | `SqlNode` | Convert the query string into the parse tree |
| 2 | Validation | `SqlNode` | `RelNode` | Semantic analysis of the parse tree and conversion to operator tree |
| 3 | Rewrite | `RelNode` | `RelNode` | Remove subqueries and trim unused columns |
| 4 | Logical Optimization | `RelNode` | `RelNode` | Apply transformation rules |
| 5 | Physical Optimization | `RelNode` | `RelNode` | Apply implementation rules |
| 6 | Splitting | `RelNode` | `SqlPlan` | Create an executable query plan |

### 3.1 Parsing

The original query is passed to Calcite's parser and is converted to the parse tree (`SqlNode`). 

We use the built-in parser, since it is sufficient for the supported feature set. Jet extends the parser with 
custom commands. 

### 3.2 Validation

The parse tree is validated for semantic correctness. Tables and columns are resolved, return types of every SQL operator is 
calculated. Since Apache Calcite supports more features than we do, we additionally apply the visitor that attempts to 
find unsupported operators. When found, an exception is thrown with the precise position in the original query string.

Once the `SqlNode` is validated, it is converted to the tree of abstract relational operators (`RelNode`), all with the
`Convention.NONE`.

We use Calcite's `SqlValidator` and `SqlToRelConverter` with custom extensions for validation and conversion respectively.   

### 3.3 Rewrite

The initial `RelNode` may contain subqueries that are difficult to deal with. Generally, every subquery (correlated or not) 
could be replaced with a sequence of join and aggregate operators [[4]]. We use Apache Calcite built-in classes to eliminate
subqueries (`SubQueryRemoveRule`, `SqlToRelConverter.decorrelate`). 

After the subqueries are eliminated, unused fields may remain in the plan. We use `SqlToRelConverter.trimUnusedFields`
method to find and remove the unused fields, thus making the plan more efficient.

The result of this stage is the optimized operator tree (`RelNode`) without subqueries and unused fields.

### 3.4 Logical Optimization

Further optimization is split into two independent phases - logical and physical.

The logical optimization is concerned with transformations to the operator tree, that simplifies the plan, without 
considering their physical implementations. Generally, we do the following:
- Fuse operators together to make the tree smaller
- Removing operators that have no impact on the final result
- Removing operators that do not produce any results
  
In other optimizers, most of these rules are typically part of the rewrite phase, where the heuristic planning is used instead
of that cost-based optimization. In Apache Calcite, the `HepPlanner` could be used for this. However, some rules may trigger 
conflicting actions, causing the heuristic planner to fail. An example is filter "move around" rules: often it is important
not only to push the filter down, but also to try to push it up, because it may enable further optimizations of the parent node
(such as transitive predicate push, partition pruning, etc). The heuristic planner may fail to find these alternatives. 
Therefore, we use the cost-based `VolcanoPlanner` as slower, but safer choice. We may reconsider this in the future, and move 
some logical rules to separate stages that use heuristic planner, as it is done in other projects (e.g. Apache Flink).
For now this is not important, because we do not support joins, so our queries have small search spaces. 
  
We define the following logical operators and rules:

*Table 3: Logical Operators*

| Name | Description |
|---|---|
| `LogicalTableScan` | Scan a table |
| `LogicalProject` | Perform a projection (e.g. get an expression `a + b` from the relation `[a, b, c]` |
| `LogicalFilter` | Filter rows of the input based on the provided condition |

*Table 4: Logical Rules*

| Name | Product | Description |
|---|---|---|
| `FilterMergeRule` | Calcite | Merges two adjacent `LogicalFilter` into one to make the operator tree simpler |
| `FilterProjectTransposeRule` | Calcite | Moves `LogicalFilter` past `LogicalProject` (aka "filter pushdown") to allow for further optimizations, such as `FilterIntoScanLogicalRule` |
| `FilterIntoScanLogicalRule` | Hazelcast | Moves a `LogicalFilter` in the table of the `LogicalScan` to make the operator tree simpler |
| `ProjectMergeRule` | Calcite | Merges two adjacent `LogicalProject` into one to make the operator tree simpler |
| `ProjectFilterTransposeRule` | Calcite | Moves `LogicalProject` past `LogicalFilter` to allow for further optimizations, such as `ProjectIntoScanLogicalRule` |
| `ProjectIntoScanLogicalRule` | Hazelcast | Gets the list of required input fields from the `LogicalProject` and ensures that the child `LogicalScan` doesn't return unused fields |
| `ProjectRemoveRule` | Calcite | Removes a `LogicalProject` if it doesn't do anything, e.g. `SELECT a, b FROM (SELECT a, b FROM table)` |
| `PruneEmptyRules` | Calcite | Removes `LogicalProject` and `LogicalProject` if their input is known to be empty, e.g. `SELECT a, b FROM table WHERE 1 != 1` |

Execution of these rules might create many hundreds and thousands of alternative plans. If we add physical optimization
to this step, it could easily blow the search space, because the EXODUS-like search algorithm do not allow for search
space pruning, and requires re-execution of the same rules multiple times to guarantee that the optimal plan is found.

Consider that we produced `N` different logical plans, and have physical rules that may produce `M` alternatives 
for every logical plan. As a result, we will have to consider `N * M` plans. Instead, we perform the logical 
optimization in a separate step, extract only one best plan from the search space, and then apply the physical rules
to on the next stage. This way, we have to consider only `N + M` plans, that alleviates the inefficiency of the core
search algorithm of `VolcanoPlanner`.

The fundamental observation, is that splitting optimization into several phases doesn't guarantee the optimal plan 
in the general case. That is, if we generated plans `P1, P2 ... Pm ...`, and picked `Pm` as the best one, it
doesn't mean that applying additional rules to `Pm` will produce the optimal plan `Pm'`. Another plan `Pn`, 
such that `cost(Pn) > cost(Pm)`, could yield better plan `Pn'`, such that `cost(Pn') < cost(Pm')`.

Therefore, the logical phase should generally include rules, that produce plans that are generally the best starting
points for the subsequent physical optimization with high probability. Operator fusion, removal of unused operators, scan 
field trimming, and filter pushdowns produces better plans in almost all cases, this is why we execute them at this 
stage.

As a part of the logical optimization process, we convert the Calcite operators with the convention `Convention.NONE` 
to our own logical operators with the convention `HazelcastConvention.LOGICAL`. We do this through a special conversion
rules that extend Calcite's `ConverterRule`.  

If there is a Calcite operator in the tree that doesn't have a logical counterpart, it indicates that there is either an 
unsupported operation, that we missed during validation phase, or that we missed some conversion rule. An exception will be 
thrown in this case.

*Table 5: Logical Conversion Rules*

| Name | From (Calcite) | To (Hazelcast) |
|---|---|---|
| `MapScanLogicalRule` | `LogicalTableScan` | `MapScanLogicalRel` |
| `FilterLogicalRule` | `LogicalFilter` | `FilterPhysicalRel` |
| `ProjectLogicalRule` | `LogicalProject` | `ProjectLogicalRel` |
| `ValuesLogicalRule` | `LogicalValues` | `ValuesLogicalRel` |

Last, we manually add a special `RootLogicalRel` on top of the result. This operator is an abstraction of a user query
cursor that returns results on the initiator member. 

The result of the logical optimization is an optimized tree of operators with the `HazelcastConvention.LOGICAL` convention, 
that has `RootLogicalRel` at the root.

### 3.5 Physical Optimization

The goal of the physical optimization is to find the physical implementations of logical operators. Some physical operators
have 1-to-1 mapping to their logical counterparts (e.g. project), while others may have completely different implementations 
(e.g. index scan created out of logical table scan). 

#### 3.5.1 Distribution Trait

Since Hazelcast stores data on multiple nodes, we should take the data distribution in count during planning. This is achieved
through the `DistributionTrait` property that is assigned to physical operators during planning.   

*Table 6: Distribution Types*

| Name | Description |
|---|---|
| `ROOT` | The result set is located on the initiator member only |
| `PARTITIONED` | The result set is distributed across member, every row is located on one member only |
| `REPLICATED` | The whole result set is located on every member |

When the optimization starts, we request the `ROOT` distribution from the planner. This literally means "deliver the final 
result to the initiator member". 

The `DistributionTraitDef` defines what should happen if a parent member requests a certain distribution that cannot
be satisfied by the child. If the data movement is needed, a special `Exchange` operator is injected between the 
parent and the child. The goal of the `Exchange` operator is to move data between members to get the desired distribution.
Therefore, the `Exchange` operator is the enforcer operator for the `DistributionTrait`, similarly to `Sort` that is the
enforcer operator for the `RelCollation`.

Currently, we support only `RootExchangePhysicalRel` that delivers results to the initiator node. Implementation of joins,
aggregations, and sorting will require specialized implementations of the `Exchange` operator. 

The table below summarizes how the ROOT distribution is enforced.

*Table 7: Root Distribution Enforcers*
| Input Distribution | Result |
|---|---|
| `ROOT` | No-op, the child operator is already located on the initiator |
| `PARTITIONED` | No-op, if there is only one member in the topology. Otherwise, inject `RootExchangePhysicalRel` on top of the child |
| `REPLICATED` | No-op, the child operator is already located on all members, including the initiator |

Below is the example of the distribution enforcement for a simple logical plan:
```
LOGICAL:
RootLogicalRel                      // Return to the user from the initiator
  MapScanLogicalRel                 // Scan an IMap

PHYSICAL:
RootPhysicalRel[ROOT]               // Return to the user from the initiator
  RootExchangePhysicalRel[ROOT]     // Send to the initiator
    MapScanPhysicalRel[PARTITIONED] // Scan an IMap on all nodes
```

#### 3.5.2 Optimization Algorithm

The goal of the optimizer is to find the cheapest plan that has the `ROOT` distribution at the top operator.  

Currently, the optimization is performed **bottom-up**. We start with the leaf nodes, because their distribution is
always known. When physical implementations for the leaf node are found, rules are triggered on the parent nodes, and 
parent distributions are resolved. The process continues until we reach the root node, that always has `ROOT` distribution. 
Then the root node enforces the `ROOT` distribution on the input, adding the `RootExchangePhysicalRel` if needed.

*Table 8: Initial Distributions of the Leaf Nodes*

| Node | Distribution | Comment |
|---|---|---|
| `MapScanLogicalRel` | `PARTITIONED` | Scan of the partitioned `IMap` |
| `ValuesLogicalRel` | `REPLICATED` | Constant values, that are delivered to all members as a part of the plan |
  
The `VolcanoPlanner` is not suitable to work in the bottom-up trait propagation out-of-the-box. Therefore, we adjust our 
integration with the Apache Calcite as follows:
1. During the physical optimization, we gradually convert nodes from the `LOGICAL` convention to `PHYSICAL`. For the 
`PHYSICAL` convention we override a couple of methods (see `HazelcastConventions.PHYSICAL`) that roughly forces the optimizer
to do the following: when a new `PHYSICAL` node is created, force re-optimization of the `LOGICAL` parent. This way, whenever a 
new `PHYSICAL` node is added to MEMO, the rules for the `LOGICAL` parent node is added to the execution queue. 
1. Optimization rules for intermediate nodes (i.e., not leaves, and not root) follow a similar pattern: get the input's 
`RelSet`, extract all `RelSubset`-s with `PHYSICAL` convention, and create one intermediate physical node per `RelSubset`. This
way we ensure that all possibly interesting properties of the current group are propagated to parent groups.

Note that this algorithm uses the optimistic approach: we create intermediate physical nodes for every possible
combination of physical properties, assuming it will help parents find better plans. For complex plans, this may
create too many operators. A better approach would be to create only those intermediate operators that are really
required by parents, effectively changing the direction of the optimization: top-down instead of bottom-up. To achieve
this we may use the solution used in Apache Flink (see `FlinkExpandConversionRule`). It is likely, that we will have
to switch to this approach, when joins are implemented.
        
#### 3.5.3 Optimizations

Currently, we have the following physical optimizations.

*Table 9: Physical Optimizations*

| Name | Produces | Description |
|---|---|---|
| `RootPhysicalRule` | `RootPhysicalRel` | Convert logical root to physical root, enforcing the `ROOT` distribution on the input |
| `ProjectPhysicalRule` | `ProjectPhysicalRel` | Convert logical project to physical project, propagate input properties |
| `FilterPhysicalRule` | `FilterPhysicalRel` | Convert logical filter to physical filter, propagate input properties |
| `ValuesPhysicalRule` | `ValuesPhysicalRel` | Convert logical values to physical values, assign `REPLICATED` distribution |
| `MapScanPhysicalRule` | `MapScanPhysicalRel`, `MapIndexScanPhysicalRel` | Convert logical map scan to physical map scan or physical map index scan based on the filter condition, assign `PARTITIONED` distribution |

The result of the physical optimization is an optimized tree of operators with the `HazelcastConvention.PHYSICAL` convention, 
that has `RootPhysicalRel` at the root, and has zero or more `Exchange` operators that move data within the cluster.

### 3.6 Splitting

Finally, we split the physical plan into one or more fragments, using `Exchange` operators as fragment boundaries:
1. Every node is converted to the `PlanNode` counterpart, that could be serialized and sent over network
1. Every `Exchange` node is converted into a pair of `send` and `receive` plan nodes (actual implementation depends on the 
exchange type)

Consider the following physical plan:
```
RootPhysicalRel[ROOT]               // Return to the user from the initiator
  RootExchangePhysicalRel[ROOT]     // Send to the initiator
    MapScanPhysicalRel[PARTITIONED] // Scan an IMap on all nodes
```

After the split it is converted into a plan with two fragments:
```
FRAGMENT 1:
RootPlanNode                       // Return to the user from the initiator
  ReceivePlanNode[edge=1]          // Receive results from all members

FRAGMENT 2:
RootSendPlanNode[edge=1]           // Send results to the initiator
  MapScanLogicalRel[PARTITIONED]   // Scan an IMap locally
```

The split logic is located in the `PlanCreateVisitor` class. The result is the `Plan` object, that doesn't depend
on Apache Calcite, and can be sent to other nodes for execution. 

[1]: https://dl.acm.org/doi/10.1145/38713.38734 "The EXODUS optimizer generator"
[2]: https://dl.acm.org/doi/10.5555/645478.757691 "The Volcano Optimizer Generator: Extensibility and Efficient Search"
[3]: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.98.9460 "The Cascades Framework for Query Optimization"
[4]: https://www.semanticscholar.org/paper/Unnesting-Arbitrary-Queries-Neumann-Kemper/3112928019f64d8c388e8cfbae34b9887c789213 "Unnesting Arbitrary Queries"
