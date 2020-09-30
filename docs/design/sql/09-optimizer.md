# SQL Optimizer 

## Overview

When a query string is submitted for execution, it is first parsed, converted to a tree of relational operators, and then 
passed to the optimizer in order to find the best execution path.

In this document, we describe the design of the Hazelcast Mustang query optimizer that is based on Apache Calcite.

## 1 Theory

In this section we describe the theoretical aspects of query optimization that forms the basis of the Hazelcast Mustang query 
optimizer.

### 1.1 Definitions

First, we define several entities from the relational algebra. The definitions are not necessarily precise from the 
relational algebra standpoint, but are sufficient for the purpose of this document. 

A **tuple** is an ordered collection of triplets `[name, type, value]`. A **relation** is an unordered collection of 
tuples. A relational **operator** is a function that takes arbitrary arguments (possibly, other operators), and produces a 
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
operator type, and operator arguments. The plan with the **least cost** is said to be the best plan for the given query.     

### 1.3 Properties

A **property** is an arbitrary value assigned to an operator, that is used during the optimization process. The query optimizer
may **enforce** a certain property on the operator. If the operator satisfies the required property, it is left unchanged. 
Otherwise, an **enforcer** operator is applied to the original operator to meet the optimizer requirements.

In the query optimization literature, the **collation** (aka sort order) is represented as a property. The **Sort** operator is 
a typical enforcer operator.

Consider the class `Person {id:Long, name:String}` and an `IMap<Long, Person>` with two entries `[{1, "John"}, {2, "Jane}]`.
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
{id:BIGINT:1, name:VARCHAR:"John"}
{id:BIGINT:2, name:VARCHAR:"Jane"}
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
{id:BIGINT:2, name:VARCHAR:"Jane"}
{id:BIGINT:1, name:VARCHAR:"John"}
```

While these two plans are equivalent in the relational theory, they produce different results from the user standpoint. 
The difference comes from the different values of the collation property: the first query has collation `[a ASC]`, while
the second query has collation `[a DESC]`.

### 1.4 MEMO

During the optimization, quite a few alternative plans could be created (thousands, millions, etc). Therefore, it is important
to encode the search space efficiently, to prevent out-of-memory conditions. The so-called **MEMO** data strcuture is often used
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
MEMO we would have to create two separate plans:
```
Project[name]
  TableScan[person]
```   
```
Project[name]
  IndexScan[person, index]
```
With MEMO, the plan is first copied into the search space:
```
G1
  G2

G1: {Project[name]}
G2: {TableScan[person]}
```
Then, when an alternative scan path is found, it is added to the group:
```
G1
  G2

G1: {Project[name]}
G2: {TableScan[person], IndexScan[person, index]}
```  

### 1.5 Logical and Physical Operators

In query optimization literature, there are typically two operator types. **Logical operators** are operators that define data 
transformations. **Physical operators** are specific algorithms that implement particular transformations. Below are several 
examples of logical operators and their physical counterparts.

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
The rules applied to logical operators are named **transformation rules(*, while the rules applied to physical operators
are named **implementation rules**.

Initially, the optimizer accepts the operator tree, and a set of transformation rules. For every rule, a pattern
matching is performed for the available operators. If a rule matches the given part of the operator tree, an
instance of the rule is placed into a priority queue called **OPEN**. This way the initial queue of rule instances
is formed.

Then the optimizer takes rule instances from the queue and applies them to the operator tree. The result of rule
execution is zero, one or more new logical operators. For every new logical operator, matching implementation rules
are executed, possibly producing new physical operators. Newly created operators are stored in a MEMO-like data 
structure called **MESH**. 

When a new operator is created, it's cost is estimated. Since it may have better cost than previously known operators
of the same equivalence group, it is necessarily to recalculate costs of parent operators. This step is called **reanalyzing**,
and is performed by re-execution of the implementation rules on parents. In addition to this, if a new logical operator was 
created, there might be new possibilities for further transformations. Therefore, the transformation rule instances are 
scheduled for execution on parent operators (i.e. added to OPEN). This step is called **rematching**.
   
The algorithm proceeds until OPEN is empty.

The EXODUS optimizer doesn't have a strict order of rule execution. One may assign weigths to rule instance to make them
fire earlier, but generally the search is not guided - every rule instance fires independently of others. As a result, the
same rule may fire multiple times during reanalyzing/rematching, what makes the engine inefficient.

Consider the following operator initial plan, and two rules one that changes the join order, and the other one that attempts 
to remove the sort operator if its input is already sorted:
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
G2:   [Join_AB]

OPEN (pending):
2. SORT_REMOVE[Sort, Join_AB]
3. SORT_REMOVE[Sort, Join_BA]

OPEN (retired):
1. JOIN_COMMUTE[Join_AB]
```
Notice how we have to schedule the same rule `SORT_REMOVE` twice not to miss the optimization opportunity.

#### 1.6.2 Volcano/Cascades

The same research group attempted to find more efficient optimization algorithm, what led to the development of Volcano [[2]] 
and Cascades [[3]] optimizers.

The main difference that is that Volcano/Cascades uses a **guided top-down search** strategy. Operators are optimized only
if requested explicitly by parents. Therefore, the optimizer is free to define any optimization logic it finds useful - it may 
prune some nodes completely, perform partial optimization of a node, etc. Since a search is guided, many redundant rule calls
could be avoided, since reanalyzing/rematching required by EXODUS is no longer needed.

The guided search could be implemented either as a recursive function calls, or as a queue of tasks. But while in the EXODUS
the task is a rule instance, in the Cascades the queue contains optimization tasks, such as "transform this operator".

Consider the similar query plan, now optimized with the Cascades approach:

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

During join optimization, the join associate rule is fired:
```
MEMO:
G1: [Sort]
G2:   [Join_AB, Join_BA]

STACK: 
-- OPTIMIZE[G1->SORT_REMOVE]
``` 

Last, the sort elimination rule is fired on top of the already optimized `G2`.

Notice, how we avoid excessive pattern matching and rule execution due to a guided search. 

The Cascades design clearly separates logical optimization (exploration) and physical optimization (implementation).
When an unoptimized group is reached, matching transformation rules are scheduled. Then the optimization proceeds
to group inputs before the transformation rules are fired. Finally, the implementation rules are fired. Careful
guided interleaving of transformation rules, input optimization, and implementation rules ensures that a single
physical plan is found as early as possible. Once the first (sub)plan is found, the cost of the group could be 
calculated. Then this cost could be used to prune less efficient alternatives, the technique known as **branch-and-bound** 
pruning.
  

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

### 2.1 Design

Apache Calcite has two optimizers - heuristical (`HepPlanner`) and cost-based (`VolcanoPlanner`). Since the heurisitical
optimizer cannot guarantee the optimal plan, we use cost-based `VolcanoPlanner`. Below we discuss the design of the latter.

The `VolcanoPlanner` employs EXODUS-like approach to query optimization. It uses rules to find alternative plans. However, it 
doesn't employ the guided top-down search strategy. Instead, the optimizer organizes rule instances in a queue, and  
The word `Volcano` in the name is a bit misleading, because the optimizer doesn't actually follow the main ideas from the 
Volcano/Cascades papers.

#### 2.1.1 Operators and Rules

The operator abstraction is defined in the `RelNode` interface. The operator may have zero or more inputs, and a set of 
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

#### 2.1.2 Traits

The operator may have a custom property, defined by the `RelTrait` interface. Example property is collation (sort order).
Every `RelTrait` has a relevant `RelTraitDef` instance, that defines whether two traits of the same type satisfies one 
another. For example, `[a ASC, b ASC]` satisfies `[a ASC]`, but not the vice versa.

Apache Calcite comes with two built-in traits:
- `RelCollation` - collation
- `Convention` - an opaque marker, that describes the application-specific type of the node

#### 2.1.3 Memoization

The search space is organized in a collection of groups of equivalent operators, called `RelSet`. Within the `RelSet`
operators are further grouped by their physical properties into one or more `RelSubset`.  

For example, the join equivalence group might look like this:
```
RelSet#1 {
    RelSubset#1: [convention=LOGICAL, collation=NONE] -> LogicalJoin(AXB), LogicalJoin(BxA)
    RelSubset#2: [convention=PHYSICAL, collation=NONE] -> HashJoin(AXB), HashJoin(BxA)
    RelSubset#3: [convention=PHYSICAL, collation={A.a ASC}] -> MergeJoin(AXB)
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
the same equivalence group. For example, for the query `SELECT * FROM t JOIN t`, two table operators will end up in the 
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
called for, and hence it adds the `newOperator` to the same `RelSet` as the original one. For example, when an index
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
  
#### 2.1.4 Enforcers  
  
It is possible to enforce a certain trait on the operator. This is done through a `RelOptRule.convert` call. When the 
conversion is requested, the optimizer creates a special `AbstractConverter` operator. When a converter is created, an 
instance of the `AbstractConverter.ExpandConversionRule` is scheduled to expand it. When this rule fires, it compares
the properties of original operator, and the requested properties. If the original properties do not satisfy the requested
ones, the relevant `RelTraitDef` is invoked to enforce the required property. The result of the enforcement could be 
either a new operator with the desired property, or `null`, which means that the conversion is not possible.

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

#### 2.1.5 Execution

TODO

## 3 Hazelcast Mustang Optimizer

TODO

[1]: https://dl.acm.org/doi/10.1145/38713.38734 "The EXODUS optimizer generator"
[2]: https://dl.acm.org/doi/10.5555/645478.757691 "The Volcano Optimizer Generator: Extensibility and Efficient Search"
[3]: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.98.9460 "The Cascades Framework for Query Optimization"
