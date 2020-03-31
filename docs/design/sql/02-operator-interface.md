# SQL Operator Interface

## Overview
In databases, SQL queries are typically represented in a form of operator tree, called **Volcano Model**,
introduced in Goetz Graefe seminal paper [[1]].

In this document, we describe the design of the Hazelcast Mustang operator interface, which is based
on the Volcano Model.

## 1 Relational Operators
An SQL query is first parsed into a **parse tree**, which is used for syntactic and semantic checking.

The parse tree is then converted into **relational operator tree**, or simply **relational tree**,
for optimization. The relational tree is more convenient because its structure is simpler than the
structure of the parse tree.

A **query plan**, consisting of a relational tree and supplemental information, is submitted for execution
after the optimization.

The table below lists common relational operators used in database engines.

*Table 1: Common Relational Operators*

| Name | Description |
|---|---|
| `Scan` | Iterate over source rows |
| `Project` | Return a set of original or derived attributes of the child operator |
| `Filter` | Return rows of the child operator which pass the provided predicate |
| `Aggregate` | Aggregate rows of the child operator |
| `Sort` | Sort rows of the child operator |
| `Join` | Join rows from several child operators |

An example of a query, its parse tree, and its relational tree is provided below.

*Snippet 1: Query*
```sql
SELECT a, SUM(b)
FROM table
GROUP BY a
HAVING SUM(b) > 50
```
*Snippet 2: Parse Tree*
```
-- Select
---- SelectList [a, SUM(b)]
---- From [table]
---- GroupBy [a]
---- Having [SUM(b) > 50]
```
*Snippet 3: Relational Tree*
```
-- Filter [SUM(b) > 50]
---- Aggregate [a -> SUM(b)]
------ Project [a, b]
-------- Scan [table]
```

## 2 Volcano Model

Volcano Model defines the common data exchange interface between operators in the relational tree. This allows
for extensibility, as new operators could be implemented with minimal changes to the engine.

In the original paper the interface consists of three operations:

*Snippet 4: Volcano Interface*
```java
interface Operator {
    void open();  // Initialize the operator
    Row next();   // Get the next row
    void close(); // Close the operator and release all resources
}
```

## 3 Volcano Model in Hazelcast Mustang

The original Volcano Model has two drawbacks:
1. Operators exchange one row at a time, which leads to performance overhead
2. Call to the `next()` is blocking, which is not optimal for the distributed environment, where
operators often wait for remote data or free space in the send buffer.

To achieve high performance, we introduce several changes to the original Volcano Model: batching and
non-blocking execution.

### 3.1 Row and RowBatch
We define the `RowBatch` interface which is a collection of rows (tuples).

*Snippet 5: RowBatch interface*
```java
interface RowBatch {
    Row getRow(int index); // Get the row by index
    int getRowCount();     // Get the number of rows 
} 
```

Then we define the `Row` interface, which provides access to values by index. The `Row` itself is considered
as a special case of `RowBatch` with one row. This allows saving on allocations in some parts of the engine.

*Snippet 6: Row interface*
```java
interface Row extends RowBatch {
    Object get(int index); // Get the value by index
    int getColumnCount();  // Get the number of values in the row 
    
    default int getRowCount() {
        return 1;
    }
    
    default Row getRow(int index) {
        return this;
    }
}
```

### 3.2 Operator
The operator is defined by `Exec` interface:
1. Operators exchange `RowBatch` instead of `Row`
1. The blocking `next()` method is replaced with the non-blocking `advance()` method, which returns the iteration
result instead of the row batch
1. The `RowBatch` could be accessed through a separate method
1. The `open()` method is renamed to `setup()`. Special query context is passed to it as an argument
1. There is no separate `close()` method because the engine doesn't need explicit per-operator cleanup at the
moment. This may change in future, in this case the current document should be updated accordingly

*Snippet 7: Executable operator interface*
```java
interface Exec {
    void setup(QueryContext context); // Initialize the operator
    IterationResult advance();        // Advance the operator if possible; never blocks
    RowBatch currentBatch();          // Get the batch returned by the previous advance() call 
}
```

The result of iteration is defined in the `IterationResult` enumeration.

*Snippet 8: IterationResult enumeration*
```java
enum IterationResult {
    FETCHED,      // Iteration produced new rows
    FETCHED_DONE, // Iteration produced new rows and reached the end of the stream, no more rows are expected
    WAIT;         // Failed to produce new rows, release the control
}
```

When the engine has received `FETCHED` or `FETCHED_DONE` from the `Exec.advance()` call, it may access the produced row batch
through the `Exec.currentBatch()` call. The ownership of the batch is held by the producer. The content of the row batch is valid
until the next `advance()` call on the producer. If the consumer may require access to the row batch content after the next call
to `advance()`, it should make a copy of the batch.

If the engine has received `WAIT`, then query execution is halted, and the control is transferred to another query in the
execution queue. The query execution is resumed upon an external signal (e.g. when the batch arrives from the remote node,
or free space in the send buffer appears).

## 4 Implementation Guidelines
Operator implementations must adhere to the following rules:
1. `Row` instances should be immutable to facilitate their transfer between batches
1. The row batch returned by the `Exec.currentBatch()` call is valid before the next call to the `Exec.advance()` interface.
Do not use the reference to the batch outside of this scope
1. Operator's state is not required to be thread-safe
1. Use row batches to minimize evaluation overhead
1. Avoid blocking the thread while waiting for data send or receive
1. Avoid blocking synchronization when possible

[1]: https://dl.acm.org/doi/10.1109/69.273032 "Volcano - An Extensible and Parallel Query Evaluation System"
