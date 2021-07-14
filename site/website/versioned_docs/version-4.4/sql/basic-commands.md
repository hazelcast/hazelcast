---
title: SQL Statements
description: Description of SELECT, INSERT and SINK statements
id: version-4.4-basic-commands
original_id: basic-commands
---


## SELECT statement

### Synopsis

```sql
SELECT [ ALL | DISTINCT ] [ * | expression [ [ AS ] expression_alias ] [, ...] ]
FROM from_item [, ...]
[ WHERE condition ]
[ GROUP BY { expression | expression_index } [, ...] ]
[ HAVING condition [, ...] ]
```

`from_item` is one of:

```sql
[schema_name.]table_name [ [ AS ] table_alias]

(SELECT ... ) AS table_alias

TABLE(function_name([ argument [, ...] ] ))

(VALUES (expression, [, ...]) [, ...] )

from_item  join_type  from_item [ ON join_condition | USING ( join_column [, ...] ) ]
```

`join_type` is one of:

```sql
[INNER] JOIN

LEFT [OUTER] JOIN

CROSS JOIN
```

The clauses above are standard SQL clauses. The `table_name` is a
mapping name, either as created using [DDL](ddl.md) or one created
automatically for non-empty IMaps.

Jet supports all operators and functions supported by IMDG. Go to the
[chapter on SQL](https://docs.hazelcast.org/docs/4.1.1/manual/html-single/index.html#sql)
in the Hazelcast IMDG reference manual for the full reference.

### Aggregate Functions

Jet supports these aggregate functions:

| Name<img width='350'/> | Description |
|--|--|
|`COUNT(*)` :: `BIGINT` | Computes the number of input rows. |
|`COUNT(any)` :: `BIGINT` | Computes the number of input rows in which the input value is not null. |
|`SUM(BIGINT)` :: `BIGINT`<br>`SUM(DECIMAL)` :: `DECIMAL`<br>`SUM(DOUBLE)` :: `DOUBLE` | Computes the sum of the non-null input values. |
|`AVG(DECIMAL)` :: `DECIMAL`<br>`AVG(DOUBLE)` :: `DOUBLE` | Computes the average (arithmetic mean) of all the non-null input values. |
|`MIN(any)` :: _same as input_ | Computes the minimum of the non-null input values. Applicable also to OBJECT type, if the underlying value is `java.lang.Comparable` |
|`MAX(any)` :: _same as input_ | Computes the maximum of the non-null input values. Applicable also to OBJECT type, if the underlying value is `java.lang.Comparable` |

Except for `COUNT`, the functions return NULL when there were no rows to
aggregate. They cannot be applied to streaming inputs: they need to
accumulate the whole of input to produce some results, but a streaming
input has no end. Currently, aggregate functions can't be used for data
coming from IMaps, either, because Jet doesn't currently support reading
from IMaps.

You can prepend `DISTINCT` to the argument of any aggregate function in
order to supply only the distinct values to it. In the case of
`MIN`/`MAX` this makes no difference and the keyword is ignored. For
example, this query calculates the number of distinct colors of cars in
the table:

```sql
SELECT COUNT(DISTINCT color)
FROM cars
```

#### Memory considerations

While computing an aggregate function over records grouped by a key, Jet
must store the aggregation state of all the groups at the same time. If
you use the `DISTINCT` keyword, it must also store all the distinct
values. Jet currently does not have any memory management. If the number
of groups in the result is large, it can lead to an `OutOfMemoryError`,
after which the cluster might be unusable. One technique to reduce the
memory needs is to arrange for the input stream to be sorted by the
grouping key: then you can store the aggregation state of just one key
at a time. Once we add aggregate functions to the default SQL engine, we
will leverage this optimization.

### Isolation level

The isolation level for each mapping is defined by the `TYPE` is uses
and in some cases can be tuned by the mapping `OPTIONS`. In general it's
_read-committed_. One aspect of this mode is that it doesn't prevent
reading different versions of a single row while executing a single
query. In streaming mode this behavior is even desired: for example, if
you join a record from an IMap to rows from a Kafka topic, this query
can run for months and you want to see the current version of the IMap
entry, not the version from the time when the query was started.

## INSERT/SINK statement

### Synopsis

```sql
[ INSERT | SINK ] INTO [schema_name.]table_name[(column_name [, ...])]
{ SELECT ... | VALUES(expression, [, ...]) [, ...] }
```

Jobs that process unbounded streams typically read from one or more
sources and write to a sink. However, writing to the sink doesn't
directly map to SQL commands. A Jet sink isn't limited to only insert
rows. Even the standard `MERGE` statement isn't easily applicable
because it can't modify a single row more than once.

As a solution, Jet uses a non-standard `SINK INTO` command, whose
semantics depend on the underlying sink connector. Jet takes the output
of the SELECT statement and sends it to the sink to process. For
example, when writing to an IMap, the value associated with the key is
overwritten, and one key can be overwritten multiple times.

Some connectors support the `INSERT INTO` statement. If they do, the
behavior is defined by the SQL standard. For example, the Apache Kafka
connector supports it, because Kafka topics are append-only. Jet doesn't
support `DELETE`, `UPDATE` or `MERGE` statements.

### Transactional behavior

In SQL a DML statement is always atomic. That is it executes in full or
not at all, and a statement can even be a part of a bigger transaction.
Other clients aren't allowed to see partial changes. In streaming DML
this is not possible: since the statement never completes, we will never
see any result. Therefore Jet relaxes the behavior: the sink is free to
define its own transaction behavior. Typically, stream items are
committed in the target system in batches.

Additionally, Jet has an option to run streaming DML with a processing
guarantee - the underlying job will restart in case of failures. It can
run in at-least-once mode in which it can happen that some stream items
are processed multiple times. Even in exactly-once mode, some connectors
don't support it and it can happen that some items are written multiple
times. Any of this isn't allowed by the SQL standard. For details check
the documentation of each connector.

## Case sensitivity

Identifiers such as table and column names are case-sensitive. Function
names and SQL keywords aren't. If your identifier contains special
characters, use `"` to quote it. For example, if your map is named
`my-map`:

```sql
SELECT * FROM "my-map";  -- works
sElEcT * from "my-map";  -- works
SELECT * FROM my-map;    -- fails, `-` interpreted as subtraction
SELECT * FROM "MY-MAP";  -- fails, map name is case-sensitive
```
