# CREATE VIEW, DROP VIEW statements

### Table of Contents

+ [Background](#background)
    - [Description](#description)
+ [Functional Design](#functional-design)
    * [Summary of Functionality](#summary-of-functionality)
+ [Technical Design](#technical-design)
    * [Notes/Questions/Issues](#notesquestionsissues)
+ [Testing Criteria](#testing-criteria)

|ℹ️ Since: 5.1|
 |-------------|

|||
 |---|---|
|Related Jira|[HZ-710](https://hazelcast.atlassian.net/browse/HZ-710)|
|Related Github issues|_-_|
|Document Status / Completeness|IN REVIEW|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|TBA|
|Support Engineer|TBA|
|Technical Reviewers|Viliam Durina|

### Background

#### Description

This document describes SQL `CREATE VIEW` and `DROP VIEW` statements semantics and design. It's a step to improve
Hazelcast ANSI SQL standard compatibility and enrich available SQL features.

### Functional Design

#### Summary of Functionality

##### CREATE VIEW

Simply said, `CREATE VIEW` statement creates virtual table based on the result set of an SQL statement. We store all
view in an internal ReplicatedMap. This table replicates its content across all cluster members. Then, aliased query (
the view) may be inlined into another query which uses the view.

Proposed grammar:

```
CREATE [ OR REPLACE ] VIEW [ IF NOT EXISTS ] name AS query
```

Note: the `query` must be a SELECT statement.

Example:

```
CREATE MAPPING m TYPE IMap OPTIONS (...);
CREATE VIEW v AS SELECT * FROM m WHERE ...;

SELECT * FROM v
-- transforms into
SELECT * FROM (SELECT * FROM m WHERE ...);
```

All optimizations are applied to the query as if the view was inlined into the outer query. Example:

```
CREATE MAPPING m TYPE IMap OPTIONS ('keyFormat'='int','valueFormat'='int');
CREATE VIEW v AS SELECT * FROM m WHERE __key < 20;

SELECT * FROM v WHERE __key > 10
-- transforms into
SELECT * FROM m WHERE __key > 10 AND __key < 20;
```

Optimizer eliminates the redundant projection (since they are equal) and merges the bounds
check (`__key > 10 AND __key < 20`).

##### DROP VIEW

Proposed grammar:

 ```
DROP [ IF EXISTS ] VIEW name
 ```

### Technical Design

Executing the `CREATE VIEW` statement is supposed to create a persistent catalog object.

Let's review proposed grammar:

```
CREATE [ OR REPLACE ] VIEW name AS query
```

Statement parameters:

- **OR REPLACE** -- If the view already exists, replace it. Without this option the command will fail.
- **name** - view name.
- **query** - valid SQL query.

#### Views Storage

The SQL engine stores catalog objects in `__sql.catalog` ReplicatedMap. As views share namespace with mappings, it makes
sense to store them in a single map to avoid naming conflicts. However, this map is read also by older members during
rolling upgrade. Due to this, we must ensure that no view is stored in the map before the cluster is upgraded to 5.1.

A view is defined by the query text. The query text isn't processed as-is: when the view is created, we validate it,
resolve the identifiers and functions, we also expand `*` projection. For example, input query

```sql
select * from m
```

will be stored as:

```sql
SELECT column1, column2 FROM hazelcast.public.m
```

If the table `m` doesn't exist at the time the view is created, the view creation will fail.

#### Dependency Management

Practically all views depend on other views or mappings. SQL databases, traditionally, strictly track these
dependencies: if a view `a` depends on table `b`, then the user isn't allowed to drop the table `b`. If a
`CASCADE` option is provided in the `DROP` command, then when table `b`
is dropped, the view `a` is dropped as well, atomically.

Secondly, modifications to objects on which a view depends, which would change the view's output, are also not allowed.
For example:

```
CREATE MAPPING m (col1 INT, col2 INT) ...;

CREATE VIEW v AS SELECT col1 FROM m;

-- col1 would be removed, opertion not allowed because `v` depends on it.
CREATE OR REPLACE MAPPING m (col2 INT) ...;  
```

We've considered two approaches to address this:

##### 1. Strict dependency management

Due to the lack of transactions we can utilize distributed locks: this limits parallelism greatly, but it's acceptable
for DDL. We have to correctly handle locks held by failed members, undo or finish partial changes etc.

##### 2. Lazy dependency management

We'll initially validate the query at view creation time. Later, if there are changes to the objects it depends on, they
will always be allowed. When the view is used anew, we'll re-validate the query.

For example, if we drop a column from a mapping the view `v` depends on
(as in the example above), the command will succeed. However, subsequent
`SELECT * FROM v` will fail because the column is missing.

There's already a precedent for this behavior in the SQL engine: SQL jobs also depend on mappings. But if a job is
running, we still allow changes to the mapping and these changes don't affect the job at all.

One more argument for this behavior is that the SQL engine depends on external objects for which we cannot strictly
control dependencies. If the remote object is dropped, the mapping will be broken, and we'll detect that at the next
attempt to use that mapping.

#### Information_Schema views

The `information_schema.views` view provides a view into the engine metadata for the user. It's standardized. View
columns are in
`information_schema.columns`, which we already have. In case we use strict dependency management, we can store column
list in the catalog. However, with lazy validation, we will need to validate all views to produce the column list at the
time the `columns` view is queried.

|     Column     |  Type   |
|----------------|---------|
| table_catalog  | VARCHAR |
| table_schema   | VARCHAR |
| table_name     | VARCHAR |
| view_definition     | VARCHAR |
| check_option   | VARCHAR |
| is_updatable   | VARCHAR  [NO / YES] |
| insertable_into | VARCHAR [NO / YES] |

#### Security

We'll add `CREATE_VIEW` and `DROP_VIEW` permissions to complement current `CREATE_MAPPING` and `DROP_MAPPING`
in `SqlPermission`. This follows the usual pattern of having separate permissions for each distinct schema object in
databases.

#### Notes/Questions/Issues

Use the ⚠️ or ❓icon to indicate an outstanding issue or question, and use the ✅ or ℹ️ icon to indicate a resolved issue
or question.

### Testing Criteria

Unit tests and soak tests are enough.