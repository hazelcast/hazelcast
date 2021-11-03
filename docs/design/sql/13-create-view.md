# CREATE VIEW statement

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
|Document Status / Completeness|DRAFT|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|TBA|
|Support Engineer|TBA|
|Technical Reviewers|Viliam Durina|

### Background

#### Description

This document describes SQL `CREATE VIEW` statement. It's a next step to improve Hazelcast ANSI SQL standard
compatibility and enrich available SQL features.

### Functional Design

#### Summary of Functionality

Simply said, `CREATE VIEW` statement creates an alias for the query and saves it in special service table. This table
spreads its content across all cluster members (same behaviour as for SQL mappings). Then, aliased query (view) text may
be inlined into query which uses the view.

Proposed grammar:

 ```
CREATE [ OR REPLACE ] VIEW name AS query
 ```

Example:

```
CREATE MAPPING m TYPE IMap OPTIONS (...);
CREATE VIEW v AS SELECT * FROM m WHERE ...;

SELECT * FROM v
-- transforms into
SELECT * FROM (SELECT * FROM m WHERE ...);
```

⚠️ View supports all optimizations may be performed on query that uses referenced view. Example:

```
CREATE MAPPING m TYPE IMap OPTIONS ('keyFormat'='int','valueFormat'='int');
CREATE VIEW v AS SELECT * FROM m WHERE __key < 20;

SELECT * FROM v WHERE __key > 10
-- transforms into
SELECT * FROM m WHERE __key > 10 AND __key < 20;
```

Optimizer eliminates one redundant projection (since they are equal) and merge bounds
check (`__key > 10 AND __key < 20`).

### Future-Proofing

We're considering possible future enhancements that we might do at some point to check, if the proposed syntax isn't at
conflict.

### Technical Design

Executing the `CREATE VIEW` statement is supposed to create a record in **information_schema.views**
service table, where view name and query text supposed be stored.

Let's review proposed grammar:

 ```
CREATE [ OR REPLACE ] VIEW name AS query
 ```

Statement parameters:

- **OR REPLACE** -- replace if view with such **name** exists.

⚠️ Since all views are stored in `ReplicatedMap`, view replacement isn't an atomic operation.

- **name** - view name.
- **query** - valid SQL query.

#### Views Storage

**information_schema.views** is another service table to store metadata for cluster views. Table should be present as
separate catalog stored in `ReplicatedMap` (similar to `MappingStorage` and `MappingCatalog`).

|     Column     |  Type   |
|----------------|---------|
| table_catalog  | VARCHAR |
| table_schema   | VARCHAR |
| table_name     | VARCHAR |
| table_name     | VARCHAR |
| view_name      | VARCHAR |
| view_query     | VARCHAR |

#### Notes/Questions/Issues

- ❓ Security: should `CREATE VIEW` share `CREATE` permission?
- ❓ Should view have any dependency on existing mapping? To clarify, consider this snippet:

```
CREATE MAPPING m TYPE IMap OPTIONS (...);

CREATE VIEW v AS SELECT * FROM m WHERE ...;

-- some other queries

DROP MAPPING m;
```

- ❓ What the strategy should we choose with view **v** since mapping **m** was removed?
- ❓ What should we do if we replace mapping with updated column types or with new column type?
- ❓ Should `CREATE VIEW` support schemas?
- ❓ TODO: More of them

Use the ⚠️ or ❓icon to indicate an outstanding issue or question, and use the ✅ or ℹ️ icon to indicate a resolved issue
or question.

### Testing Criteria

Unit tests and soak tests are enough.