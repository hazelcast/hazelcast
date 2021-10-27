# CREATE INDEX statement

### Table of Contents

+ [Background](#background)
  - [Description](#description)
+ [Functional Design](#functional-design)
  * [Summary of Functionality](#summary-of-functionality)
  * [Notes/Questions/Issues](#notesquestionsissues)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)

|ℹ️ Since: 5.1|
|-------------|

|||
|---|---|
|Related Jira|[HZ-566](https://hazelcast.atlassian.net/browse/HZ-566)|
|Related Github issues|_-_|
|Document Status / Completeness|DRAFT|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|TBA|
|Support Engineer|TBA|
|Technical Reviewers|Viliam Durina, TBA|

### Background
#### Description

This document describes IMap index creation via SQL.
It's a logical step to improve Hazelcast SQL engine dynamic configuration possibilities and enrich available SQL syntax.

### Functional Design
#### Summary of Functionality

`CREATE INDEX` query performs IMap index creation.
⚠ : only IMap index creation supported.

Proposed grammar:
```
CREATE INDEX [ IF NOT EXISTS ] name ON mapping_name ( { column_name } )
[ TYPE ( SORTED | HASH | BITMAP ) ]
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

Generally, `CREATE INDEX` query translates to `IMap#addIndex(indexConfig)` method call.  

##### Notes/Questions/Issues

- ❓ Should we depend on mapping name or map name in index creation query?
In current state index creation depends on mapping name.
  1. **Pros** **of mapping name** usage : 
     1. Consistent and clear UX: mappings are explicitly defined, they are visible to the user.
     2. Security permissions sharing. 
     3. 
  2. **Cons**:
     1. Additional action required : user should create mapping to create an index.
     2. Mapping are shared also for other connectors type, where sources don't support queries.
     4. 
  3. **Pros** of **map name** usage are opposite to mapping name usage: 
     1. Simplicity: user just launch the SQL CLI and type query. Bingo.
     2. Other connectors sources doesn't support indices, the only possible index creation target is IMap.
  4. **Cons**:
     1. Unclear UX: user don't know if map exists or not, they blindly create an index without any confirmation.
     2. Security - no permissions involved.


[TODO]: <> (@viliam, please, add your thoughts.)

- ❓ State of json indexes?
- ❓ What is permissible index names scope? 
- ⚠ CREATE INDEX does support for BITMAP index, when scans are not supported for this index type. 

Use the ⚠️ or ❓icon to indicate an outstanding issue or question, and use the ✅ or ℹ️ icon to indicate a resolved issue or question.


### Technical Design

Let's review proposed grammar:
```
CREATE INDEX [ IF NOT EXISTS ] name ON mapping_name ( { column_name } )
[ TYPE ( SORTED | HASH | BITMAP ) ]
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

Statement parameters:

- **IF NOT EXISTS** -- index creation call would be performed only if index is not exists.
- **name** - index name.
- **mapping_name** - mapping name for index creation. Mapping must have IMap type.
  Design for this property still not finished, see [discussion](#notesquestionsissues)
- list of **column_name** - attribute(s) to be indexed. Composite indices are also supported.
- **index_type**: all IMap indices are supported for CREATE INDEX statement : `SORTED`, `HASH`,  `BITMAP`.
- **options** - options are available only for BITMAP index since it has additional `BitmapIndexConfig`.
  Those options are supported:
    1. `unique_key`
    2. `unique_key_transformation`

In case of `SORTED`/`HASH` index, options usage causes `QueryException`.

Then, SQL engine collects  provided parameters `indexConfig` and perform 
`IMap#addIndex(new IndexConfig(index_type, { column_name })).setName(name)` method call.

⚠ : SQL engine doesn't support `BITMAP` index scans, but does support `BITMAP` index creation.

- Security questions:
    - ❓ Should we use mapping name? If yes, ->
    - ❓ Should we use different permissions for create mapping and create index?
    
### Testing Criteria

Unit tests and soak tests are enough.
