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

`CREATE INDEX` statement creates an IMap index
⚠: only IMap index creation supported, index removal isn't implemented in IMDG.

Proposed grammar:
```
CREATE INDEX [ IF NOT EXISTS ] name ON mapping_name ( { column_name } )
[ TYPE ( SORTED | HASH | BITMAP ) ]
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

Generally, `CREATE INDEX` query translates to `IMap#addIndex(indexConfig)` method call.  

##### Notes/Questions/Issues

- ❓ Should the index be created based on the mapping name, or based on the IMap name?
  1. **Advantages of using the mapping name:**
     1. Simpler UX: the user uses the column names as defined in the mapping, not the column external names.
     2. Security permissions sharing. ?? Sasha add details
     3. 
  2**Advantages of using the IMap name:**
     1. There are much less edge cases that we can get wrong
     2. Better matches the physical reality of IMaps: 
        1. No translation of index attribute names is needed
        2. If the user creates an index for mapping and drops the mapping, index is not dropped
        3. If the user creates an index for a JSON field and the format in the mapping is native JSON, the user needs to use `JSON_VALUE` to use that index
     3. No need to create mapping first before creating the index. Useful if SQL is used as a configuration tool. This point is also an instance of a "better matching to the physical reality"
     4. We'll not suffer from similar issues if we in the future support index creation for other connectors.

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
- **index_type**: all IMap indices are supported for CREATE INDEX statement: `SORTED`, `HASH`,  `BITMAP`.
- **options** - options are available only for BITMAP index since it has additional `BitmapIndexConfig`.
  Those options are supported:
    1. `unique_key`
    2. `unique_key_transformation`

In case of `SORTED`/`HASH` index, options usage causes `QueryException`.

Then, SQL engine collects  provided parameters `indexConfig` and perform 
`IMap#addIndex(new IndexConfig(index_type, { column_name })).setName(name)` method call.

⚠: SQL engine doesn't support `BITMAP` index scans, but does support `BITMAP` index creation.

- Security questions:
    - ❓ Should we use mapping name? If yes, ->
    - ❓ Should we use different permissions for create mapping and create index?
    
### Testing Criteria

Unit tests and soak tests are enough.
