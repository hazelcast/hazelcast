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
|Document Status / Completeness|IN PROGRESS|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|TBA|
|Support Engineer|TBA|
|Technical Reviewers|Viliam Durina, TBA|

### Background
#### Description

This document describes IMap index creation via SQL.
It's logical step to improve Hazelcast SQL engine dynamic configuration possibilities and enrich available SQL syntax.

```TODO: rephrase/end this section.```

Proposed grammar :
```
CREATE INDEX [ IF NOT EXISTS ] name ON mapping_name ( { column_name } )
[ TYPE ( SORTED | HASH | BITMAP ) ]
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

### Functional Design
#### Summary of Functionality

Proposed grammar:
```
CREATE INDEX [ IF NOT EXISTS ] name ON mapping_name ( { column_name } )
[ TYPE ( SORTED | HASH | BITMAP ) ]
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

Statement parameters:

- **name** - index name.
- **mapping_name** - mapping name for index creation. Mapping must have IMap type. 
Design for this property still not finished, see [discussion](#notesquestionsissues) 
- list of **column_name** - attribute(s) to be indexed. Composite indices are also supported.
- **index type** : all IMap indices are supported for CREATE INDEX statement : `SORTED`, `HASH`,  `BITMAP`.
- **options** - options are available only for BITMAP index since it has additional BitmapIndexConfig. 
Those options are supported:
  1. `unique_key`
  2. `unique_key_transformation`

In case of `SORTED`/`HASH` index, options usage causes `QueryException`.

Generally, `CREATE INDEX` query translates to `IMap#addIndex(indexConfig)` method call, 
where `indexConfig` is assembled by .  

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
```
TODO

- Questions about the change:
  - What components in Hazelcast need to change? How do they change? This section outlines the implementation strategy: for each component affected, outline how it is changed.
  - How does the change behave in mixed-version deployments? During a version upgrade? Which migrations are needed?
  - What are the possible interactions with other features or sub-systems inside Hazelcast? How does the behavior of other code change implicitly as a result of the changes outlined in the design document? (Provide examples if relevant.)
  - What are the edge cases? What are example uses or inputs that we think are uncommon but are still possible and thus need to be handled? How are these edge cases handled? Provide examples.

- Security questions:
  - 
```
The section should return to the user stories in the motivations section, and explain more fully how the detailed proposal makes those stories work.

### Testing Criteria

Unit tests and soak tests are enough.
