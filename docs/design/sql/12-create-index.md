# CREATE INDEX statement

|ℹ️ Since: 5.1|
|-------------|

|||
|---|---|
|Related Jira|[HZ-566](https://hazelcast.atlassian.net/browse/HZ-566)|
|Related Github issues|_-_|
|Document Status / Completeness|IN REVIEW|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|TBA|
|Support Engineer|TBA|
|Technical Reviewers|Viliam Durina, TBA|

## Background

### Description

This document describes IMap index creation via SQL. It's a step to improve Hazelcast SQL engine dynamic configuration
possibilities and enrich available SQL syntax.

## Functional Design

### Summary of Functionality

`CREATE INDEX` statement creates an IMap index.

⚠: only creation index supported for now, index removal isn't implemented in IMDG.

Proposed syntax:

```
CREATE INDEX [ IF NOT EXISTS ] index_name 
ON imap_name ( attribute_name [, ...] )
[ TYPE ( SORTED | HASH | BITMAP ) ]
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

```
DROP INDEX [ IF EXISTS ] index_name
ON imap_name
```

Generally, `CREATE INDEX` query translates to
`IMap#addIndex(indexConfig)` method call.

The `attribute_name` is the name as IMDG index API defines it. It might not be exactly the same as SQL maps it, because
there's no link to SQL mapping. For example, if the attribute refers to an array, the index contains the array elements
as individual index entries, all referring to the containing entry. On the other hand, in SQL, arrays are not currently
supported at all.

The created index doesn't become an object in the catalog. It cannot have schema specified, it directly refers to the
IMap. The `index_name`
is scoped to the map, that's why we need the unusual `ON` clause also in the `DROP INDEX` command.

The indexes will not be visible in the `information_schema` for now.

### Future Changes

#### Indexes on different connectors

We expect that we'll need to create indexes for `ReplicatedMap`. For this we envision adding `CONNECTOR` clause
after `ON`:

```
CREATE INDEX index_name
ON imap_name ( attribute_name, ...)
[ CONNECTOR IMap ]
```

The clause will be optional, the `IMap` connector will be the default. The `CONNECTOR` clause will need to be added also
to the `DROP INDEX`
command. We're not implementing this for now.

#### Function-based indexes

In SQL world, function-based indexes (FBIs) are indexes where the indexed value is a result of an SQL expression. Common
use case is case-insensitive search where instead of the original value,
`UPPER(column)` is indexed. Later when doing the search we do `WHERE UPPER(column)=UPPER(?)`.

IMap doesn't support this, but it supports derived attributes which can be used to a similar end. A derived attribute
can be specified in two ways:

- for java-serialized and `DataSerializable` objects, the user can add a getter that will return the derived value.
  Later, instead of using the original field, the derived field will be used to do the search.

- specify an extractor: this works for all serialization types, including Portable and Compact serialization

SQL engine already supports the 1st approach because custom getters are already mapped as fields. We plan to also map
extractors and add ability to use the index, if these columns appear in queries.

The implementation is not a true FBI where the SQL expression appears in the index definition and in the queries.

### Rejected Alternatives

We considered creating the indexes also based on the mapping name. The idea was rejected for these reasons:

- the index lifecycle will be unclear: the index is created for a mapping, but if the mapping is dropped, the index
  remains

- there will be a dependency between the mapping and the index, which we cannot correctly implement due to the lack of
  transactions in the catalog storage. Also, if the mapping is altered, the index might need to be rebuilt (e.g. when a
  field type is changed from INT to VARCHAR, the index should be rebuilt), or we should prevent the alteration of the
  mapping.

- the index created for a mapping will not be usable for other mappings of the same imap, or through the old predicate
  API

- it will blur the idea that mapping is just a lightweight reference to actual data object

In the future we might add index support based on mappings. It can be incorporated in the syntax in several ways:

```
CREATE INDEX index_name
ON MAPPING mapping_name ...
```

```
CREATE INDEX index_name
ON mapping_name(...)
CONNECTOR mapping ...
```

```
CREATE MAPPING INDEX index_name
ON mapping_name ...
```

### Technical Design

Let's review proposed grammar:

```
CREATE INDEX [ IF NOT EXISTS ] index_name 
ON imap_name ( attribute_name [, ...] )
[ TYPE index_type ]
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

Statement parameters:

- **IF NOT EXISTS** -- if `addIndex` throws an exception because the index already exists, this exception will be
  ignored.
- list of **column_name** - attribute(s) to be indexed. Composite indices are also supported.
- **index_type**: all IMap indices are supported for CREATE INDEX statement: `SORTED`, `HASH`,  `BITMAP`. `SORTED` is
  the default.
- **options** - options are available only for BITMAP index since it has additional `BitmapIndexConfig`. Those options
  are supported:
    1. `unique_key`
    2. `unique_key_transformation`

Unknown options will cause an error to be thrown.

Then, SQL engine collects provided parameters into an `IndexConfig` and
performs `IMap#addIndex(new IndexConfig(index_type, { column_name })).setName(name)` method call.

⚠: SQL engine doesn't support `BITMAP` index scans, but does support
`BITMAP` index creation.

#### Backwards Compatibility

There feature is only an API to existing implementations. Older clients will seamlessly support it. In mixed-version
clusters, if the command lands at 5.0 members, they will throw syntax exception (something like
`unexpected token: INDEX` after the `CREATE` keyword). If the command lands at a 5.1 member, even 5.0 members will be
able to use it. No special handling of versions is needed.

#### Permissions

The index creation should be protected by `ACTION_INDEX` permission given to the target IMap.

### Testing Criteria

Unit tests and soak tests are enough.
