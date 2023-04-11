# User Defined Types design document

|||
|---|---|
|Related Jira|[HZ-1121](https://hazelcast.atlassian.net/browse/HZ-1121)|
|Document Status / Completeness|DRAFT|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Ivan Yaschishin|
|Quality Engineer||
|Technical Reviewers|Viliam Durina|

# Background

The Predicate API, which SQL is aiming to replace, allowed access to nested
fields of the keys or values. SQL so far lacks this feature.

Hazelcast is most commonly used by Java shops, which tend to store complex
objects. This is contrary to traditional RDBMS, where denormalized structures
are typically used. This made the feature very useful for our user base.

User Defined Types are meant to provide users with ability to query nested
fields within their OBJECT fields, by providing both backend and frontend with
necessary type information required for validation and execution of queries.

The original motivation was to support queries like `SELECT
(organization).office.name FROM users` where `organization` would be an OBJECT
field itself (e.g. a Java class or a nested GenericRecord) with its own
columns/fields (here and thereafter Column and Field are used interchangeably).
This was also expanded to include ability to perform UPDATE/INSERT queries as
well as support for Portable and Compact MAPPINGs in addition to Java ones.

## Terminology

|Term|Definition|
|---|---|
|||
|UDT|**User Defined Types**, also known as Custom Types, also previously known as Nested Type and Nested Fields Types|
|CRec Type| Cyclically Recurrent UDTs, UDT type hierarchies with type or instance-level cycles|
|Nested Field|A column (field) inside of mapped OBJECT column (field)|
|Nested Type| In this document, synonymous to UDT|

## Goals

The feature should provide:

- using nested fields to filter rows (WHERE clause and JOIN conditions)
- using nested fields in the results and for aggregation
- updating fields of complex objects in the UPDATE statement
- inserting complex objects by providing values to all fields

## Non-goals

- Provide access to polymorphic data (i.e. a value has multiple subclasses, the
  user will be able to use the various fields of different subclasses)

## Structured vs. non-structured data

We already support non-structured JSON data using the JSON family of functions.
In this effort we're aiming at structured data, that is those, whose schema is
known in advance. That's why we'll need the custom types to be created before
they can be used.

# Functional Design

## Current status

SQL currently supports the `OBJECT` type. This type is used for all fields that
don't have one of the more specific types (we call them SQL primitive types).

Behavior of `OBJECT`:

- supported operators: comparison operators (`=`, `<`, `>`, ...) (all use
  `compareTo`, supported only for instances the cluster can deserialize to java
  instances, doesn't work for `GenericRecord`), `IS NULL` is also supported

- can be cast to `VARCHAR` (uses `toString()` on the java instance or on
  `GenericRecord`)

- when returned to client, either Java instance or, if not available, a
  `GenericRecord` is returned. They are only supported in java client, other
  clients will fail (TODO confirm)

- they can be used for dynamic arguments from the Java client

- they can be assigned (`UPDATE t SET objectField1=objectField2`)

The current status must remain unchanged, for b/w compatibility. This is ensured
that unless a type is created using `CREATE TYPE`, all non-primitive fields will
remain `OBJECT`.

## New custom types

We'll add user-defined types (UDTs). The user will need to create a type before
it can be used. He will also need to specify serialization options, in the same
way as it is specified for mappings.

```sql
CREATE TYPE foo_type (
    field1 TYPE1,
    ...
) OPTIONS (
    ...
)
```

The serialization options should in all aspects behave the same as in mappings
(TODO use the same code):

- The field list is optional, if it can be extracted from the options (e.g. by
  reflecting the java class, or by listing Portable fields, if the class
  definition is known), in the same way as in mappings.

- The user can specify a new portable or compact type by specifying the field
  list and providing appropriate options

- If the user provides the field list, and the field list is also known to the
  cluster, it will be validated - fields can be omitted in the field list, but
  those that are present must exist in the class and have a matching type.

- The order of the fields matters - it is important when converting to ROW
  values.

The types are schema objects. They share the namespace with mappings and views,
and are scoped to schemas. Currently, they can be created only in `public`
schema, as all other objects.

### No mixing of formats

Type formats can not be mixed together. For example a `java` type can not be
used as column inside of `portable` type or mapping or vice-versa. Violations
are reported at runtime when using such mapping/type in a statement, not when
executing the DDL. (TODO test)

## References to types are symbolic

When creating a type, references to other custom types are only validated at
run-time. The main reason for this is the ability to create circularly-dependent
types. When creating a mapping, types are validated at mapping-creation time. 

As is the case with other schema objects, we don't track dependencies at DDL
time, mainly due to the lack of a consistent transactional metadata store, so if
the type a type or mapping depends on is modified or dropped later, it will not
fail when the type is dropped, but at run-time when that type is about to be
used and is missing. Additionally cyclic types are meant to be supported for Java
format.

## Behavior of custom types

All the behavior of `OBJECT` will also apply to UDTs, but additionally:

- one can access fields of the value using the DOT operator (`.`)

This has notable consequences:

- when a custom-typed value is returned to the client, the value will be the
  java instance or a `GenericRecord`. Non-java clients must either query individual primitive fields
  or alternatively convert the object into JSON with `CAST(udtObject AS JSON)` operator,
  as an java object can't be deserialized by a non-java client.

- comparison operators (and most notably, the `=` operator) will compare the
  java instances, not the values field-by-field. To compared field by field users can manually
  compare each field or alternatively convert the object into JSON with `CAST(udtObject AS JSON)`
  operator and compare that. This might seem counter-intuitive, but it is the
  limitation already present: if the imap's `__key` field is compared, we compare
  the instances and not the individual fields, if they are expanded.

## Case sensitivity

Unquoted custom type names, same as all other schema objects, are
case-sensitive. This is contrary to built-in types, whose names are
case-insensitive. This might seem illogical, but is consistent with the rest of
our SQL implementation where identifiers are case-sensitive, but keywords (which
include built-in functions and types) are not.

```sql
CREATE TYPE My_Type ...;

CREATE MAPPING m (
  field my_type  -- fails here
) ...
```

Here we need to note that identifiers in SQL are always case-sensitive, however,
many SQL implementations convert unquoted identifiers to canonical case, which
gives the impression that identifiers are case-insensitive. Quoted identifiers
are always case-sensitive. The conversion to canonical case is optional in the
SQL standard, Hazelcast doesn't do it for legacy reasons.

## UDTs and the top-level fields

Currently, if the top-level `__key` or `this` field of an imap isn't a SQL
primitive type, we make the field available as a hidden field, and all the
fields are expanded as top-level fields. For example, if we have `IMap<Long,
Person>`, then the mapping will have these fields:

- `__key BIGINT`
- `this OBJECT` (hidden)
- `name`, `birthDate`, ... (all fields of the `Person` class)

This behavior remains unchanged, that is, even if there is a UDT for `Person`,
`this` will have `OBJECT` type. In this case, the top-level OBJECT fields cannot
be assigned in an INSERT/UPDATE stmt (TODO test this).

The user can explicitly map the complex key or value to a UDT:

```sql
CREATE TYPE Person_Type AS (name VARCHAR, birthdate ...);

CREATE MAPPING foo_map (
  __key BIGINT,
  this Person_Type
)
TYPE IMap
OPTIONS (...)
```

In this case the Person's field cannot be mapped. I.e. mapping `this` as
`Person_Type`, and also mapping `name` will throw an error (TODO test).

## ROW values

The SQL standards specifies the ROW type. A ROW value contains an array of
values, but doesn't contain field names. It is important to note that UDT values
aren't ROW values. However, ROW values are implicitly converted into target UDT on INSERT/UPDATE:

```sql
CREATE TYPE UserType (id BIGINT, username VARCHAR) OPTIONS ('format'='compact', 'compactTypeName'='UserType');
CREATE MAPPING tMap TYPE IMap (__key BIGINT, entryId BIGINT, user UserType)
OPTIONS ('keyFormat'='bigint', 'valueFormat'='compact', 'valueCompactTypeName'='obj');

INSERT INTO tMap VALUES (1, 100, (101, 'testUserName'));
```

In the example above ROW value `(101, 'testUserName')` is implicitly converted into UDT during INSERT operation.
Note that this mechanism does not support cyclic structures as ROW type can not support cycles in current implementation.

Additionally its possible to manually construct ROW values inside of SELECT, but

### Semantics of comparison operators (not currently supported)

Comparison operators are: `=`, `!=`, `<`, `>`, `<=`, `>=`. `IS [NOT] DISTINCT
FROM` isn't currently supported.

When one operand of a comparison operator is ROW, the other operand must:
- have a ROW type
- have the same number of fields

For individual fields of the ROW, we apply normal rules for comparison operator
recursively. That is, `(value1, value2) = (value3, value4)` is equivalent to
`value1 = value3 AND value2 = value4`, including the type conversion this
involves.

For example: 

```
(INT, INT) <cmp> (BIGINT, INT)   # ok, INT converted to BIGINT
(INT, INT) <cmp> OBJECT  # reject ROW and OBJECT comparison
(INT) <cmp> (VARCHAR) # ok, VARCHAR onverted to INT
(INT) <cmp> (INT, INT)  # reject - different number of fields
(INT, (INT, BIGINT)) <cmp> (TINYINT, (BIGINT, INT))  # ok, same rules for nested ROW
```

The comparison will be done from left to right. For evaluation, initial equal
fields are ignored, and the first non-equal field determines the result. If all
fields are equal, any field determines the result.

### Issues with `CAST(field AS ROW)`

Due to current parser limitations its not possible to implement `CAST(field AS ROW)` because the specification for
`ROW` type requires providing full list of columns e.g. `CAST(field AS ROW(BIGINT, VARCHAR, ROW(BIGINT, VARCHAR)))`
which is not very practical. It might be possible to override this behavior, but it most likely requires extensive
changes to the parser/validation as wel.

## DML operations

### INSERT/SINK

For inserting with SQL literals, one can use ROW values:

```sql
INSERT INTO foo_map(__key, udtField) VALUES(1, ('value1', 'value2'))
```

It is not possible to use nested fields in the insert command

```sql
INSERT INTO foo_map(__key, udtField.fieldA, udtField.fieldB)
VALUES (1, 'value1', 'value2');
--> ERROR
```

### UPDATE

When updating, it is possible to set the whole field using a ROW literal, or to
set subfields directly:

```sql
UPDATE foo_map
SET udtField=('value1', 'value2');

UPDATE foo_map
SET udtField.fieldA='value1', udtField.fieldB='value2';
```

It is not possible to assign to a UDT and to its fields at the same time:

```sql
UPDATE ... SET udtField=('value1', 'value2'), udtField.fieldA='value3'; 
--> ERROR
```

## Information schema

We need to provide these new tables:
- `USER_DEFINED_TYPES`
- `ATTRIBUTES`

We should provide `INFORMATION_SCHEMA.USER_DEFINED_TYPES` view, which is SQL
standard.

# Implementation details

## CREATE/DROP TYPE statement

Syntax:
```sql
CREATE [OR REPLACE] TYPE [IF NOT EXISTS] <typeName> [<columnList>] 
OPTIONS ('format'='<typeFormat>',
  <...other options relevant for the chosen format...>)

<columnList>:
(
    columnName ColumnType,
    [<... more columns ...>]
)

DROP TYPE [IF EXISTS] <typeName>
```

## Catalog storage

Types are stored in `__sql.catalog` map, along with mappings and views. This
ensures name uniqueness.

As with other SQL metadata, we don't enforce mutual dependencies. For example, a
type can be dropped even if it is used in some mapping - when this happens, the
mapping then becomes invalid. (TODO test this)

## Circular dependencies

Because types are validated when used in a statement, it is possible to create
mutually (or circularly) dependent types.

We distinguish circular dependencies in types and in instances.

### Circular dependencies in types

This happens when from type `A` exists a field path that that is also of type
`A`. Most simple example is a self-reference, such as:

```sql
CREATE TYPE GraphNode(connectedNodes GraphNode[]) 
```

Another example is a chain: `A` has a field of type `B`, `B` has a field of type
`C` and `C` has a field of type `A`; then if the type of field `f` is `A`, then
property path `f.b.c.a` is also of type `A`.

It is possible to create such types in SQL because the types are validated only
when used in a statement, so it's possible to create a type that has a field
with type that doesn't yet exist.

Such types pose the risk of infinite recursion if the code follows the type
path, without checking for types that have already been seen by the recursion.
In fact, one such issue within Calcite codebase prevented us from importing DML
that involves such types.

### Circular dependencies in instances

A circular dependency in instances happens when from instance `a` there exists a
property path that refers to the same instance. It is not possible to create
such values through SQL, because it is not possible to obtain a reference to a
value that can be assigned elsewhere; values are always copied by value.
However, it is possible to encounter such data that was created using one of the
clients, and the implementation has to deal with it.

The `TO_ROW` function raises an error when it encounters such value. We could
possibly refer to the same nested `RowValue` at this conversion, but for
example, the conversion to `VARCHAR` of such a value would be infinite because
in it we cannot refer to another nested value in this way.

## How values are stored

Each type must have one of supported serialization types: java, portable,
compact, json, avro, ... Initial support might not support all types.

For Java custom types, the cluster needs access to the Java class of the custom
type. When reading, writing or evaluating operators, we need to deserialize the
value.

This is unlike other types, where the cluster can work with generic records

In the initial release we will support only: java, portable, compact.