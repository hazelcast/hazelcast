# Nested fields access design document

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
fields inside their OBJECT mappings, by providing both SQL Backend and
Frontend with necessary type information required for validation and execution
of queries.

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
don't have one of the more specific type (we call them SQL primitive types). 

Behavior of `OBJECT`:

- supported operators: comparison operators (`=`, `<`, `>`, ...) (all use
  `compareTo`, supported only for instances the cluster can deserialize to java
  instances, doesn't work for `GenericRecord`), `IS NULL` is also supported

- can be cast to `VARCHAR` (uses `toString()` on the java instance or on
  `GenericRecord`)

- when returned to client, either Java instance or, if not available, a
  `GenericRecord` is returned. They are only supported in java client, other
  clients will fail (TODO confirm)

- they can be used for dynamic arguments

- they can be assigned (`UPDATE t SET objectField1=objectField2`)

The current status must remain unchanged, for b/w compatibility. This is ensured
that unless a type is created using `CREATE TYPE`, all non-primitive fields will
remain `OBJECT`.

## New custom types

We'll add user-defined types (UDTs). The user will need to create a type before
it can be used. He will also need to specify serialization options, in the same
way as it is specified for mappings. (Serialization options are mandatory for
now, we consider making Compact format the default in the future, but it's out
of scope here).

```sql
CREATE TYPE foo_type (
    field1 TYPE1,
    ...
) OPTIONS (
    ...
)
```

The field list is optional, if it can be extracted from the options (e.g. by
reflecting the java class, or by listing Portable fields, if the class
definition is known). On the other hand, the user can specify a new portable or
compact type by specifying the field list and providing appropriate options.

If the user provides the field list, and the field list is also known to the
cluster, it will be validated - fields can be omitted in the field list, but
those that are present must exist in the class and have a matching type.

The order of the fields matters - it is important when converting to ROW values.

The types are schema objects. They share the namespace with tables and views,
and are scoped to schemas. Currently, they can be created only in `public`
schema, as all other objects.

### No mixing of formats

Type Formats can not be mixed together. For example a `java` type can not be
used as column inside of `portable` type or mapping or vice-versa. Violations
are reported at runtime when using such mapping/type in a statement, not when
executing the DDL.

## References to types are symbolic

When creating a type, references to other custom types are only validated at
run-time. The main reason for this is the ability to create circularly-dependent
types. When creating a mapping, types are validated at mapping-creation time. 

As is the case with other schema objects, we don't track dependencies at DDL
time, mainly due to the lack of a consistent transactional metadata store, so if
the type a type or mapping depends on is modified or dropped later, it will fail
at run-time when that mapping is used.

## Behavior of custom types

All the behavior of `OBJECT` will also apply to UDTs, but additionally:

- one can access fields of the value using the DOT operator (`.`)

This has notable consequences:

- when a custom-typed value is returned to the client, the value will be the
  java instance or a `GenericRecord`. Non-java clients must use `TO_ROW`
  function.

- comparison operators (and most notably, the `=` operator) will compare the
  java instances, not the values field-by-field. Again, one can use `TO_ROW` to
  compare field-by-field. This might seem counter-intuitive, but it is the
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

## Automatic resolution

When a mapping or another type is auto-resolved, for non-primitive types we
check the SQL catalog if a type exists.

## UDTs and the top-level fields

Currently, if the top-level `__key` or `this` field of an imap isn't a SQL
primitive type, we make the field available as a hidden field, and all the
fields are expanded as top-level fields. For example, if we have `IMap<Long,
Person>`, then the mapping will have these fields:

- `__key BIGINT`
- `this OBJECT` (hidden)
- `name`, `birthDate`, ... (all fields of the `Person` class)

This behavior remains unchanged, that is, even if there is a UDT for `Person`,
`this` will have `OBJECT` type. 

## ROW values

The SQL standards specifies the ROW type. A ROW value contains an array of
values, but doesn't contain field names. It is important to note that UDT values
aren't ROW values. However, they can be converted both ways:

- `TO_ROW` function converts an UDT value to `ROW` value

- `CAST(rowExpression AS <UDT name>)`: converts a `ROW` value to a UDT value.
  The ROW value must have the same number and types of fields as the UDT, in the
  same order.

When the ROW value is returned to the client, a
`com.hazelcast.sql.impl.expression.RowValue` is returned to the java client.
Note that it is a private API class - the reason is that STRUCT types aren't
supported by JDBC. We could make this API public in our custom SQL API, but we
consider JDBC to be the primary Java API. This is also what PG and Oracle do
(TODO confirm Oracle). The user can reasonably only use `toString` on the value,
which is what JDBC UI clients do, the `toString` will contain comma-separated
list of all fields.

The `ROW` type supports comparison operators that is different from how the
comparison operator works with `OBJECT` type. It compares all field one by one,
from left to right (this is important for operators other than `=`).

The `ROW` type can also be assigned to another `ROW` type, provided they have
the same number and types of fields. This is important with `ROW` literals, they
can be used to create UDT values:

```sql
UPDATE m
SET address=CAST(('street', 'city', 12345, 'Slovakia') AS AddressType
WHERE ...;
```

### Issues with `CAST(field AS ROW)`

For some reason we weren't able to implement this (TODO ivan).

## Information schema

We need to provide these new tables:
- `USER_DEFINED_TYPES`
- `ATTRIBUTES`

We should provide `INFORMATION_SCHEMA.USER_DEFINED_TYPES` view, which is SQL
standard. I couldn't find an information schema table to list type fields.

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