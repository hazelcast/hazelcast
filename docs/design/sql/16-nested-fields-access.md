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

## References to types are symbolic

- validated at run-time (also in mappings?)
- allows creation of circular dependencies in types
- doesn't require transactions on the SQL catalog for implementation - lazily evaluated

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

## SQL standards

I actually didn't consult the standard, but the implementation in major databases.

# Implementation details

## Catalog storage

## Circular dependencies

### Circular dependencies in types

### Circular dependencies in instances
