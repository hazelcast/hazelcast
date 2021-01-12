---
title: IMap Connector
description: Description of the SQL IMap connector
---

The IMap connector supports reading and writing to local IMaps. Remote
IMaps are not supported yet.

There is an auto-generated mapping for all the local IMaps in the
`partitioned` schema. To query a local IMap, you can directly execute a
statement against it. If you don't name a schema, Jet first looks up the
object in the `public` schema (explicitly created mappings) and then in
the `partitioned` schema.

IMap itself is schema-less, however SQL assumes a schema. We assume all
entries in the map are of the same type (with some exceptions). IMap
also supports several serialization options, see below.

If you don't create a mapping for an IMap explicitly, we'll sample an
arbitrary record to derive the schema and serialization type. You need
to have at least one record on each cluster member for this to work. If
you can't ensure this, create the mapping explicitly.

## Serialization Options

The `keyFormat` and `valueFormat` options are mandatory. Currently, if
you create the mapping explicitly, we can't resolve these from a sample.

Possible values for `keyFormat` and `valueFormat`:

* `portable`
* `json`
* `java`

The key and value formats can be different.

### `Portable` Serialization

For this format, you need to specify additional options:

* `keyPortableFactoryId`, `valuePortableFactoryId`
* `keyPortableClassId`, `valuePortableClassId`
* `keyPortableVersion`, `valuePortableVersion`: optional, default is `0`

Jet resolves the column list by looking at the `ClassDefinition` found
using the given factory ID, class ID, and version.

The benefit of this format is that it doesn't deserialize the whole key
or value when reading only a subset of fields. Also it doesn't require a
custom Java class to be defined on the cluster, so it's usable for
non-Java clients.

Example mapping where both key and value are `Portable`:

```sql
CREATE MAPPING my_map
TYPE IMap
OPTIONS (
    'keyFormat' = 'portable',
    'keyPortableFactoryId' = '123',
    'keyPortableClassId' = '456',
    'keyPortableVersion' = '0',  -- optional
    'valueFormat' = 'portable',
    'valuePortableFactoryId' = '123',
    'valuePortableClassId' = '789',
    'valuePortableVersion' = '0',  -- optional
)
```

For more information on `Portable` see Hazelcast IMDG Reference Manual.

### JSON Serialization

You don't have to provide any options for the JSON format, but since
Jet can't automatically determine the column list, you must explicitly
specify it:

```sql
CREATE MAPPING my_map(
    __key BIGINT,
    ticker VARCHAR,
    amount INT)
TYPE IMap
OPTIONS (
    'keyFormat' = 'json',
    'valueFormat' = 'json')
```

JSON's type system doesn't match SQL's exactly. For example, JSON
numbers have unlimited precision, but such numbers are typically not
portable. We convert SQL integer and floating-point types into JSON
numbers. We convert the `DECIMAL` type, as well as all temporal types,
to JSON strings.

We don't support the JSON type from the SQL standard. That means you
can't use functions like `JSON_VALUE` or `JSON_QUERY`. If your JSON
documents don't all have the same fields, the usage is limited.

Internally, we store all JSON values in the string form.

### Java Serialization

Java serialization is the last-resort serialization option. It uses the
Java object exactly as `IMap.get()` returns it. You can use it for
objects serialized using the Java serialization or Hazelcast custom
serialization (`DataSerializable` or `IdentifiedDataSerializable`).

For this format you must specify the class name using `keyJavaClass` and
`valueJavaClass` options, for example:

```sql
CREATE MAPPING my_map
TYPE IMap
OPTIONS (
    'keyFormat' = 'java',
    'keyJavaClass' = 'java.lang.Long',
    'valueFormat' = 'java',
    'valueJavaClass' = 'com.example.Person')
```

If the Java class corresponds to one of the basic data types (numbers,
dates, strings), that type will directly be used for the key or value
and mapped as a column named `__key` for keys and `this` for values. In
the example above, the key will be mapped under the `BIGINT` type.

If the Java class is not one of the basic types, Hazelcast will analyze
the class using reflection and use its properties as column names. It
recognizes public fields and JavaBeans-style getters. If some property
has a non-primitive type, it will be mapped under the `OBJECT` type.

## External Column Name

You rarely need to specify the columns in DDL. If you do, you might need
to specify the external name for the column.

The entries in a map naturally have _key_ and _value_ elements. Because
of this, the format of the external name must be either `__key.<name>`
for a field in the key or `this.<name>` for a field in the value.

The external name defaults to `this.<columnName>`, so normally you only
need to specify it for key fields. There are also columns that represent
the entire key and value objects, called `__key` and `this`.

## Heterogeneous Maps

If you have values of varying types in your map, you have to specify the
columns in the `CREATE MAPPING` command. You may specify columns that
don't exist in some values. If a property doesn't exist in a specific
value instance, the corresponding column value will be NULL.

For example, let's say you have this in an IMap:

|key|value|
|-|-|
|1|`{"name":"Alice","age":42}`|
|2|`{"name":"Bob","age":43,"petName":"Zaz"}`|

If you map the column `petName`, it will have the value `null` for the
entry with `key=1`.

The objects in the IMap may even use different serialization strategies
without breaking SQL. We'll try to extract the field by name regardless
of the actual serialization format. The serialization you specify in the
mapping is needed only when inserting into that map.

## Remote IMap

Remote IMaps are not yet supported.
