---
title: IMap Connector
description: Description of the SQL IMap connector
---

The IMap connector supports reading and writing to local IMaps. Remote
IMaps are not supported yet.

Jet SQL works with IMaps through the concept of the external mapping,
just like all other resources. It provides an additional convenience: if
your IMap already stores some data, Jet can automatically detect the key
and value types by sampling an existing entry. This works only when
there's at least one entry stored in every cluster node.

From the SQL semantics perspective, there is an auto-generated mapping
for all the local IMaps in the `partitioned` schema. To query a local
IMap, you can directly execute a statement against it. If you don't name
a schema, Jet first looks up the object in the `public` schema
(explicitly created mappings) and then in the `partitioned` schema.

### External Mapping for a Java Class

Here's our Java class for a trade event:

```java
public class Trade implements Serializable {
    public String ticker;
    public BigDecimal price;
    public long amount;
}
```

We used public fields, but of course you can use private fields and use
setters/getters. This class must be available to the cluster. You can
either add it to the members class paths by creating a JAR file and
adding to the `lib` folder, or you can use User Code Deployment. The
user code deployment has to be enabled on the members; add the following
section to the `config/hazelcast.yaml` file:

```yaml
hazelcast:
  user-code-deployment:
    enabled: true
```

Then use a client to upload the class:

```java
ClientConfig clientConfig = new JetClientConfig();
clientConfig.getUserCodeDeploymentConfig()
            .setEnabled(true)
            .addClass(Trade.class);
JetInstance jet = Jet.newJetClient(clientConfig);
```

After this, you can create the mapping for the IMap. The name of the
IMap is `latest_trades`:

```sql
CREATE MAPPING latest_trades
TYPE IMap
OPTIONS (
    'keyFormat' = 'varchar',
    'valueFormat' = 'java',
    'valueJavaClass' = 'com.example.Trade'
)
```

We didn't provide an explicit column list, so Jet determines it
automatically from the provided OPTIONS. Our key is a simple `varchar`
with no internal structure, so we get the default name for the key
column: `__key`. Value is an object whose fields become columns of the
mapping. So we get this structure:

```sql
(
    __key VARCHAR,
    ticker VARCHAR,
    price DECIMAL,
    amount BIGINT
)
```

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
