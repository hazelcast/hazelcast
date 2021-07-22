---
title: Apache Kafka Connector
description: Description of the SQL Apache Kafka connector
---

The Apache Kafka connector supports reading and writing to Apache Kafka
topics.

Apache Kafka is schema-less, however SQL assumes a schema. We assume all
messages in a topic are of the same type (with some exceptions). Kafka
also supports several serialization options, see below.

## Serialization Options

To work with Kafka, you must specify the `keyFormat` and `valueFormat`
options. Currently, even if you create a table mapping explicitly, we
can't resolve these. These are the supported values for `keyFormat` and
`valueFormat`:

* any of the supported [SQL data
  types](https://docs.hazelcast.org/docs/{imdg-minor-version}/manual/html-single/index.html#data-types),
  except for `OBJECT`
* `avro`
* `json`
* `java`

The key and value format can be different. Any options not recognized by
Jet are passed directly to the Kafka producer or consumer. See the
examples for individual serialization options below.

### Primitive serialization

If the format is one of the primitive type names, then the Java class
representing that type will be stored in the IMap. By _primitive type_
we mean any [supported SQL data
type](https://docs.hazelcast.org/docs/{imdg-minor-version}/manual/html-single/index.html#data-types),
except for `OBJECT`.

For example, if the topic key is an `Integer` and the topic message is a
`String`, use:

```sql
CREATE MAPPING my_topic
TYPE Kafka
OPTIONS (
    'keyFormat'='int',
    'valueFormat'='varchar',
    'bootstrap.servers' = '127.0.0.1:9092'
)
```

Note that you don't have to provide `key.serializer`,
`key.deserializer`, `value.serializer` and `value.deserializer` kafka
options, which are otherwise required - in the above case the following
will be automatically added:

```sql
'key.serializer' = 'org.apache.kafka.common.serialization.IntegerSerializer',
'key.deserializer' = 'org.apache.kafka.common.serialization.IntegerDeserializer',
'value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
'value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer'
```

### Avro Serialization

When using Avro, Jet reads the fields from the `GenericRecord` returned
by the `KafkaAvroDeserializer`. When inserting to a topic, we create an
ad-hoc Avro schema named `jet.sql` from the mapping columns. Because of
this, the column list is required. Jet currently can't use your custom
Avro schema to create objects, but it can use it to read objects written
through our ad-hoc schema, as long as the field names and types match.

#### Mapping Between SQL and Avro Types

| SQL Type | Avro Type |
| - | - |
| `TINYINT`<br/>`SMALLINT`<br/>`INT` | `INT` |
| `BIGINT` | `LONG` |
| `REAL` | `FLOAT` |
| `DOUBLE` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `VARCHAR`<br/>and all other types | `STRING` |

All Avro types are a union of the `NULL` type and the actual type.

```sql
CREATE MAPPING my_topic (
    __key VARCHAR,
    ticker VARCHAR,
    amount BIGINT,
    price DECIMAL
)
TYPE Kafka
OPTIONS (
    'keyFormat' = 'varchar',
    'valueFormat' = 'avro',
    'bootstrap.servers' = '127.0.0.1:9092',
    'schema.registry.url' = 'http://127.0.0.1:8081/'
    /* more Kafka options ... */
)
```

In this example, the key is a plain `Long` number, the value is
avro-serialized. `keyFormat` and `valueFormat` options are handled by
Jet, the rest is passed directly to Kafka producer or consumer.

We omitted the kafka serialization options, in this case
`io.confluent.kafka.serializers.KafkaAvroSerializer` and
`io.confluent.kafka.serializers.KafkaAvroDeserializer` are automatically
added.

### JSON Serialization

The value will be stored as a JSON object. Jet can't automatically
determine the column list for this format, you must explicitly specify
it:

```sql
CREATE MAPPING my_topic(
    __key BIGINT,
    ticker VARCHAR,
    amount INT)
TYPE Kafka
OPTIONS (
    'keyFormat' = 'bigint',
    'valueFormat' = 'json',
    'bootstrap.servers' = '127.0.0.1:9092')
```

There are no additional options for this format.

JSON's type system doesn't match SQL's exactly. For example, JSON
numbers have unlimited precision, but such numbers are typically not
portable. We convert SQL integer and floating-point types into JSON
numbers. We convert the `DECIMAL` type, as well as all temporal types,
to JSON strings.

We don't support the `JSON` type from the SQL standard yet. That means
you can't use functions like `JSON_VALUE` or `JSON_QUERY`. If your JSON
documents don't all have the same fields or if they contain nested
objects, the usability is limited.

### Java Serialization

Java serialization is the last-resort serialization option. It uses the
Java objects exactly as `KafkaConsumer.poll()` returns them. You can use
it for objects serialized using the Java serialization or any other
serialization method.

For this format you must specify the class name using `keyJavaClass` and
`valueJavaClass` options, for example:

```sql
CREATE MAPPING my_topic
TYPE Kafka
OPTIONS (
    'keyFormat' = 'java',
    'keyJavaClass' = 'java.lang.Long',
    'valueFormat' = 'java',
    'valueJavaClass' = 'com.example.Person',
    'value.serializer' = 'com.example.serialization.PersonSerializer',
    'value.deserializer' = 'com.example.serialization.PersonDeserializer',
    'bootstrap.servers' = '127.0.0.1:9092')
```

If the Java class corresponds to one of the basic data types (numbers,
dates, strings), that type will directly be used for the key or value
and mapped as a column named `__key` for keys and `this` for values. In
the example above, the key will be mapped with the `BIGINT` type. In
fact, the above `keyFormat` & `keyJavaClass` duo is equivalent to
`'keyFormat'='bigint'`.

If the Java class is not one of the basic types, Hazelcast will analyze
the class using reflection and use its properties as column names. It
recognizes public fields and JavaBean-style getters. If some property
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

## Heterogeneous Messages

For example, let's say you have these messages in your topic:

```json
{"name":"Alice","age":42}
{"name":"Bob","age":43,"petName":"Zaz"}
```

If you map the column `petName`, it will have the value `null` for the
entry with `key=1`. This scenario is supported. Similar behavior works
with Avro format.

## Installation

You need the `hazelcast-jet-kafka` module on your classpath. For
Gradle or Maven, make sure to add the dependency:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-kafka:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-kafka</artifactId>
    <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

If you're using the distribution package make sure use the fat one,
because the `hazelcast-jet-kafka-{jet-version}.jar` you need is not
contained in the slim one.
