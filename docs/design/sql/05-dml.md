# SQL DML

## Overview
In SQL, the Data Manipulation Language comprises the SQL-data change statements which modify stored data.
Typically, we can distinguish three classes of commands:
- **CREATE** which creates new record(s) (`INSERT`)
- **UPDATE** which updates record(s) fields (`UPDATE`, `MERGE`)
- **DELETE** which deletes record(s) (`DELETE`)

## 1. INSERT
To create new record(s) one of two variations of `INSERT` statement can be used:
- `INSERT INTO` - will append to the target table. If it’s not possible due to unique constraint violation, it will
  fail. It’s a safe choice if the target never overwrites (such as Kafka, JMS or a JDBC that use a sequence for the
  primary key or has no primary key)
- `INSERT OVERWRITE` - will allow overwrites. It will behave as a sequence of `DELETE` followed by an `INSERT`,
  performed atomically. Specifically, the fields not listed in the insert statement will not retain their values, but
  will be assigned `null` or default values. If `INSERT OVERWRITE` is used for a target that doesn’t overwrite by
  design (like Kafka), it will behave exactly as `INSERT INTO`. No error will be thrown. The delete in the imaginary
  `DELETE` action will delete no rows.

### 1.1 Serialization
In addition to primitive SQL types (which can be mapped to Java types and stored directly), two serialization formats
are supported:
- POJO (reflective access to public properties and fields) - requires Java class on the classpath and no-arg
  constructor
- Portable (supported just for IMDG data structures) - if `InMemoryFormat` is set to `BINARY` it requires just
  `ClassDefinition` to be registered upfront, if `InMemoryFormat` is set to `OBJECT` it requires also Portable factory
  and Portable class itself on the classpath. Currently, temporal types as well as `BigInteger` & `BigDecimal` are not
  supported.

Serialization formats have following precedence (the order in which we check whether given serialization can be
applied):
1. primitive SQL types
2. POJO
3. Portable

Eventual errors from type mismatch between declared and actual types are deferred to statement execution.

The way the data is serialized is expressed via DDL. Depending on the nature of stored objects that information can be
encoded either in column names or using `OPTIONS` clause. To avoid name clashes, so called _external_ tables are
created in dedicated schema named `public`.

#### 1.1.1 Key-Value storage
The serialization formats of key and value are specified separately. To determine whether given field belongs to key or
value either Java class or Portable `ClassDefinition` is inspected. In case of a name clash between fields of key and
value composite objects, the ones from key take precedence and are actually written.

To define a table where both key and value are of primitive SQL types, `__key` & `this` as column names should be used.
For example:
```
CREATE EXTERNAL TABLE name (
  __key INT,
  this VARCHAR
) TYPE "com.hazelcast.LocalPartitionedMap"
```

Given:
```
class PersonId {
    public int id;
}

class Person {
    public String name;
}
```
To define a table where both key and value are POJOs and respectively of `PersonId` & `Person` class, `keyClass` &
`valueClass` should be used. For example:
```
CREATE EXTERNAL TABLE name (
  id INT,
  name VARCHAR
) TYPE "com.hazelcast.LocalPartitionedMap"
OPTIONS (keyClass 'PersonId', valueClass 'Person')
```

Given:
```
ClassDefinition keyClassDefinition = new ClassDefinitionBuilder(1, 2, 3)
    .addIntField("id")
    .build();

ClassDefinition valueClassDefinition = new ClassDefinitionBuilder(4, 5, 6)
    .addUTFField("name")
    .build();
```
To define a table where both key and value are Portables and map to respective `ClassDefinition`s, `keyFactoryId` +
`keyClassId` + `KeyClassVersion` & `valueFactoryId` + `valueClassId` + `valueClassVersion` should be used. For example:
```
CREATE EXTERNAL TABLE name (
  id INT,
  name VARCHAR
) TYPE "com.hazelcast.LocalPartitionedMap"
OPTIONS (
  keyFactoryId '1',
  keyClassId '2',
  keyClassVersion '3',
  valueFactoryId '4',
  valueClassId '5',
  valueClassVersion '6'
)
```

The only difference between an IMap and Kafka table declaration is the used `TYPE` (and additional Kafka specific
options). So, to declare a table referencing a Kafka topic:
```
CREATE EXTERNAL TABLE name (
  __key INT,
  this VARCHAR
) TYPE "com.hazelcast.Kafka"
OPTIONS (
  -- connection options
)
```