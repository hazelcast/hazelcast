

### Indexing

Hazelcast distributed queries will run on each member in parallel and only results will return the conn. When a query runs on a
member, Hazelcast will iterate through the entire owned entries and find the matching ones. This can be made faster by indexing
the mostly queried fields, just like you would do for your database. Indexing will add overhead for each `write`
operation but queries will be a lot faster. If you query your map a lot, make sure to add indexes for the most frequently
queried fields. For example, if your `active and age < 30` query, make sure you add index for `active` and
`age` fields. Here is how to do it.

```java
IMap map = hazelcastInstance.getMap( "employees" );
// ordered, since we have ranged queries for this field
map.addIndex( "age", true );
// not ordered, because boolean field cannot have range
map.addIndex( "active", false );
```

`IMap.addIndex(fieldName, ordered)` is used for adding index. For each indexed field, if you have ranged queries such as `age>30`,
`age BETWEEN 40 AND 60`, then you should set the `ordered` parameter to `true`. Otherwise, set it to `false`.

Also, you can define `IMap` indexes in configuration. An example is shown below.

```xml
<map name="default">
  ...
  <indexes>
    <index ordered="false">name</index>
    <index ordered="true">age</index>
  </indexes>
</map>
```

You can also define `IMap` indexes using programmatic configuration, as in the example below.

```java
mapConfig.addMapIndexConfig( new MapIndexConfig( "name", false ) );
mapConfig.addMapIndexConfig( new MapIndexConfig( "age", true ) );
```

The following is the Spring declarative configuration for the same sample.

```xml
<hz:map name="default">
  <hz:indexes>
    <hz:index attribute="name"/>
    <hz:index attribute="age" ordered="true"/>
  </hz:indexes>
</hz:map>
```
<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Non-primitive types to be indexed should implement *`Comparable`*.*

