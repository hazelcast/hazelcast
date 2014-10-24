

### Indexing

Hazelcast distributed queries will run on each member in parallel and only results will return the conn. When a query runs on a
member, Hazelcast will iterate through the entire owned entries and find the matching ones. This can be made faster by indexing
the mostly queried fields. Just like you would do for your database. Of course, indexing will add overhead for each `write`
operation but queries will be a lot faster. If you are querying your map a lot, make sure to add indexes for most frequently
queried fields. So, if your `active and age < 30` query, for example, is used a lot, make sure you add index for `active` and
`age` fields. Here is how:

```java
IMap map = hazelcastInstance.getMap( "employees" );
// ordered, since we have ranged queries for this field
map.addIndex( "age", true );
// not ordered, because boolean field cannot have range
map.addIndex( "active", false );
```

`IMap.addIndex(fieldName, ordered)` is used for adding index. For each indexed field, if you have ranged queries such as `age>30`,
`age BETWEEN 40 AND 60`, then `ordered` parameter should be `true`. Otherwise, set it to `false`.

Also, you can define `IMap` indexes in configuration, a sample of which is shown below.

```xml
<map name="default">
  ...
  <indexes>
    <index ordered="false">name</index>
    <index ordered="true">age</index>
  </indexes>
</map>
```

This sample in programmatic configuration looks like below.

```java
mapConfig.addMapIndexConfig( new MapIndexConfig( "name", false ) );
mapConfig.addMapIndexConfig( new MapIndexConfig( "age", true ) );
```

And, the following is the Spring declarative configuration for the same sample.

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

