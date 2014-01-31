
### Indexing

Hazelcast distributed queries will run on each member in parallel and only results will return the conn. When a query runs on a member, Hazelcast will iterate through the entire owned entries and find the matching ones. Can we make this even faster? Yes by indexing the mostly queried fields. Just like you would do for your database. Of course, indexing will add overhead for each `write` operation but queries will be a lot faster. If you are querying your map a lot then make sure to add indexes for most frequently queried fields. So if your `active and age < 30 ` query, for example, is used a lot then make sure you add index for `active` and `age` fields. Here is how:

```java
IMap imap = Hazelcast.getMap("employees");
imap.addIndex("age", true);        // ordered, since we have ranged queries for this field
imap.addIndex("active", false);    // not ordered, because boolean field cannot have range
```
API `IMap.addIndex(fieldName, ordered)` is used for adding index. For a each indexed field, if you have -ranged- queries such as`age>30`, `age BETWEEN 40 AND 60` then `ordered` parameter should be`true`, otherwise set it to`false`.

Also you can define `IMap` indexes in configuration.

-   *Hazelcast XML configuration*

```xml
<map name="default">
    ...
    <indexes>
        <index ordered="false">name</index>
        <index ordered="true">age</index>
    </indexes>
</map>
```
-   *Config API*

```java
mapConfig.addMapIndexConfig(new MapIndexConfig("name", false));
mapConfig.addMapIndexConfig(new MapIndexConfig("age", true));
```
-   *Spring XML configuration*

```xml
<hz:map name="default">
    <hz:indexes>
        <hz:index attribute="name"/>
        <hz:index attribute="age" ordered="true"/>
    </hz:indexes>
</hz:map>
```

