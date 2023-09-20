Type mapping in MongoDB SQL Connector.
=====

The type system in MongoDB and SQL is not exactly the same. That
leads to potential confusions and the need of type coercion. 

Type mapping
----

| BSON Type           | SQL Type                | Java type                      |
|---------------------|-------------------------|--------------------------------|
| double              | double                  | double                         |
| string              | varchar                 | String                         |
| object              | object                  | org.bson.Document              |
| array               | object                  | List                           |
| binData             | -                       | -                              |
| undefined           | -                       | -                              |
| ObjectId            | object                  | org.bson.ObjectId              |
| bool                | boolean                 | boolean                        |
| date                | date_time* or timestamp | LocalDateTime*                 |
| timestamp           | date_time or timestamp  | LocalDateTime                  |
| null                |                         |                                |
| regex               | OBJECT                  | org.bson.BsonRegularExpression |
| dbPointer           | -                       | -                              |
| javascript          | varchar                 | String                         |
| javascriptWithScope | object                  | org.bson.CodeWithScope         |
| symbol              | -                       | -                              |
| int (32 bit)        | int                     | int                            |
| long (64 bit)       | bigint                  | long                           |
| decimal (128 bit)   | decimal                 | BigDecimal                     |
| minKey              | object                  | org.bson.MinKey                |
| maxKey              | object                  | org.bson.MaxKey                |


`*` - Type `date` in BSON represents seconds from Unix epoch in UTC timezone. 
Therefore it's not mapped to pure `DATE` SQL type nor `LocalDate` 
in Java (nor any formats with timezones).

The "Java Type" column represents an object returned by SQL query
if the object put into the collection is of given BSON type.

Bare in mind, that, while we are able to convert MongoDB type to requested SQL type
in the projection, the argument binding will not always work the same 
due to technical limitations. 

For example: you can have object with type `timestamp` represented as `DATE_TIME`, 
that in SELECT will give you LocalDateTime. However, binding LocalDateTime
as an argument won't work, as only native MongoDB types will work for arguments.
Same for e.g. having BSON column of type "string" mapped to "integer" in SQL.

Type coercion
-------------

Below is the table with possible and supported type coercions.

All the default mappings from previous chapter are always valid.


| Type of provided argument        | Resolved insertion type |
|----------------------------------|-------------------------|
| LocalDateTime                    | BsonDateTime            |
| OffsetDateTime                   | BsonDateTime            |
| HazelcastJsonValue (json column) | Document                |