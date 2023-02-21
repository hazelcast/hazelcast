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