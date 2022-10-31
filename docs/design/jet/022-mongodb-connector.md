# Low code / no code MongoDB connector

### Table of Contents

+ [Background](#background)
   - [Description](#description)
   - [Terminology](#terminology)
   - [Actors and Scenarios](#actors-and-scenarios)
+ [Functional Design](#functional-design)
   * [Summary of Functionality](#summary-of-the-functionality)
   * [Additional Functional Design Topics](#additional-functional-design-topics)
      + [Notes/Questions/Issues](#notesquestionsissues)
+ [User Interaction](#user-interaction)
   - [API design and/or Prototypes](#api-design-andor-prototypes)
+ [Client Related Changes](#client-related-changes)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)
+ [Other Artifacts](#other-artifacts)


|                                |                                                  |
|--------------------------------|--------------------------------------------------|
| Related Jira                   | _https://hazelcast.atlassian.net/browse/HZ-1508_ |
| Related Github issues          | _None_                                           |
| Document Status / Completeness | DRAFT                                            |
| Requirement owner              | _TBD_                                            |
| Developer(s)                   | _Tomasz Gawęda_                                  |
| Quality Engineer               | _TBD_                                            |
| Support Engineer               | _TBD_                                            |
| Technical Reviewers            | _Frantisek Hartman_                              |
| Simulator or Soak Test PR(s)   | _TBD_                                            |

## Background

### Terminology
| Term          | Definition                                                                                                           |
|---------------|----------------------------------------------------------------------------------------------------------------------|
 | Jet connector | A set of classes that can be used as a Hazelcast Jet's source and/or sink.                                           | 
 | SQL Connector | A set of classes (one implementing SqlConnector) that will add functionality to use given source/sink in SQL queries |

### Description
MongoDB is a widely used NoSQL, document-based database. Although there
was a Jet sink and source connector implemented in 
the `hazelcast-jet-contrib` repository, users didn't really have 
properly tested and supported connector for this database.


## Goals
Create connector that:
 - has minimal API available to quickly prototype and more advanced
   one for those who need many customizations
 - support parallelism where possible**
 - support encryption for data-in-transit
 - support SQL <-> MongoDB
 - support no/low code `MapStore` feature
 - be tested with integration and soak tests

## Non-goals

It's not a goal of this connector to control MongoDB settings; any 
changes in MongoDB configuration or any administration action will still 
be needed to be done on MongoDB side.

## Actors and Secnarios
TBD

## Functional design

### Summary of the Functionality

- Jet source and sink to MongoDB.
- SQL Connector that will allow to use MongoDB as `type` in SQL mappings.
- Support for GenericMapStore - low/no code write-behind to MongoDB.

### Additional Functional Design Topics
#### Notes/Questions/Issues

- ⚠ Schema inference will probably have a constraint, that user must provide all values,
  `null` in case of lack of values.

Use the ⚠️ or ❓icon to indicate an outstanding issue or question, and use the ✅ or ℹ️ icon to indicate a resolved issue or question.

### User Interaction

#### Source/sink

Example usage of the source:
```
BatchSource<Document> batchSource =
        MongoDBSources.batch(
                "batch-source",
                "mongodb://127.0.0.1:27017",
                "myDatabase",
                "myCollection",
                new Document("age", new Document("$gt", 10)),
                new Document("age", 1)
        );
Pipeline p = Pipeline.create();
BatchStage<Document> srcStage = p.readFrom(batchSource);
```


This example queries documents in a collection having the field
`age` with a value greater than `10` and applies a projection so that only
the `age` field is returned in the emitted document. User should be also
able to add e.g. `MyUser.class` as the last argument, which will mean
that all returned objects will be automatically mapped to Java POJO
[see MongoDB docs](https://www.mongodb.com/developer/languages/java/java-mapping-pojos/)

Example usage of sink:
```
Pipeline p = Pipeline.create();
p.readFrom(Sources.list(list))
 .map(i -> new Document("key", i))
 .writeTo(
     MongoDBSinks.mongodb(
        "sink", 
        "mongodb://localhost:27017",
        "myDatabase",
        "myCollection"
     )
 );
```

#### SQL Connector
User should be able to run following SQL query:
```sql
CREATE MAPPING people
TYPE MONGODB 
OPTIONS (
  'externalDataStoreRef'='mongodb-database' 
)
```

In such cases, automatic schema inference will be used. User may also want
to provide schema explicitly:

```sql
CREATE MAPPING people (
    firstName VARCHAR(100),
    lastName VARCHAR(100),
    age INT
)
TYPE MONGODB 
OPTIONS (
  'externalDataStoreRef'='mongodb-database' 
)
```

#### GenericMapStore support

User will be able to configure map store:
```
Config config = new Config();
config.addExternalDataStoreConfig(
        new ExternalDataStoreConfig("mongodb-ref")
            .setClassName(MongoDbDataStoreFactory.class.getName())
            .setProperty("url", dbConnectionUrl)
  );

MapConfig mapConfig = new MapConfig(mapName);
MapStoreConfig mapStoreConfig = new MapStoreConfig();
mapStoreConfig.setClassName(GenericMapStore.class.getName());
mapStoreConfig.setProperty(OPTION_EXTERNAL_DATASTORE_REF, "mongodb-ref");
mapConfig.setMapStoreConfig(mapStoreConfig);
instance().getConfig().addMapConfig(mapConfig);
```

### Technical Design

#### Source and sink
The `hazelcast-jet-contrib` repository contained MongoDB connector. It was
based on the `SourceBuilder`, which is nice in general, but for more 
flexibility we need to rework this connector to low-level Processor/
ProcessorSupplier/ProcessorMetaSupplier. 

We can check if we could use MongoDB's Replica sets. If yes, then
our MetaSupplier can spawn as many ProcessorSuppliers as MongoDB has 
replicas and configure Mongo client to use this specific replica. 

Each MongoDB source processor can read just a part of collection. Two main
approaches for reading a part of collections are:
 - slice `__id` range to as many elements as there will be processors
   and make each processor read this slice
 - use `__id mod processorGlobalIndex` to determine which element in 
   collection will be read by which processor. 
 - For a sharded MongoDB cluster the first approach is preferable, as
   shard keys are naturally divided into chunks. 
   [Docs on sharding](https://www.mongodb.com/docs/manual/reference/command/listShards/#mongodb-dbcommand-dbcmd.listShards)

User may want to read from many collections in one source. In such
situation it's questionable if each processor should deal with one 
collection or split it's work across collections. In the first iteration
each processor will deal with one collection, Processor Supplier will 
create processors on each node per collection.

#### SQL Connector
The SQL Connector should support:
 - read (selects)
 - inserts
 - deletes
 - updates
 - upserts

Defining schema is not trivial (Mongo is schema-less theoretically).
Possible solutions:
 - query for some documents in the collection (e.g. first one) and 
   read returned objects' fields and infer types.
 - provide schema manually by user.
   - read `$jsonSchema` validator 
     (`db.getCollectionInfos( { name: "myCollection" } )[0].options.validator`)
     and use it to create a schema or read one element and check if schema 
     is available (`db.runCommand({listCollections: 1, filter:{ name: "students" }})`).
     [MongoDB's docs on this topic](https://www.mongodb.com/docs/manual/core/schema-validation/specify-query-expression-rules/#std-label-schema-validation-query-expression)
     [and second documentation page](https://www.mongodb.com/docs/manual/core/schema-validation/view-existing-validation-rules/).


### Implementation parts

The implementation will be split into 3 parts:

**PR#1**: Basic source and sink for MongoDB

**PR#2**: The SQL connector

**PR#3**: Support for GenericMapStore
