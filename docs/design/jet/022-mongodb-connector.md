# Low code / no code MongoDB connector

### Table of Contents

+ [Background](#background)
   - [Description](#description)
   - [Terminology](#terminology)
+ [Functional Design](#functional-design)
   * [Summary of Functionality](#summary-of-the-functionality)
   * [Additional Functional Design Topics](#additional-functional-design-topics)
      + [Notes/Questions/Issues](#notesquestionsissues)
+ [User Interaction](#user-interaction)
+ [Technical Design](#technical-design)


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
 - support parallelism where possible
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
```java
BatchSource<Document> batchSource =
        MongoSources.batch(
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
```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.list(list))
 .map(i -> new Document("key", i))
 .writeTo(
     MongoSinks.mongodb(
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
DATA CONNECTION "mongodb-ref"
```

In such cases, automatic schema inference will be used. User may also want
to provide schema explicitly:

```sql
CREATE MAPPING people (
    firstName VARCHAR(100),
    lastName VARCHAR(100),
    age INT
)
DATA CONNECTION "mongodb-ref"
```

#### GenericMapStore support

User will be able to configure map store:
```java
Config config = new Config();
config.addDataConnectionConfig(
        new DataConnectionConfig("mongodb-ref")
            .setType("MongoDB")
            .setProperty("connectionString", dbConnectionUrl)
  );

MapConfig mapConfig = new MapConfig(mapName);
MapStoreConfig mapStoreConfig = new MapStoreConfig();
mapStoreConfig.setClassName(GenericMapStore.class.getName());
mapStoreConfig.setProperty(OPTION_DATA_CONNECTION_REF, "mongodb-ref");
mapConfig.setMapStoreConfig(mapStoreConfig);
instance().getConfig().addMapConfig(mapConfig);
```

### Technical Design

#### Source and sink
The `hazelcast-jet-contrib` repository contained MongoDB connector. It was
based on the `SourceBuilder`, which is nice in general, but for more 
flexibility we need to rework this connector to low-level Processor/
ProcessorSupplier/ProcessorMetaSupplier:
- `SqlConnector` class' methods are working on DAG level, so it will be easier
  to reuse processors
- custom MetaSupplier will make it easier to distribute workload based on
  MongoDB's replica sets.

We can check if we could use MongoDB's Replica sets. If yes, then
our MetaSupplier can spawn as many ProcessorSuppliers as MongoDB has 
replicas and configure Mongo client to use this specific replica. 

Each MongoDB source processor can read just a part of collection. Two main
approaches for reading a part of collections are:
 - slice `__id` ([see Document's docs](https://www.mongodb.com/docs/manual/core/document/)) 
   range to as many elements as there will be processors
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

Source and sink should take [read](https://www.mongodb.com/docs/manual/reference/read-concern/) 
and [write](https://www.mongodb.com/docs/manual/reference/write-concern/) concerns into consideration.

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

All the methods can be combined and used in following order:
 - manual schema definition
 - reading `$jsonSchema`
 - reading some documents

Each of steps will be invoked only if previous failed.

It should be also possible to push the predicates down to Mongo if possible.
For example, if user invokes ``select name from people where age > 18``,
the `age > 18` predicate should be - if possible - changed to Mongo's predicate
and `name` should become the projection.

### Implementation parts

The implementation will be split into 4 parts:

**PR#1**: Import old connector to core repository

**PR#2**: Source and sink for MongoDB

**PR#3**: The SQL connector

**PR#4**: Support for GenericMapStore (incl. mapping derivation)

### Implementation questions


#### Questions about the change:

  - What components in Hazelcast need to change? How do they change?
    * N/A

  - How does this work in an on-prem deployment?
    * N/A
  
  - Are there new abstractions introduced by the change? New concepts? If yes, provide definitions and examples.
    * It is configured and used on code side, so deployment changes needed.
    
  - How about on AWS and Kubernetes, platform operator?
    * Same as above, as long as necessary jars are on the classpath it doesn't have deployment requirements.
    
  - How does the change behave in mixed-version deployments? During a version upgrade? Which migrations are needed?
    * Processors won't be strictly backward-compatible. They will follow Jet backward compatibility.
    
  - How does this work in Cloud Viridan clusters?
    * Same as above, it's code-configured.
    
  - How does the change behave in mixed-version deployments? During a version upgrade? Which migrations are needed?
    * Support for hot updates is not planned. All nodes should have the same version of processors.
    
  - What are the possible interactions with other features or sub-systems inside Hazelcast? How does the behavior of other code change implicitly as a result of the changes outlined in the design document?
    * This feature will use GenericMapStore and Data Connections, as mentioned in the examples above.
    
  - What are the edge cases? What are example uses or inputs that we think are uncommon but are still possible and thus need to be handled? How are these edge cases handled?
    * User may have documents of very different schemas in one collection. In such cases the connector should not fail - for inserts
      it should just put new documents, for reading it should use `null` for missing fields and omit all additional fields.
    
  - What are the effect of possible mistakes by other Hazelcast team members trying to use the feature in their own code? How does the change impact how they will troubleshoot things?
    * It won't be reusable in other context. Troubleshooting will be the same as any other source/sink/connector for Jet. 
    
  - Mention alternatives, risks and assumptions. Why is this design the best in the space of possible designs? What other designs have been considered and what is the rationale for not choosing them?
    
    * Other alternatives:
      - leave implementation to users (bad: requires code duplication, no official support is also not good)
      - keep connector in `hazelcast-jet-contrib` repository - but it's not as well tested as main repo and less visible
    There is also a debatable change, that source and sink will now be a processor implementation, not using
      `SourceBuilder`. However, as mentioned in [Technical design](#Source-and-sink), it's a preferred way to 
      create a SQL connector.
      
  - Add links to any similar functionalities by other vendors, similarities and differentiators
    * [Mongo Flink](https://github.com/mongo-flink/mongo-flink)

#### Questions about performance:

  - Does the change impact performance? How?
    * Won't affect performance of other subsystems
  - How is resource usage affected for “large” loads? For example, what do we expect to happen when there are 100000 items/entries? 100000 data structures? 1000000 concurrent operations?
    * Reading will be done in parallel to distribute the load ([see source and sink](#source-and-sink)).
  - Also investigate the consequences of the proposed change on performance. Pay especially attention to the risk that introducing a possible performance improvement in one area can slow down another area in an unexpected way. Examine all the current "consumers" of the code path you are proposing to change and consider whether the performance of any of them may be negatively impacted by the proposed change. List all these consequences as possible drawbacks.
    * N/A

#### Stability questions:

  - Can this new functionality affect the stability of a node or the entire cluster? How does the behavior of a node or a cluster degrade if there is an error in the implementation?
    * It can affect stability if too much data is ingested and there's not enough memory.
  - Can the new functionality be disabled? Can a user opt out? How? Can the user disable it from the Management Center?
    * Yes, user can just avoid using it.
  - Can the new functionality affect clusters which are not explicitly using it?
    * No
  - What testing and safe guards are being put in place to protect against unexpected problems?
    * There will be a soak test to verify stability on high load.
    * source/sink will be non-cooperative, so should not starve other threads.

#### Security questions:
 
  - Does the change concern authentication or authorization logic? If so, mention this explicitly tag the relevant security-minded reviewer as reviewer to the design document.
    * N/A  
  - Does the change create a new way to communicate data over the network?  What rules are in place to ensure that this cannot be used by a malicious user to extract confidential data?
    * It will connect to MongoDB server. Encryption for data in transit will be implemented.
  - Is there telemetry or crash reporting? What mechanisms are used to ensure no sensitive data is accidentally exposed?
    * Same as any Jet processors.

#### Observability and usage questions:

- Is the change affecting asynchronous / background subsystems?
  * Should not
- Which other inspection APIs exist?
  * Standard Jet job status APIs.

- Are there new APIs, or API changes (either internal or external)?
  * Yes, new Source and Sink, along with new SQL connector type.
- How would you document the new APIs? Include example usage.
  * Documentation in Javadocs
  * How-to guide in documentation.
- What are the other components or teams that need to know about the new APIs and changes?
  * SQL Team should be aware of new connector type.
- Which principles did you apply to ensure the APIs are consistent with other related features / APIs? (Cross-reference other APIs that are similar or related, for comparison.)
  * Processors will be done in similar manner to other extensions' processors.

- Is the change visible to users of Hazelcast or operators who run Hazelcast clusters?
  * No if not used.
- Are there any user experience (UX) changes needed as a result of this change?
  * No
- Are the UX changes necessary or clearly beneficial? (Cross-reference the motivation section.)
  * No
- Which principles did you apply to ensure the user experience (UX) is consistent with other related features? (Cross-reference other features that have related UX, for comparison.)
  * Use same structure as other connectors.
- Which other engineers or teams have you polled for input on the proposed UX changes? Which engineers or team may have relevant experience to provide feedback on UX?
- Is usage of the new feature observable in telemetry? If so, mention where in the code telemetry counters or metrics would be added.
  * TBD - based on MongoDB metrics.
- What might be the valuable metrics that could be shown for this feature in Management Center and/or Viridan Control Plane?
  * No special metrics; does standard for Jet jobs would be enough.
- Should this feature be configured, enabled/disabled or managed from the Management Center? How do you think your change affects Management Center?
  * No, it's  code-only feature.
- Does the feature require or allow runtime changes to the member configuration (XML/YAML/programmatic)?
  * No
- Are usage statistics for this feature reported in Phone Home? If not, why?
  * Nothing planned as for now.
