---
title: 022 - Low code / no code MongoDB connector
description: Describes the design of low/no code connector for MongoDB 
---

MongoDB is a widely used NoSQL, document-based database. Although there
was a Jet sink and source connector implemented in 
the `hazelcast-jet-contrib` repository, users didn't really have 
properly tested and supported connector for this database.

Such connector should:
 - have minimal API available to quickly prototype and more advanced
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


## Implementation details

### Source and sink
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

User may want to read from many collections in one source. In such
situation it's questionable if each processor should deal with one 
collection or split it's work across collections. In the first iteration
each processor will deal with one collection, Processor Supplier will 
create processors on each node per collection.

### SQL Connector
The SQL Connector should support:
 - read (selects)
 - inserts
 - deletes
 - updates
 - upserts

Defining schema is not trivial (Mongo is schema-less theoretically).
Possible solutions:
 - query for some documents in the collection (e.g. 10 first) and 
   read returned objects' schemas
 - provide schema manually by user
 - provide schema class - a Java class, that will represent elements
    in the collection
 - read `$jsonSchema` validator 
   (`db.getCollectionInfos( { name: "myCollection" } )[0].options.validator`)
   and use it to create a schema or read one element and check if schema 
   is available (`db.runCommand({listCollections: 1, filter:{ name: "students" }})`).


## Implementation parts

The implementation will be split into 3 parts:

**PR#1**: Basic source and sink for MongoDB

**PR#2**: The SQL connector

**PR#3**: Support for GenericMapStore
