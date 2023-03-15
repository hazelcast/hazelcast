# MongoDB Connector

A Hazelcast Jet connector for MongoDB which enables Hazelcast Jet pipelines to 
read/write data points from/to MongoDB.

## Connector Attributes

### Source Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Source  |  Yes  |
| Batch       |  Yes  |
| Stream      |  Yes  |
| Distributed |  No   |

### Sink Attributes
|  Attribute  | Value |
|:-----------:|-------|
| Has Sink    |  Yes  |
| Distributed |  Yes  |

## Getting Started

### Installing

The MongoDB Connector artifacts are published on the Maven repositories. 

Add the following lines to your pom.xml to include it as a dependency to your
project:

```
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-mongodb</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet', name: 'hazelcast-jet-mongodb', version: ${version}
```

## Usage

### As a Batch Source

MongoDB batch source (`MongoSources.mongodb()`)  executes the 
query and emits the results as they arrive.

Here's an example which queries documents in a collection having the field 
`age` with a value greater than `10` and applies a projection so that only
the `age` field is returned in the emitted document.

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

For more detail check out 
[MongoSources](src/main/java/com/hazelcast/jet/mongodb/MongoSources.java),
[MongoSourceBuilder](src/main/java/com/hazelcast/jet/mongodb/MongoSourceBuilder.java)
and 
[MongoSourceTest](src/test/java/com/hazelcast/jet/contrib/mongodb/MongoSourceTest.java).

### As a Stream Source

MongoDB stream source (`MongoSources.streamMongodb()`) watches the changes to
documents in a collection and emits these changes as they arrive. Source uses 
( `ChangeStreamDocument.getClusterTime()` ) as native timestamp.

Change stream is available for replica sets and sharded clusters that use 
WiredTiger storage engine and replica set protocol version 1 (pv1). Change streams
can also be used on deployments which employ MongoDB's encryption-at-rest feature.
Without enabling change streams, the source will not work. 
See [MongoDB Change Streams](https://docs.mongodb.com/manual/changeStreams/) for
more information. 

You can watch the changes on a single collection,
on all the collections in a single database or on all collections across all
databases. You cannot watch on system collections and collections in admin,
local and config databases.

Following is an example pipeline which watches changes on `myCollection`.
Source filters the changes so that only `insert`s which has the `val` field
greater than or equal to `10` will be fetched, applies the projection so that
only the `val` and `_id` fields are returned.

Here's an example which streams inserts on a collection having the field `age`
with a value greater than `10` and applies a projection so that only the `age`
field is returned in the emitted document.

```java
StreamSource<? extends Document> streamSource =
        MongoSources.stream(
                "stream-source",
                "mongodb://127.0.0.1:27017",
                "myDatabase",
                "myCollection",
                new Document("fullDocument.age", new Document("$gt", 10))
                        .append("operationType", "insert"),
                new Document("fullDocument.age", 1)
        );
Pipeline p = Pipeline.create();
StreamSourceStage<? extends Document> srcStage = p.readFrom(streamSource);
```

For more detail check out 
[MongoSources](src/main/java/com/hazelcast/jet/mongodb/MongoSources.java),
[MongoSourceBuilder](src/main/java/com/hazelcast/jet/mongodb/MongoSourceBuilder.java)
and 
[MongoSourceTest](src/test/java/com/hazelcast/jet/contrib/mongodb/MongoSourceTest.java).


### As a Sink

MongoDB sink (`MongoSinks.mongodb()`) is used to write documents from 
Hazelcast Jet Pipeline to MongoDB. 

Following is an example pipeline which reads out items from Hazelcast
List, maps them to `Document` instances and writes them to MongoDB.

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

For more detail check out 
[MongoSinks](src/main/java/com/hazelcast/jet/mongodb/MongoSinks.java),
[MongoSinkBuilder](src/main/java/com/hazelcast/jet/mongodb/MongoSinkBuilder.java)
and 
[MongoSinkTest](src/test/java/com/hazelcast/jet/contrib/mongodb/MongoSinkTest.java).

## Fault Tolerance

MongoDB stream source saves the resume-token of the last emitted item as a 
state to the snapshot. In case of a job restarted, source will resume from the
resume-token.  

## Running the tests

To run the tests run the command below: 

```
mvn test
```

## Authors

* **[Ali Gurbuz](https://github.com/gurbuzali)**
