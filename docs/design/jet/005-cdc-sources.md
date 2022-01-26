---
title: 005 - Change Data Capture (CDC) Sources
description: Change Data Capture (CDC) Sources
---

*Since*: 4.2

## Goal

Make Jet's CDC sources production ready: consistent with each other in
behaviour, easy to use, reliable and performant.

## Background

In Jet 4.0 we have introduced [support for CDC sources](/blog/2020/03/02/jet-40-is-released#debezium)
based on [Debezium](https://debezium.io/).

That form however is not production ready:

* it's **inconvenient** to use: gives you huge JSON messages which you
  then have to interpret on your own, based on Debezium documentation

* native **timestamps** aren't provided (due to bugs in the code); even
  if fixed their shortcomings aren't documented (mostly that timestamps
  in events coming from database snapshots aren't really event-times)

* **performance** impact on the DBs is not explored at all, recommended
  deployment guide is not provided

* miscellaneous **shortcomings** and **inconsistencies** of the various
  connectors are not documented

These are the problems we are trying to address for the next version(s).

## Parsing Convenience

We want to help users not to have to deal with low level JSON messages
directly. In order to achieve this we:

* **flatten** the original Debezium event's complex structure using the
  [Kafka Connect
  SMT](https://docs.confluent.io/current/connect/transforms/index.html)
  they themselves provide (see [Record State
  Extraction](https://debezium.io/documentation/reference/1.1/configuration/event-flattening.html))
* define standard interfaces which offer **event structure**
* offer support for mapping various event component directly into
  **POJO data objects** or to simply extract individual from them

### Record Structure

The interfaces framing the general structure of change events (we call
them "records") are quite simple:

```java
public interface ChangeRecord {

    long timestamp() throws ParsingException;

    Operation operation() throws ParsingException;

    RecordPart key();

    RecordPart value();

    String toJson();
}
```

```java
public interface RecordPart {

    <T> T toObject(Class<T> clazz) throws ParsingException;

    Map<String, Object> toMap() throws ParsingException;

    String toJson();

}
```

### Content Extraction

As can be noticed from the [record structure
interfaces](#record-structure), most content can be found in the form of
`RecordPart` instances.

Such an object is basically an encapsulated JSON message (part of the
original, big message) and offers two main methods to access the
data:

* mapping the entire content to a **POJO** directly
* parsing the JSON into an easy to work with **map** form

More information on both will be presented in the [JSON
Parsing](#json-parsing) section.

### Fallback

Considering the complexity of the original JSON messages and even their
varying formats it is clear that parsing them can be error prone and
can fail in some scenarios (for example on some untested DB-connector
version combination).

To prevent a total API failure in this case each of the record structure
interfaces contains an extra `toJson()` method, which will provide the
raw JSON message the object is based on:

* `ChangeRecord.toJson()` gives you the original message from the
  connector (although already flattened), without anything having been
  done to it; if all else fails this will still work
* `RecordPart.toJson()` gives you the JSON message of that particular
  fragment

There is also a second type of fallback. Among our [generic Debezium
sources](/javadoc/{jet-version}/com/hazelcast/jet/cdc/DebeziumCdcSources.html)
we also provide a version, called `debeziumJson`, which streams data
as it comes in from Debezium, without doing any processing on it. This
bypasses our convenience API completely.

### JSON parsing

Debezium database connectors provide messages in standard JSON format
which can be parsed by any one of the well-known JSON parsing libraries
out there. For backing the implementations of our [record structure
interfaces](#record-structure) we have chosen to use [Jackson
jr](https://github.com/FasterXML/jackson-jr) with [annotation
support](https://github.com/FasterXML/jackson-jr/tree/master/jr-annotation-support).

The consequence of this for object mapping is that the classes we intend
to map to need to define Bean style accessors. Further details in the
[Jackson jr documentation](https://github.com/FasterXML/jackson-jr). The
[annotation
support](https://github.com/FasterXML/jackson-jr/tree/master/jr-annotation-support)
can simplify things. Accessors can be replaced with `@JsonProperty`
annotations, Bean fields can be named differently then JSON ones and so
on.

For example a `Customer` class to which we could map JSON like

```json
"id":1004,"first_name":"Anne Marie","last_name":"Kretchmar","email":"annek@noanswer.org"
```

could be defined as follows:

```java
import com.fasterxml.jackson.annotation.JsonProperty;

public class Customer {

    @JsonProperty("id")
    public int id;

    @JsonProperty("first_name")
    public String firstName;

    @JsonProperty("last_name")
    public String lastName;

    @JsonProperty("email")
    public String email;

    public Customer() {
    }

    // also needs: hashCode, equals, toString
}
```

Once all these things are in place it becomes quite easy to turn the
Debezium stream of raw, complicated JSON messages into a stream of
simple data objects:

```java
StreamStage<Customer> stage = pipeline.readFrom(source)
    .withNativeTimestamps(0)
    .map(record -> {
        RecordPart value = record.value();
        return value.toObject(Customer.class);
    });
```

## Source Configuration

Debezium CDC connectors have a ton of configuration options and we don't
want to ask users to go to the Debezium website to explore them.

We need to provide two things. A way to **discover** and **easily use**
the config options and a way to still allow any properties to be set or
overwritten, even if for some reason we forgot or failed to handle them.

For this we are going to use builders:

```java
StreamSource<ChangeRecord> source = MySqlCdcSources.mysql(tableName)
    //set any of the essential properties we cover
    .setDatabaseAddress(mysql.getContainerIpAddress())
    .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
    .setDatabaseUser("debezium")
    .setDatabasePassword("dbz")
    .setClusterName("dbserver1")
    .setDatabaseWhitelist("inventory")
    .setTableWhitelist("inventory." + tableName)

    //set arbitrary custom property
    .setCustomProperty("database.server.id", "5401")

    //check the properties that have been set
    //construct the source
    .build();
```

Such public builders can:

* hardcode some properties we rely on
* explain what various properties do
* check rules like properties that always need to be set or can't be
  used together

## Inconsistencies

### Timestamps aren't always event times

The current version of the sources does fix timestamps, because they
haven't been working before, but beyond that timestamps have a big
weakness, which we need to at least document.

The timestamps we normally get from these sources are proper event
times, sourced from the database change-log. The problem is with fault
tolerance. If the connector needs to be restarted it will need to
re-fetch some change records (that happened after the last Jet
snapshot).

It can happen in such cases, that not all these events are in the quite
limited database changelog. In such cases the connector uses a snapshot
of the database, which merges many past events into a single, current
image. Not only do events get lost for good, the timestamps in the
snapshot ones aren't event times any longer.

Luckily change events that come from snapshots can be simply identified.
They are basically insert records and their `Operation` field will not
only reflect that, but use a special type of insert value, called `SYNC`
for them. Except some, see [further inconsistencies](#snapshots-are-not-marked-in-mysql).

This problem can be mitigated for example by longer DB changelogs. They
probably aren't that big of a deal in most practical situations, but
still, they can be and we need to make that clear in our documentation.

### Snapshots are not marked in MySQL

As discussed in the [section about
timestamps](#timestamps-arent-always-event-times) there is a need to
identify change records that originate in database snapshots (as opposed
to database changelogs).

This is almost always possible, because such records will have `SYNC` as
their operation, except one case, MySQL which does not mark them the
same way. We could fix this in our implementation, but the fix might not
work as expected in some messages we fail to consider, causing their
parsing to fail. We don't want to do more harm then good, so for now
we'll just document this inconsistency.

## Dependencies

Regardless what convenience we add, our CDC connectors ultimately remain
Debezium based which means lots of dependencies that need to be
satisfied.

CDC sources in Jet are set up as extension modules:

* [cdc-debezium](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-debezium):
  contains all the helper classes we have defined and also allows for
  the creation of generic Debezium sources (see
  [DebeziumCdcSources](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-debezium/src/main/java/com/hazelcast/jet/cdc/DebeziumCdcSources.java))
* [cdc-mysql](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-mysql):
  allows for the creation of MySQL based CDC sources (see [MySqlCdcSources](https://github.com/hazelcast/hazelcast-jet/blob/master/extensions/cdc-mysql/src/main/java/com/hazelcast/jet/cdc/mysql/MySqlCdcSources.java))
* [cdc-postgres](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-postgres):
  allows for the creation of PostgreSQL based CDC sources (see [PostgresCdcSources](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-mysql/src/main/java/com/hazelcast/jet/cdc/postgres/PostgresCdcSources.java))
* more supported databases coming in future versions

`cdc-debezium` is the core module. It generates a self-contained JAR
with all dependencies included and is stored in the `lib` folder of the
distribution. The `cdc-mysql` and `cdc-postgres` jars are also in the
`lib` folder of the distribution, but they don't contain dependencies,
so have to be put on the classpath together with the `cdc-debezium` jar.

The above mentioned jars will take care of all external dependencies
too, no need to explicitly deal with Debezium connector jars or anything
else. Just take care that these jars aren't included in the slim
distribution of Jet, only the fat one.

## Serialization

The helper classes defined in `cdc-debezium`, such as `ChangeRecord` and
 its innards are data objects that need to travel around in Jet
 clusters. For that they need to be serializable. Among the
 [serialization options available in
 Jet](../api/serialization.md#serialization-of-data-types) they
 implement the `StreamSerializer` and their `SerializerHook` gets
 registered with the cluster when the `cdc-debezium` jar is put on the
 classpath (or any of the other, database specific jars, which also
 include the core).

## Performance

### Setup

Initial performance tests have been performed on non-production grade
hardware, but results should still be relevant:

* MySQL (v5.7) / PostgreSQL (v11) server running via Docker on a
  dedicated Ubuntu 19.10 box (quad core i7-6700K CPU @ 4.0GHz, 16GB
  memory, AMD Radeon R3 SSD drive)
* Jet CDC pipeline running in a single node Jet cluster on a MacBook
  Pro (6 core i7 CPU @ 2.6GHz, 32GB memory, Apple AP0512M SSD)

### Results

#### MySQL

Maximum number of *sustained* change records that could be produced in
the MySQL database (by continuously inserting into and then deleting
data from) was around **100,000 rows/second**. The Jet job processing
all the corresponding CDC events (reading them and mapping them to user
data objects) had no problems with keeping up, no lagging behind observed.

*Peak* even handling rate of the Jet pipeline has been observed around
**200,000 records/second**. This scenario was achieved by making the
connector snapshot a very large table and inserting into the table at
peak rate while snapshotting was going on. Once the connector finished
snapshotting and switched to reading the binlog the above peak rate
seems to be what it's capable of.

*Snapshotting* seems to be pretty fast too, was observed to happen at
around **185,000 rows/second** (10 million row table finished in under
one minute).

##### Database Hit

Enabling the *binlog* on the MySQL database has been observed to produce
a **15%** performance hit on the number of update/insert/delete events
it's able to process.

Enabling *global transaction IDs* doesn't seem to produce a
significant performance impact.

#### PostgreSQL

Maximum number of *sustained* change records that could be produced in
the database (by continuously inserting into and then deleting data
from) was only around **20,000 rows/second**.

This rate is much lower than what was possible with MySQL and seems to
stem from how [logical
decoding](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)
is implemented by PostgreSQL, namely that it seems to be a single
threaded process which doesn't scale much.

We are not sure if this is a technical limitation or is intentionally
done so by Postgres to limit the impact of logical replication, since it
can be enabled only on the primary server of the Postgres cluster.
