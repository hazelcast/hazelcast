---
title: 005 - Change Data Capture (CDC) Sources
description: Change Data Capture (CDC) Sources
---

*Target Release*: 4.1

## Background

In Jet 4.0 we have introduced [support for CDC sources](/blog/2020/03/02/jet-40-is-released#debezium)
based on [Debezium](https://debezium.io/).

That form however is not production ready:

* it's **inconvenient** to use: gives you huge JSON messages which you
  then have to interpret on your own, based on Debizium documentation

* native **timestamps** aren't provided (due to bugs in the code) and
  their shortcomings aren't documented either (mostly that timestamps in
  events coming from database snapshots aren't really event-times)
  
* **performance** impact on the DBs is not explored at all, recommended
  deployment guide is not provided
  
* miscellaneous **shortcomings** and **inconsistencies** of the various
  connectors are not documented

These are the problems we are trying to address for the next version.

## Parsing Convenience

We want to help users not to have to deal with low level JSON messages
directly. In order to achieve this we:

* define standard interfaces which offer **event structure**
* offer support for mapping various event component directly into
  **POJO data objects**

### Event Structure Interfaces

The interfaces framing the general structure of change events are
quite simple, but they have proven a nice fit across all connectors.

```java
public interface ChangeEvent extends Serializable  {

    // ...

    /**
     * Identifies the particular record or document being affected
     * by the change event.
     */
    ChangeEventKey key();

    /**
     * Describes the actual change affected on the record or ducument
     * by the change event.
     */
    ChangeEventValue value();
}
```

```java
public interface ChangeEventKey extends ChangeEventElement {
}
```

```java
public interface ChangeEventValue extends ChangeEventElement {

    // ...

    /**
     * Specifies the moment in time when the event happened. ...
     */
    long timestamp() throws ParsingException;

    /**
     * Specifies the type of change being described (insertion, delete or
     * update). ...
     */
    Operation operation() throws ParsingException;

    /**
     * Describes how the database record or document looked like BEFORE
     * applying the change event. Not provided for MongoDB updates.
     */
    ChangeEventElement before() throws ParsingException;

    /**
     * Describes how the database record or document looks like AFTER
     * the change event has been applied. Not provided for MongoDB updates.
     */
    ChangeEventElement after() throws ParsingException;

    /**
     * Describes the change being done by the event. Only used by
     * MongoDB updates.
     */
    ChangeEventElement change() throws ParsingException;

}
```

### Content Extraction

As can be noticed from the [event structure interfaces](#event-structure-interfaces),
all actual content can be found in the form of `ChangeEventElement`
instances.

Such an object is basically an encapsulated JSON message (part of the
original, big event message) and offers following methods to access the
data.

```java
public interface ChangeEventElement extends Serializable {

    <T> T map(Class<T> clazz) throws ParsingException;

    Optional<Object> getObject(String key) throws ParsingException;

    Optional<String> getString(String key) throws ParsingException;

    Optional<Integer> getInteger(String key) throws ParsingException;

    Optional<Long> getLong(String key) throws ParsingException;

    Optional<Double> getDouble(String key) throws ParsingException;

    Optional<Boolean> getBoolean(String key) throws ParsingException;

}
```

There are two options there:

* fetching JSON elements directly, via their name/key
* mapping the entire content to a POJO directly; more on that in the
  sections describing the various JSON formats used by the connectors

### Fallback

Considering the complexity of the original JSON messages and even their
varying formats it is clear that parsing them can be error prone and
can fail in some scenarios (for example on some untested DB-connector
version combination).

To prevent a total API failure in this case each of the event structure
interfaces contains an extra `asJson()` method, which will provide the
raw JSON message the object is based on:

* `ChangeEvent.asJson()` gives you the original message from the
  connector, without anything having been done to it; if all else fails
  this will still work
* `ChangeEventKey.asJson()` & `ChangeEventValue.asJson()` will give you
  the key and value parts of the original message
* `ChangeEventElement.asJson()` gives you the JSON message of that
  particular fragment

### Standard JSON

Most Debezium database connecters, with the notable exception of the
MongoDB connector, provide messages in standard JSON format which can
be parsed by any one of the well-known JSON parsing libraries out
there. For backing the implementations of our
[event structure interfaces](#event-structure-interfaces) we have
chosen to use [Jackson Databind](https://github.com/FasterXML/jackson-databind).

This is mostly an internal implementation detail,
``ChangeEventElement``'s `getXXX()` methods don't reveal it. Where it
becomes visible is the object mapping method. This is based on
Jackson's `ObjectMapper` and the classes we pass to it expose this fact
via the annotations we have to use in them. For example:

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
    .map(event -> {
        ChangeEventValue eventValue = event.value();
        Operation operation = eventValue.operation();
        ChangeEventElement mostRecentImage = DELETE.equals(operation) ?
                eventValue.before() : eventValue.after();
        return mostRecentImage.map(Customer.class);
    });
```

### MongoDB's Extended JSON (v2)

The MongoDB Debezium connector unfortunately doesn not use the standard
JSON format, but their [MongoDB Extended JSON (v2)](https://docs.mongodb.com/manual/reference/mongodb-extended-json/).

The most obvious consequence of this is that the messages need to be
parsed with the help of their Java driver. ``ChangeEventElement``'s
`getXXX()` methods hide this fact, but the object mapping is affected.
As it stands now our implementation can map only to the
`org.bson.Document` class.

And that's not all. The extended part of the syntax also comes in play,
because Mongo update messages arrive in the form of "patch" expressions,
like `"patch": "{\"$set\":{\"first_name\":\"Anne Marie\"}}"`. As of now
we don't really have a non-hacky way of parsing these messages...

> TODO: Nicer parsing of patch messages.

Using our API on MongoDB events looks like this:

```java
StreamStage<Document> stage = pipeline.readFrom(source)
    .withoutTimestamps()
    .mapStateful(
            State::new,
            (state, event) -> {
                Operation operation = event.operation();
                switch (operation) {
                    case SYNC:
                    case INSERT:
                        state.set(event.value().after().map(Document.class));
                        break;
                    case UPDATE:
                        state.update(event.value().change().map(Document.class));
                        break;
                    case DELETE:
                        state.clear();
                        break;
                    default:
                        throw new UnsupportedOperationException(operation.name());
                }
                return state.document;
            });
```

```java
private static class State implements Serializable {

    private Document document = new Document();

    Document get() {
        return document;
    }

    void set(Document document) {
        this.document = Objects.requireNonNull(document);
    }

    void update(Document document) {
        this.document.putAll((Document) document.get("$set")); //todo: blasphemy!
    }

    void clear() {
        document = new Document();
    }
}
```

For what makes this state necessary pls. see the [inconsistencies](#inconsistencies)
section.

## Source Configuration

Debezium CDC connectors have a ton of configuration options and we don't
want to ask users to go to the Debezium website to explore them.

We need to provide two things. A way to **discover** and **easily use**
the config options and a way to still allow any properties to be set
or overwritten, even if for some reason we forgot or failed to handle
them.

For this we are going to use builders, like this:

```java
StreamSource<ChangeEvent> source = CdcSources.mysql("customers")
    .setDatabaseAddress(mysql.getContainerIpAddress())
    .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
    .setDatabaseUser("debezium")
    .setDatabasePassword("dbz")
    .setDatabaseClusterName("dbserver1")
    .setDatabaseWhitelist("inventory")
    .setTableWhitelist("inventory.customers")
    .setCustomProperty("database.server.id", "184054") //generic method to set anything
    .build();
```

In the public builder code we can:

* hardcode some properties needed for our parsing
* explain what various properties do
* check rules like properties that always need to be set or can't be
  used together

```java
public static final class MySqlBuilder extends AbstractBuilder<MySqlBuilder> {

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        public MySqlBuilder(String name) {
            super(name, "io.debezium.connector.mysql.MySqlConnector");
            properties.put("include.schema.changes", "false");
        }

        // ...

        /**
         * An optional comma-separated list of regular expressions that
         * match fully-qualified table identifiers for tables to be
         * monitored; any table not included in the whitelist will be
         * excluded from monitoring. Each identifier is of the form
         * 'databaseName.tableName'. By default the connector will
         * monitor every non-system table in each monitored database.
         * May not be used with table blacklist.
         */
        public MySqlBuilder setTableWhitelist(String whitelist) {
            properties.put("table.whitelist", whitelist);
            return this;
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            check();
            return KafkaConnectSources.connect(properties, ...);
        }
    }
```

```java
private abstract static class AbstractBuilder<SELF extends AbstractBuilder<SELF>> {

        protected final Properties properties = new Properties();

        private AbstractBuilder(String name, String connectorClass) {
            properties.put("name", name);
            properties.put("connector.class", connectorClass);
            properties.put("database.history", HazelcastListDatabaseHistory.class.getName());
            properties.put("database.history.hazelcast.list.name", name);
            properties.put("tombstones.on.delete", "false");
        }

        /**
         * Can be used to set any property not covered by our builders,
         * or to override properties we have hidden.
         *
         * @param key   the name of the property to set
         * @param value the value of the property to set
         * @return the builder itself
         */
        public SELF setCustomProperty(String key, String value) {
            properties.put(key, value);
            return (SELF) this;
        }
    }
```

## Inconsistencies

### Timestamps aren't always event times

Current version of the sources does fix timestamps, because they haven't
been working before, but beyond that timestamps have a big weakness,
which we need to at least document.

The timestamps we normally get from these sources are proper
event-times, sourced from the database change-log. The problem is with
fault tolerance. If the connector needs to be restarted it will need
to re-fetch some events (that happened after the last Jet snapshot).

It can happen in such cases, that not all these events are in the quite
limited database changelog. In such cases the connector uses a snapshot
of the database, which merges many past events into a single, current
image. Not only do events get lost for good, the timestamps in the
snapshot ones aren't event times any longer.

Luckily change events that come from snapshots can be simply identified.
They are basically insert events and their `Operation` field will not
only reflect that, but use a special type of insert value, called `SYNC`
for them. Except some, see [further inconsistencies](#snapshots-are-not-marked-in-mysql).

This problem can be mitigated for example by longer DB changelogs. They
probably aren't that big of a deal in most practical situations, but
still, they can be and we need to make that clear in our documentation.

### Snapshots are not marked in MySQL

As discussed in the [section about timestamps](#timestamps-arent-always-event-times)
there is a need to identify events that originate in database snapshots
(as opposed to database changelogs).

This is almost always possible, because such events will have `SYNC` as
their operation, except one case, MySQL which does not mark them the
same way. We could fix this in our implementation, but the fix might
not work as expected in some messages we fail to consider, causing their
parsing to fail. We don't want to do more harm then good, so for now
we'll just document this inconsistency.

### Parsing MongoDB events requires state

Change events coming in from most database connectors always have either
a `ChangeEventValue.before()` or `ChangeEventValue.after()` part, from
which the latest complete image of the affected database record can be
inferred. This makes processing events in isolation possible.

However, in the case of MongoDB, update events don't have either of the
two... They only carry a "patch" message, the actual change itself, like
"set this and that field to this and that value", but don't tell us
anything about the unaffected parts of the database document.

In order to process such events we need to maintain state, as can be
seen in the [previously provided example](#mongodbs-extended-json-v2).

## Dependencies

Regardless what convenience we add, our CDC connectors ultimately remain
Debezium based which means lots of dependencies that need to be
satisfied.

Currently all connectors are in the [contrib repository](https://github.com/hazelcast/hazelcast-jet-contrib)
and individual connector jar archives still need to be explicitly
added to the `JobConfig`.

How this problem will be solved once we move them to the main
Jet repo, remains an open question. We'll probably use one module per
connector with all dependencies included.

> TODO: Move code to main Jet repo (requires new release of Kafka
> Connect source).
>
> TODO: Find a way to get rid of `JobConfig` jar upload.

## Distributedness

> To be explored. All Debezium connectors are based on Kafka Connect,
> which does have a [distributed mode](https://docs.confluent.io/current/connect/userguide.html#distributed-mode)
> and perhaps we can/should make use of that somehow.

## Performance

> Has to be addressed still. Research will focus on MySQL at
> first. There the impact should be minimal if a DB cluster replica
> is used as the source for the connector. All the primary cluster
> needs extra in that case is for global transaction IDs to be enabled.
