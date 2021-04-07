---
title: 006 - Declarative Serialization
description: Out of box support for additional serialization types such as Avro and Protobuf.
---

*Since*: 4.1

## Problem statement

In Jet 4.0 we support 4 interfaces to serialize custom types. However,
either implementations are slow (`java.io.Serializable` &
`java.io.Externalizable`) or must depend on IMDG specific classes and
require hand crafted code from the user which is cumbersome and error
prone (`com.hazelcast.nio.serialization.Portable` &
`com.hazelcast.nio.serialization.StreamSerializer`).

There are already fast and declarative serialization libraries out
there, such as Avro or Google Protocol Buffers. We would like to reduce
the effort to use them with Jet to the minimum.

Note that we don't try to solve the limitations of job-level
serializers, which are:

- inability to work with remote `IMap`s, `Cache`s & `IList`s
- inability to update an `IMap` and read from it using a user-defined
  predicate and projections
- inability to read from `EventJournal`

## Prototyped solution

The goal was to require from a user as little
configuration/typing/implementation as possible.

The prototype is based on Protocol Buffers but similar mechanism hopefully
can be applied for other serialization frameworks.

Assuming Java classes are already generated and added to Jet classpath
via `addClass()`, `addJar()` etc. you can simply enable/register
Protocol Buffers serializer with:

```java
new JobConfig()
  .enableSerialization(PROTOCOL_BUFFERS)
```

Exploiting the fact that all
[generated protocol message classes extend GeneratedMessageV3](https://www.javadoc.io/static/com.google.protobuf/protobuf-java/3.11.4/com/google/protobuf/GeneratedMessageV3.html)
a dynamic job-level serializer is configured for a given job. It detects
whether `GeneratedMessageV3` is assignable from an object's class and
serializes it in a generic way.

Here is the wire format:

```text
+---------------------------------+--------------------+
| FQCN length                     |   4 bytes (int)    |
+---------------------------------+--------------------+
| FQCN                            |   variable size    |
+---------------------------------+--------------------+
| Payload length                  |   4 bytes (int)    |
+---------------------------------+--------------------+
| Payload                         |   variable size    |
+---------------------------------+--------------------+
```

FQCN (Fully Qualified Class Name) has been used but any globally unique
identifier should do the trick.

Here's a comparison between implemented serializer and base one (simply
writing/reading plain Protocol Buffer message):

```text
Benchmark                       Mode  Cnt  Score   Error   Units
SerializerBenchmark.serializer thrpt    5  2.827 ± 0.184  ops/us
SerializerBenchmark.base       thrpt    5  7.279 ± 0.745  ops/us
```

### Type Identifier

FQCN adds not only processing but also memory overhead. The id of the
type which in `StreamSerializer` was a 4 bytes `int` now is a `String`
of arbitrary length. However, if we want to spare the user of assigning
unique type ids manually we need to rely on an identifier that would
work with:

- job resumes/restarts
- job upgrades
- potentially IMDG data structures

Other feasible solution would be to maintain a registry of types.
Basically an immutable mapping of FQCN to an `int` which could be
dynamically updated/queried/cached by serializers.

Beam and Flink take a different approach. Instead of encoding type id
they rely on `TypeDescriptor`/`TypeInformation` and inferred/user
supplied type information to assign appropriate
`Coder`s/`TypeSerializer`s to each transformer. That not only lets them
avoid managing type ids but also save on runtime serializer lookup.

## Considered solutions

1. FQCN:
   - easiest & fastest to implement, we already have everything to make
   it work
   - slow & bloated

2. Id registry:
   - more CPU & memory friendly than above
   - requires yet another moving part in our serialization universe

3. Inferred type information:
   - does not require runtime lookup of serializer which ultimately might
   be the fastest option
   - would require (complete?) redesign of the pipeline, most time
   consuming to implement
   - sometimes requires input from the user

## Implemented solution

Ultimately, we have decided to proceed with none of the above and implement
something that would better fit current serialization framework. User still
needs to manage type ids however we make it little easier to create
serializers and hooks.

### Protocol Buffers

An extension *hazelcast-jet-protobuf* including basic support for Protocol
Buffers has been created. To implement a Protocol Buffers serializer it's
now enough to extend:

```java
class PersonSerializer extends ProtobufSerializer<Person> {

    private static final int TYPE_ID = 1;

    PersonSerializer() {
        super(Person.class, TYPE_ID);
    }
}
```

Such serializer is an IMDG
`com.hazelcast.nio.serialization.StreamSerializer` and as such can be
registered for the job:

```java
new JobConfig()
    .registerSerializer(Person.class, PersonSerializer.class)
```

Similarly, to implement a hook that could be used to register Protocol
Buffers serializer on a cluster level it's enough to extend:

```java
class PersonSerializerHook extends ProtobufSerializerHook<Person> {

    PersonSerializerHook() {
        super(Person.class, TYPE_ID);
    }
}
```

Both, provided serializer and hook adapters are supported as any other
custom IMDG serializers and hooks. For a detailed instructions on how
to use them, please follow
[the guidelines](../api/serialization.md#register-a-serializer-for-a-single-jet-job).

Classes generated by Protocol Buffers already implement
`java.io.Serializable`, so they are automatically supported without a
custom serializer. However, with provided adapter you can expect better
performance:

```text
Benchmark                               Mode  Cnt  Score   Error   Units
SerializerBenchmark.protobufSerializer thrpt    5  6.606 ± 0.646  ops/us
SerializerBenchmark.javaSerializer     thrpt    5  0.155 ± 0.002  ops/us
```

### Avro

Classes generated by Avro already implement
`java.io.Externalizable`, so they work out of the box - no custom
serializers nor hooks are needed. The benchmark numbers present as
follows:

```text
Benchmark                           Mode  Cnt  Score   Error   Units
SerializerBenchmark.avroSerializer thrpt    5  2.076 ± 0.044  ops/us
SerializerBenchmark.javaSerializer thrpt    5  0.896 ± 0.013  ops/us
```

Despite having better performance than plain `java.io.Serializable`
it's still more than half of the *ideal* throughput so we might
consider creating custom adapter for it as well.
