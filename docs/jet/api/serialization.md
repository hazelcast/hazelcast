---
title: Serialization
description: Options available for (de)serialization when using Jet.
---

To be able to send object state over a network or store it in a file
you have to first serialize it into raw bytes. Similarly, to be able to
fetch an object state over a wire or read it from a persistent storage
you have to deserialize it from raw bytes first. As Hazelcast Jet is a
distributed system by nature serialization is integral part of it.
Understanding, when it is involved, how does it support the pipelines
and knowing differences between supported strategies is crucial to
efficient usage of Hazelcast Jet.

## Serialization of Pipelines

A typical Jet pipeline involves lambda expressions. Since the whole
pipeline definition must be serialized to be sent to the cluster, the
lambda expressions must be serializable as well. The Java standard
provides an essential building block: if the static type of the lambda
is a subtype of `Serializable` you will automatically get a lambda
instance that can serialize itself.

None of the functional interfaces in the JDK extend `Serializable` so
we had to mirror the entire `java.util.function` package in our own
`com.hazelcast.function` with all the interfaces subtyped and made
`Serializable`. Each subtype has the name of the original with `Ex`
appended. For example, a `FunctionEx` is just like `Function` but
implements `Serializable`. We use these types everywhere in the
Pipeline API.

As always with this kind of magic, auto-serializability of lambdas has
its flipside: it is easy to overlook what’s going on.

If the lambda references a variable in the outer scope, the variable is
captured and must also be serializable. If it references an instance
variable of the enclosing class, it implicitly captures this so the
entire class will be serialized. For example, this will fail because
`JetJob1` does not implement `Serializable`:

```java
class JetJob1 {
    private String instanceVar;

    Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list("input"))
          // Refers to `instanceVar`, capturing `this`, but `JetJob1` is not
          // `Serializable` so this call will fail.
          .filter(item -> item.equals(instanceVar));
        return p;
    }
}
```

Just implementing `Serializable` for `JetJob1` would be a viable
workaround here. However, consider something just a bit different:

```java
class JetJob2 implements Serializable {
    private String instanceVar;
    // A non-serializable field.
    private OutputStream fileOut;

    Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list("input"))
         // Refers to `instanceVar`, capturing `this`. `JetJob2` is declared
         // as `Serializable`, but has a non-serializable field and this fails.
         .filter(item -> item.equals(instanceVar));
        return p;
    }
}
```

Even though we never refer to `fileOut`, we are still capturing the
entire `JetJob2` instance. We might mark `fileOut` as transient, but
the sane approach is to avoid referring to instance variables of the
surrounding class. We can simply achieve this by assigning to a local
variable, then referring to that variable inside the lambda:

```java
class JetJob3 {
    private String instanceVar;

    Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        // Declare a local variable that loads the value of the instance field.
        String findMe = instanceVar;
        p.readFrom(Sources.list("input"))
         // By referring to the local variable `findMe` we avoid
         // capturing `this` and the job runs fine.
         .filter(item -> item.equals(findMe));
        return p;
    }
}
```

Another common pitfall is capturing an instance of `DateTimeFormatter`
or a similar non-serializable class:

```java
DateTimeFormatter formatter = DateTimeFormatter
        .ofPattern("HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault());
Pipeline p = Pipeline.create();
BatchStage<Long> src = p.readFrom(Sources.list("input"));
// Captures the non-serializable formatter, so this fails.
src.map((Long tstamp) -> formatter.format(Instant.ofEpochMilli(tstamp)));
```

Sometimes we can get away by using one of the preconfigured formatters
available in the JDK:

```java
// Accesses the static final field `ISO_LOCAL_TIME`. Static fields are
// not subject to lambda capture, they are dereferenced when the code
// runs on the target machine.
src.map((Long tstamp) -> DateTimeFormatter.ISO_LOCAL_TIME
        .format(Instant.ofEpochMilli(tstamp).atZone(ZoneId.systemDefault())));
```

This refers to a `static final` field in the JDK, so the instance is
available on any JVM. If this is not available, you may create a
`static final` field in your own class, but you can also use
`mapUsingService()`. In this case you provide a serializable factory
that Jet will ask to create an object on the target member. The object
it returns does not have to be serializable. Here’s an example of that:

```java
Pipeline p = Pipeline.create();
BatchStage<Long> src = p.readFrom(Sources.list("input"));
ServiceFactory<?, DateTimeFormatter> serviceFactory = nonSharedService(
        pctx -> DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
                              .withZone(ZoneId.systemDefault()));
src.mapUsingService(serviceFactory,
        (formatter, tstamp) -> formatter.format(Instant.ofEpochMilli(tstamp)));
```

## Serialization of Data Types

Hazelcast Jet closely integrates with Hazelcast IMDG, exposing many of
its features to Jet users. In particular, you can use an IMDG data
structure as a Jet `Source` and/or `Sink`. The objects you store in them
must be serializable.

Another case that requires serializable objects is sending computation
results between nodes, for example when grouping by key. To catch
serialization issues early on, we recommend using a 2-member local Jet
cluster for development and testing.

Currently, Hazelcast Jet supports 4 interfaces to serialize custom
types:

- [java.io.Serializable](https://docs.oracle.com/javase/8/docs/api/java/io/Serializable.html)
- [java.io.Externalizable](https://docs.oracle.com/javase/8/docs/api/java/io/Externalizable.html)
- [com.hazelcast.nio.serialization.Portable](/javadoc/{jet-version}/com/hazelcast/nio/serialization/Portable.html)
- [com.hazelcast.nio.serialization.StreamSerializer](/javadoc/{jet-version}/com/hazelcast/nio/serialization/StreamSerializer.html)

The following table provides a comparison between them to help you in
deciding which interface to use in your applications.
|Serialization interface|Advantages|Drawbacks|
|:---------------------:|:---------|:--------|
|Serializable|Easy to start with, does not require implementation or registration|CPU intensive and space inefficient|
|Externalizable|Does not require registration, faster and more space efficient than Serializable|CPU intensive, space inefficient and requires implementation|
|Portable|Faster and more space efficient than Serializable. Supports versioning and partial deserialization|Requires implementation and registration|
|StreamSerializer|Fastest and lightest|Requires implementation and registration|

Below you can find rough performance numbers you can expect when
employing each of those strategies. A straightforward benchmark that
continuously serializes and then deserializes this simple object:

```java
class Person {
    private String firstName;
    private String lastName;
    private int age;
    private float height;
}
```

yields following throughputs:

```text
# Processor: Intel(R) Core(TM) i7-4700HQ CPU @ 2.40GHz
# VM version: JDK 13, OpenJDK 64-Bit Server VM, 13+33

Benchmark                              Mode  Cnt  Score   Error   Units
SerializationBenchmark.serializable   thrpt    3  0.259 ± 0.087  ops/us
SerializationBenchmark.externalizable thrpt    3  0.846 ± 0.057  ops/us
SerializationBenchmark.portable       thrpt    3  1.171 ± 0.539  ops/us
SerializationBenchmark.stream         thrpt    3  4.828 ± 1.227  ops/us
```

Here are the sizes of the serialized form by each serializer:

```text
Strategy                                        Number of Bytes  Overhead %
java.io.Serializable                                        162         523
java.io.Externalizable                                       87         234
com.hazelcast.nio.serialization.Portable                    104         300
com.hazelcast.nio.serialization.StreamSerializer             26           0
```

You can see that using plain `Serializable` can easily become a
bottleneck in your application, as even with this simple data type it's
more than an order of magnitude slower than other serialization options,
not to mention very wasteful with memory.

## Write a Custom Serializer

For the best performance and simplest implementation we recommend using
the Hazelcast
[StreamSerializer](/javadoc/{jet-version}/com/hazelcast/nio/serialization/StreamSerializer.html)
mechanism. Here is a sample implementation for a `Person` class:

```java
class PersonSerializer implements StreamSerializer<Person> {

    private static final int TYPE_ID = 1;

    @Override
    public int getTypeId() {
        return TYPE_ID;
    }

    @Override
    public void write(ObjectDataOutput out, Person person) throws IOException {
        out.writeUTF(person.firstName);
        out.writeUTF(person.lastName);
        out.writeInt(person.age);
        out.writeFloat(person.height);
    }

    @Override
    public Person read(ObjectDataInput in) throws IOException {
        return new Person(in.readUTF(), in.readUTF(), in.readInt(), in.readFloat());
    }
}
```

The type ID you use must be unique across all the serializers you
register for a job, and additionally it must not clash with any
global serializers you registered with the Hazelcast Jet cluster.

## Register a Serializer for a Single Jet Job

You can register a serializer in a Jet job's configuration object:

```java
new JobConfig()
    .registerSerializer(Person.class, PersonSerializer.class)
```

Such a serializer is scoped &mdash; its type ID doesn't clash with the
same type ID in another Jet job. However, if you also use the serializer
hook to register a global serializer on the Jet cluster, a job-local ID
would clash with it. The job-local serializer takes precedence, but it
is best to avoid such clashes due to the potential for surprising
behavior and hard-to-diagnose bugs.

Jet uses the job-local serializer to serialize the objects as they
travel through the Jet pipeline (over distributed DAG edges) and get
saved to snapshots.

Job-level serializers can also be used with IMDG [sources and
sinks](sources-sinks.md). You can read from/write to a local
`Observable`, `IList`, `IMap` or `ICache`. We are working on adding the
ability to read from an `IMap` using a user-defined predicate and
projections, update an `IMap`, and read from `EventJournal`.

## Register a Serializer with the Jet Cluster

You can register a serializer with the Jet cluster, before starting it.
For that you need a `SerializerHook`:

```java
class PersonSerializerHook implements SerializerHook<Person> {

    @Override
    public Class<Person> getSerializationType() {
        return Person.class;
    }

    @Override
    public Serializer createSerializer() {
        return new PersonSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
```

Hazelcast Jet uses the Java service discovery mechanism to find your
serializer hook. You should create a JAR with the serializer hook and
its dependent classes, and the JAR should have a file
`META-INF/services/com.hazelcast.SerializerHook` with the
fully-qualified name of the serializer hook class:

```text
com.hazelcast.jet.examples.PersonSerializerHook
```

Alternatively, you can add the following configuration to
`hazelcast.yaml`:

```yaml
hazelcast:
  serialization:
    serializers:
      serializer:
        "type-class": "com.hazelcast.jet.examples.Person"
        "class-name": "com.hazelcast.jet.examples.PersonSerializer"
```

Put the JAR containing the serializer hook and related classes in the
`<jet_home>/lib` directory. Make sure that each registered serializer
has a unique type ID.

The advantage of a cluster-level serializer is that it is supported in
all Hazelcast Jet features.

## 3rd-Party Serialization Support

### Google Protocol Buffers

Since the classes generated by Google Protocol Buffers (Protobuf)
already implement `java.io.Serializable`, Hazelcast Jet automatically
supports them without a custom serializer. However, for best performance
we encourage registering a Protobuf-specific serializer. There is a Jet
extension module that simplifies this for Protobuf version 3.

If you want to use it locally within a Jet job, add the extension as a
dependency to your Jet job's project:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile "com.hazelcast.jet:hazelcast-jet-protobuf:${jet-version}"
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-protobuf</artifactId>
    <version>${jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

Implement the adapter by extending the provided class (where `Person`
is of any Protobuf `GeneratedMessageV3` type):

```java
class PersonSerializer extends ProtobufSerializer<Person> {

    private static final int TYPE_ID = 1;

    PersonSerializer() {
        super(Person.class, TYPE_ID);
    }
}
```

Then register it with the job:

```java
new JobConfig()
    .registerSerializer(Person.class, PersonSerializer.class)
```

Also make sure that the Protobuf extension JAR is either on the Jet
cluster's classpath or inlined into your job JAR by creating a fat
JAR.

You can also install the serializer in the Jet cluster by implementing
and registering a serialization hook, as explained above.
