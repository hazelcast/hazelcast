---
title: 002 - Job-level Serialization
description: Make it possible to add serializer per job
---

*Since*: 4.1

## Problem statement

Be able to run jobs with context scoped custom serializers - in
particular it should be possible to configure two different serializers
for a given data type on two different jobs.

## Possible solution

### JobConfig

Assuming we want to allow users to add their hand written serializers to
a given job we could extend `JobConfig` API:

```java
new JobConfig().addSerializer(Value.class, ValueSerializer.class)
```

More complex serializers - i.e. a custom one implementing Avro
serialization - might require additional resources, those could be added
via existing `add[Class|Jar]`:

```java
new JobConfig()
    .addSerializer(Value.class, ValueSerializer.class)
    .addJar("/file/serialization-library.jar")
```

All the serialization classes/jars could be then uploaded to job’s
`IMap`, similarly to how is it done for other resources via
`JobRepository`. That would allow reusing existing class loading mechanism
and make sure resources are cleaned up on job completion.

### SerializationService

Currently all the serializers are registered up front, before cluster
startup. Moreover, they are all public scoped - they are accessible for
all the jobs running in a given cluster - which forbids different
serializers for same data type on a different job.

There are couple of ways we could support job level serializers:

1. Extend `SerializationService` to allow runtime registration and
   de-registration of serializers. Moreover, we would need to add the
   possibility to lookup serializers not only by class but also by
   something like a tuple of job id and a class.
2. Duplicate the `SerializationService` for each job. That would also
   require extending `SerializationService` to allow runtime
   registration of serializers. However, it lets us use existing
   serializer lookup mechanism.
3. Create new `SerializationService` with job level only serializers
   which would fallback to public `SerializationService`.

All above require to some extent API changes in IMDG `SerializationService`.

Registering serializers in runtime would introduce the possibility to
create a simpler `JobLevelSerializer` interface - without the need to
declare type id and necessity to manage it. For instance:

```java
public interface Codec<T> {

    void encode(T value, DataOutput output) throws IOException;

    T decode(DataInput input) throws IOException;
}
```

However, that's another interface in already overcrowded universe of
Hazelcast serialization interfaces so whether it is valuable or not is
questionable. Another thing is that most probably it could not be used
to work with data types stored in IMDG.

### Pipeline

Going with 1. would require changing code in each Jet's
`SerializationService` call site - altering not only the way lookup is
performed but also making sure job id is available in there.

Going with 2. or 3. requires spawning a new `SerializationService` per
job and hooking it up (at least) in:

* `SenderTasklet`
* `ReceiverTasklet`
* `OutboxImpl`
* `ExplodeSnapshotP`

## Implemented Solution

Each job execution gets its own `SerializationService` which encapsulates
job-level serializers and falls back to cluster `SerializationService`.

Job-level serializers can be used to read/write data from/to local IMDG
`Observable`s, `List`s, `Map`s & `Cache`s.

Job-level `SerializationService` serializers have precedence over any
cluster serializers - if type `A` have serializers registered on both
levels, cluster and job, the latter will be chosen for given job.

### API changes

`JobConfig` has been extended with:

```java
/**
 * Registers the given serializer for the given class for the scope of the
 * job. It will be accessible to all the code attached to the underlying
 * pipeline or DAG, but not to any other code. (An important example is the
 * {@code IMap} data source, which can instantiate only the classes from
 * the Jet instance's classpath.)
 * <p>
 * Serializers registered on a job level have precedence over any serializer
 * registered on a cluster level.
 * <p>
 * Serializer must have no-arg constructor.
 *
 * @return {@code this} instance for fluent API
 * @since 4.1
 */
 @Nonnull
 @EvolvingApi
 public <T, S extends StreamSerializer<?>> JobConfig registerSerializer(@Nonnull Class<T> clazz,
                                                                        @Nonnull Class<S> serializerClass) {
    Preconditions.checkFalse(serializerConfigs.containsKey(clazz.getName()),
                "Serializer for " + clazz + " already registered");
    serializerConfigs.put(clazz.getName(), serializerClass.getName());
    return this;
 }
 ```

Given sample value class and its serializer:

```java
class Value {

    public static final int TYPE = 1;

    private Integer v;

    public Value(Integer v) {
            this.v = v;
    }
}

class ValueSerializer implements StreamSerializer<Value> {

    @Override
    public int getTypeId() {
        return Value.TYPE;
    }

    @Override
    public void write(ObjectDataOutput objectDataOutput, Value value) throws IOException {
        objectDataOutput.writeInt(value.v);
    }

    @Override
    public Value read(ObjectDataInput objectDataInput) throws IOException {
        return new Value(objectDataInput.readInt());
    }
}
```

one registers them with `JobConfig`, the following way
(`registerSerializer()` does not add classes to the classpath, we have
other means to do that, see `addClass()`, `addJar()` etc):

```java
JobConfig jobConfig = new JobConfig()
    .addClass(Value.class, ValueSerializer.class)
    .registerSerializer(Value.class, ValueSerializer.class)
```

## Soak Tests

Each job execution gets its own job-level `SerializationService`. It
would be beneficial to add a soak test to make sure that nothing is
leaking when there are thousands of jobs running with job-level
serializers.

## Limitations

Job-level serializers are used to serialize objects between distributed
edges & to/from snapshots. They can also be used to read/write data
from/to local IMDG data structures. However, if you want to work with
them outside of the job, you have to register compatible serializers on
a cluster level as well.

Moreover, following functionalities are not currently supported:

* querying `Map`s (reading from an `IMap` with user defined predicates
    & projections)
* merging/updating `Map`s
* streaming `Journal` data

## Improvements

Allow job-level serializers to be used with remote IMDG data structures.

Allow job-level serializers to:

* query `Map`s
* merge/update `Map`s
* stream `Journal` data
