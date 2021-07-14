# Processor Classloader Design Document

The goal of this document is to describe the design of the processor
classloader feature, which allows specifying custom classpath for a
particular processor. This is useful when a different version of a
library than what is present in Hazelcast distribution is needed. It
might also be used to combine different versions of the same library in
different pipeline steps.

This is a prerequisite for using Hazelcast 3 client to access Hazelcast
3 members (see the Hazelcast 5.0 evaluation experience for Hazelcast
users and HZ-344).

## User facing API

We are implementing this feature for access to Hz 3. We will provide the
jars as part of the distribution (e.g. in some ext folder or similar).
We can provide a more general-purpose API later.

### Option 1

Use JobConfig and the transform name

```java
Pipeline p = Pipeline.create();
BatchSource<Integer> source = TestSources.items(1, 2, 3);
BatchStage<Integer> stage = p.readFrom(source);
stage.writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig().addCustomClasspathJar(source.name(), "hazelcast-3.12.12.jar");
```

The transform name is not unique but can be changed via setName(), so
it’s likely not an issue.

### Option 2

Add withCustomClasspath modifier to Stage 

```java
Pipeline p = Pipeline.create();
BatchSource<Integer> source = TestSources.items(1, 2, 3);
BatchStage<Integer> stage = p.readFrom(source)
        .withCustomClasspath("hazelcast-3.12.12.jar");
stage.writeTo(Sinks.logger());
```

The stage would delegate the call to the underlying transform.

There is a small issue with this approach during deserialization - the
classloader may already be needed to deserialize the transform - we
would need to write (and then read) the classloader configuration first,
create the classloader and then deserialize the required bits (e.g.
metaSupplier) with it, all during deserialization.

## Handling resources

### Option 1 Only server side resources

Allow specifying jars only in a particular folder already present on
the member - the main advantage is that it is simple.

The folder must be configurable, the most common case will use the
server distribution, so a HazelcastProperty is enough. A property in the
configuration has too much overhead, we can introduce this later, if
needed.

### Option 2 Allow upload of any resources from the client (similar to JobConfig)

This is more involved. There are more edge cases, need to perform
cleanup of the uploaded resources etc. While this is more powerful the
implementation of the resource upload is left for future improvements.

## Processor Classloader

The idea is to use what is commonly referred to as a child-first classloader.
The main advantage of this approach is that it can use a different
version of a class than what is present in the cluster (or in the Jet
job classloader).

The classloader is used for deserialization of the transform (and in
turn MetaSupplier, Supplier and Processor) in the DAG/Pipeline.  The
classloader is also set as a context classloader when interacting with
MetaSupplier, Supplier and processor instances.

Then there are 2 options how to implement the processor

- use reflection to load the classes from the processor classloader

- leverage JVM's lazy class loading, where the loading of the class is
  triggered only when e.g. a processor supplier is actually executed,
  This approach doesn't currently work for MetaSupplier and Supplier when
  using Pipeline (works with DAG), due to using Java serialization.

## Security

Currently, only server-side resources from the configured folder are
possible. The configured resource (jar) name must be checked against
breakouts like "../../some.jar".

Code changes

TODO describe code changes needed and issue with PipelineImpl
serialization.

