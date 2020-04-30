---
title: Jet 4.1 is Released
author: Can Gencer
authorURL: http://twitter.com/cgencer
authorImageURL: https://pbs.twimg.com/profile_images/1187734846749196288/elqWdrPj_400x400.jpg
---

We are happy to present the new release of Hazelcast Jet 4.1. Here's a
quick overview of the new features.

## Extended gRPC Support

We've applied the lessons learned from the Jet-Python integration and
made it easier to integrate a Jet pipeline with [gRPC](https://grpc.io)
services. The utility class `GrpcServices` introduces two new
`ServiceFactory`s you can use with the `mapUsingServiceAsync` transform.
Using this feature can be a significant performance boost vs. using the
sync `mapUsingService` call.

Here's a quick example on how you can use the gRPC service factory:

```java
var greeterService = unaryService(
    () -> ManagedChannelBuilder.forAddress("localhost", 5000).usePlaintext(),
    channel -> GreeterGrpc.newStub(channel)::sayHello
);

Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("one", "two", "three", "four"))
 .mapUsingServiceAsync(greeterService, (service, input) -> {
    HelloRequest request = HelloRequest.newBuilder().setName(input).build();
    return service.call(request);
}).writeTo(Sinks.logger());
```

In addition to the unary gRPC service, we support bidirectional
streaming as well as request batching. For a more in-depth look, see the
[Call a gRPC Service how-to guide](/docs/how-tos/grpc) and the [design
document](/docs/design-docs/007-grpc-support).

## Transactional JDBC and JMS sinks

In Jet 4.0 we added support for [transactional sources and
sinks](/blog/2020/02/20/transactional-processors) through the use of
two-phase commit. We're now extending this support for two additional
sinks: JDBC and JMS. The support requires the broker or the database to
support XA transactions. To test your database's support for XA
transactions, we've also released a [how-to guide](/docs/how-tos/xa).

You can also see a full summary of sinks and sources and the variety of
transaction support on the [sources and sinks
page](/docs/api/sources-sinks#summary).

## Code Deployment Improvements

When you're deploying a Jet job programmatically (not using the `jet
submit` command-line tool), you must add every class the job needs to
the job's configuration. So far, Jet has supported adding classes one by
one with `JobConfig.addClass()` and that wouldn't add any of the class's
nested classes. This was especially problematic for anonymous classes,
which you can't even refer to from Java code. In 4.1 we improved
`addClass()` so that it adds all the nested classes and we added
`JobConfig.addPackage()` so you can add the whole package in a
one-liner, and don't have to manually maintain the list of classes as
you develop your pipeline. Take a look at the [design
document](/docs/design-docs/001-code-deployment-improvements) for more
details.

## Job-Scoped Serializer Support

So far Jet has had a pain point in terms of serialization. The objects
that travel through the pipeline must sometimes be sent from one cluster
member to the other, so they must be serialized. You can let the object
implement `Serializable`, but that's inefficient. If you wanted to use
a better serialization scheme, you had to register a serializer object
with the Jet cluster and restart the whole cluster.

It is now possible to attach a serializer directly to the job you're
submitting.

```java
JobConfig config = new JobConfig()
  .registerSerializer(Person.class, PersonSerializer.class);

jet.newJob(pipeline, config);
```

Jet will use these serializers only inside the job. You can read more
about how serialization in Hazelcast Jet works in the [serialization
guide](/docs/api/serialization).

## Protocol Buffers Support

Having added the job-level serializers, we also added an extra layer of
convenience to use [Google Protocol
Buffers](https://developers.google.com/protocol-buffers) for
serialization. You just need to write a simple class that delegates the
work to the Protobuf compiler-generated serializer class (`Person` in
this case):

```java
class PersonSerializer extends ProtobufSerializer<Person> {

    private static final int TYPE_ID = 1;

    PersonSerializer() {
        super(Person.class, TYPE_ID);
    }
}
```

For more information, see the [serialization guide](/docs/api/serialization#google-protocol-buffers).

## Spring Boot Starter

Spring Boot is a framework that helps you create standalone Spring-based
applications that just run. Spring Boot provides auto-configuration of
some of the commonly used libraries through spring-boot-starters.
Hazelcast Jet now provides its own [Spring Boot
Starter](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/hazelcast-jet-spring-boot-starter)
which can be used to auto-configure and start a Hazelcast Jet instance.

Just by adding the starter dependency to your Spring Boot application,
you can start a `JetInstance` with the default configuration. If you
want to customize the configuration, just add a configuration file
(`hazelcast-jet.yaml`) to your classpath or working directory. The
starter will pick it up and configure your Hazelcast Jet instance.  If
you want a client instance, add the client configuration file
(`hazelcast-client.yaml`).

For more details, see the [design
document](/docs/design-docs/004-spring-boot-starter).

## Kubernetes Operator and OpenShift Support

With version 4.1 we are introducing our [Hazelcast Jet Kubernetes
Operator](https://operatorhub.io/?keyword=jet). It's available for both
Hazelcast Jet open-source and Enterprise editions. Hazelcast Jet
Enterprise Operator is also a certified by Red Hat and available on the
Red Hat Marketplace.

## Discovery Support for Microsoft Azure

We have extended the list of cloud environments where Hazelcast Jet
instances are able to automatically discover each other and form a
cluster. Self-discovery now works in the Microsoft Azure environment.
Here's a quick example on how to enable it:

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      azure:
        enabled: true
        tag: TAG-NAME=HZLCAST001
        hz-port: 5701-5703
```

For more details, please see the [discovery
guide](/docs/operations/discovery#azure-cloud).

## Full Release Notes

Members of the open source community that appear in these release notes:

- @TomaszGaweda
- @caioguedes
- @SapnaDerajeRadhakrishna

Thank you for your valuable contributions!

### New Features

- [jms] Exactly-once guarantee for JMS sink (#1813)
- [jdbc] Exactly-once guarantee for JDBC sink (#1813)
- [core] JobConfig.addClass() automatically adds nested classes to the
  job (#1932)
- [core] JobConfig.addPackage() adds a whole Java package to the job
  (#1932, #2077)
- [core] Job-scoped serializer deployment (#2020, #2038, #2039, #2043,
  #2071, #2075, #2082, #2190)
- [core] [006] Protobuf serializer support (#2100)
- [pipeline-api] [007] Support gRPC for mapUsingService (#2095, #2185)

### Enhancements

- [jet-cli] Use log4j2 instead of log4j (#1981)
- [jet-cli] Simplify default log output (#2047)
- [core] Add useful error message when serializer not registered (#2061)
- [jet-cli] Add hazelcast-azure cluster self-discovery plugin to the
  fat JAR in the distribution archive (#2079)
- [pipeline-api] First-class support for inner hash join (@TomaszGaweda
  #2089)
- [core] When Jet starts up, it now logs the cluster name (@caioguedes
  #2105)
- [core] Add useful error message when trying to deploy a JDK class with
  JobConfig (#2108)
- [core] Implement JobConfig.toString (@SapnaDerajeRadhakrishna #2152)
- [core] Do not destroy Observable on shutdown (#2170)

### Fixes

- [core] Don't send the interrupt signal to blocking threads when a job
  is terminating (#1971)
- [core] Consistently prefer YAML over XML config files when both
  present (#2033)

### Breaking Changes

- [avro] Replace Supplier<Schema> with just Schema for Avro Sink (#2005)
- [jms] Reorder parameters in JMS source so the lambda comes last
  (#2062)
- [jet-cli] Change smart routing (connecting to all cluster members)
  default to disabled (#2104)
- [pipeline-api] For xUsingServiceAsync transforms, reduce the default
  number of concurrent service calls per processor. Before: 256; now: 4.
  (#2204)
