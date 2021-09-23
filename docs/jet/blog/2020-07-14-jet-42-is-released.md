---
title: Jet 4.2 is Released
author: Can Gencer
authorURL: http://twitter.com/cgencer
authorImageURL: https://pbs.twimg.com/profile_images/1187734846749196288/elqWdrPj_400x400.jpg
---

Jet 4.2 is finally here! Here's an overview of what's new:

## Change Data Capture Support for MySQL and PostgreSQL

Previously, Jet has had support for [Debezium](https://debezium.io/) as
a contrib package. We're happy to announce that we've made several
improvements to this package and decided to make this a part of our main
release.

Debezium was developed initially as a Kafka Connect module, which can
read the snapshot and changes of relational databases such as MySQL and
PostgreSQL. Jet's Debezium integration removes the Kafka dependency
completely, and you can work with the stream of changes directly using
the full power of the Jet API.

Along with this change, we've created a new high-level API which makes
it easier to work directly with change stream records. For example, to
observe changes from MySQL, all you need to do do is:

```java
pipeline.readFrom(
    MySqlCdcSources.mysql("customers")
        .setDatabaseAddress("127.0.0.1")
        .setDatabasePort(3306)
        .setDatabaseUser("debezium")
        .setDatabasePassword("dbz")
        .setClusterName("dbserver1")
        .setDatabaseWhitelist("inventory")
        .setTableWhitelist("inventory.customers")
        .build()
    ).withNativeTimestamps(0)
    .writeTo(Sinks.logger());
```

You can also combine this feature with Jet's
[in-memory-storage](/docs/api/data-structures) allowing you to build an
in-memory replica of the database in just a few lines of code. The
example below will hydrate the distributed map `customers` with the
records from the database table with the same name:

```java
StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("source")
   ...
    .setTableWhitelist("inventory.customers")
    .build();

pipeline.readFrom(source)
        .withoutTimestamps()
        .peek()
        .writeTo(CdcSinks.map("customers",
                r -> r.key().toMap().get("id"),
                r -> r.value().toObject(Customer.class).toString())
        );
```

For a more in-depth example of this feature, see the [CDC
Tutorials](/docs/tutorials/cdc).

## ElasticSearch Connectors

We've also had ElasticSearch (5, 6, 7) connectors available as contrib
modules and happy to announce that they have also been through several
rounds of improvements and merged into the main release. A summary is
below:

- Support for slicing reads: Jet can use the slicing feature of
  ElasticSearch to read data in parallel.
- Collocated read and write: You can make use of collocated reading from
  ElasticSearch by placing Jet on the same nodes as your ElasticSearch
  cluster - this will significantly improve the speed of querying and
  ingestion.
- Improved the retry mechanism for writes: As Jet can be used to write
  to ElasticSearch as part of a streaming job, we've improved the retry
  mechanism so that transient ES cluster failures can be retries.

The ElasticSearch source and sink can be used with a simple API with the
example given below:

```java
BatchSource<String> elasticSource = new ElasticSourceBuilder<String>()
        .name("elastic-source")
        .clientFn(() -> RestClient.builder(new HttpHost(
                "localhost", 9200
        )))
        .searchRequestFn(() -> new SearchRequest("my-index"))
        .optionsFn(request -> RequestOptions.DEFAULT)
        .mapToItemFn(hit -> hit.getSourceAsString())
        .slicing(true)
        .build();

Sink<Map<String, Object>> elasticSink = ElasticSinks.elasticsearch(
    () -> client("user", "password", "host", 9200),
    item -> new IndexRequest("my-index").source(item)
);
```

See
[GitHub](https://github.com/hazelcast/hazelcast-jet/tree/master/examples/elastic)
for a full, end-to-end example.

## .rebalance() operator

Hazelcast Jet, by default, prefers not to send the data around the
computing cluster. If your data source retrieves some part of the data
stream on member A and you apply a mapping to it, the processing will
only happen on member A. If you have a non-distributed data source, this
may mean that all processing only happens on one member.

Jet 4.2 introduces the `.rebalance()` operator, which lets Jet
re-distribute data across the cluster at any point in the pipeline. To
use it is very simple:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.itemStream(1_000))
 .withIngestionTimestamps()
 .rebalance()
 .map(..)
```

For more details, please see the [documentation page](/docs/api/more-transforms#rebalance).

## Improved JSON Support

In previous versions of Jet, it was possible to read JSON files using
the file source, but it required some manual effort to set up the parsing
yourself. With 4.2, we're now making use of
[jackson-jr](https://github.com/FasterXML/jackson-jr) to parse JSON
files and offer a native JSON file source. The source provides support
for object mapping out of the box, so all you need to do is like below:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.json("/home/data/people", Person.class))
 .filter(person -> person.location().equals("NYC"))
 .writeTo(Sinks.logger());
```

Similarly, we have added a sink that can output JSON files
in the same way. For more details, please see the [documentation
page](/docs/api/sources-sinks#json-files).

### Support for JSON parsing

As part of JSON improvements, there is also now a built-in utility
method which you can use to parse JSON inside a pipeline:

```java
stage.map(json -> JsonUtil.beanFrom(json, Person.class));
```

See the [documentation page](/docs/api/more-transforms#json) for
additional instructions.

## Apache Pulsar Connector

[Apache Pulsar](https://pulsar.apache.org/) is a popular, fault-tolerant
pub-sub messaging system which is a good fit for stream processing
systems. A connector for Apache Pulsar is now also available as a
[contrib module](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/pulsar)
.

The source is fault-tolerant and can be used as below:

```java
StreamSource<Event> source = PulsarSources.pulsarReaderBuilder(
                topicName,
                () -> PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build(),
                () -> Schema.JSON(Event.class),
                Message::getValue).build();
```

For a more detailed example, you can see the [Apache Pulsar
Tutorial](/docs/tutorials/pulsar).

### Command-line and Docker Improvements

We've also made some improvements to the Jet command-line scripts. A
quick summary is below:

- You can now use the `config/jvm.options` file to control the JVM
  options when starting Jet without having to set environment variables.
- We've added support to export JMX metrics through Prometheus. It's
  enough to specify a `PROMETHEUS_PORT` environment variable to start
  exporting metrics using Prometheus.
- Rebuild the docker image using a multi-stage process for a
  smaller footprint.
- You can use the `JET_MODULES` environment variable to auto-import
  modules to Jet without having to copy files, which is especially
  useful in a docker environment.

### Documentation Improvements

As part of the release, we've also made improvements and added new
sections to the documentation:

- Revamped and simplified [Jet on
  Kubernetes](/docs/operations/kubernetes) documentation.
- Overhauled [Running With Docker](/docs/operations/docker) with
  additional information.
- A new section on [Garbage Collection](/docs/operations/gc-concerns) which
  was the result of our [extensive research and benchmarking](/blog/2020/06/09/jdk-gc-benchmarks-part1)
- Extended the documentation for [Python
  transformations](/docs/api/stateless-transforms#mapusingpython).
- Added additional docs about [adding timestamps to a
  stream](/docs/api/pipeline#adding-timestamps-to-a-stream).

## Full Release Notes

Members of the open source community that appear in these release notes:

- @caioguedes

Thank you for your valuable contributions!

### New Features

- [core] [011] Add JSON file source as well as built-in functions for
  parsing JSON strings (#2218, #2270)
- [pipeline-api] [008] Add support for stage rebalancing (#2149)
- [cdc] [005] New Change Data Capture Source for MySQL (#2142)
- [cdc] [005] New Change Data Capture Source for PostgreSQL (#2247)
- [cdc] [005] CDC Map Sink for keeping a Map in sync with a stream of
  changes from the database (#2262)
- [elasticsearch] [003] Added source and sink connectors for
  Elasticsearch 5, 6, 7 (#2098, #2286, #2287)

### Enhancements

- [core] Support Hazelcast Serialization for ProcessorSupplier (#2298)
- [core] Increase default parallelism for file and Avro source to 4
  (#2359)
- [pipeline-api] Support for keyFn and valueFn in map sink (#2198)
- [jet-cli] Introduce --targets as the default command for specifying
  where to connect and add it as a mixins for all comments (@caioguedes
  #2276)
- [jet-cli] Add support JET_MODULES environment variable to import
  modules automatically without having to copy them (#2314)
- [jet-cli] Support PROMETHEUS_PORT environment variable to start Jet
  with prometheus metrics enabled (#2328)
- [jet-cli] Add jvm.options file which can be used to specify JVM
  options during startup (#2349)
- [docker] Several Docker image improvements (hazelcast-jet-docker#27)
- [grpc] Performance improvements to gRPC module (#2245)

### Fixes

- [core] Fix potential ClassCastException in onSnapshotPhase2Completed()
  (#2338)
- [core] Fix JobConfig.attachFile path resolution on Windows (#2357)
- [jet-cli] Fix bad rolling filename causing misplaced files (#2270)
- [jet-cli] Use exec in jet-start to support ctrl-C in docker
  environment [#2307)
- [avro] Add missing serializer for Avro Utf-8 class (#2358)
