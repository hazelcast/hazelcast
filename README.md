# Hazelcast

[![Slack](https://img.shields.io/badge/slack-chat-green.svg)](https://slack.hazelcast.com/) 
[![javadoc](https://javadoc.io/badge2/com.hazelcast/hazelcast/latest/javadoc.svg)](https://javadoc.io/doc/com.hazelcast/hazelcast/latest)
[![Docker pulls](https://img.shields.io/docker/pulls/hazelcast/hazelcast)](https://hub.docker.com/r/hazelcast/hazelcast)

----

## What is Hazelcast

The world’s leading companies trust Hazelcast to modernize applications and take instant action on data in motion to create new revenue streams, mitigate risk, and operate more efficiently. Businesses use Hazelcast’s unified **real-time data platform** to process **streaming** data, enrich it with historical context and take instant action with standard or **ML/AI-driven automation** - before it is stored in a database or data lake. 

Hazelcast is named in the Gartner Market Guide to Event Stream Processing and a leader in the GigaOm Radar Report for Streaming Data Platforms. To join our community of CXOs, architects and developers at brands such as Lowe’s, HSBC, JPMorgan Chase, Volvo, New York Life, and others, visit [hazelcast.com](https://hazelcast.com).

## When to use Hazelcast

Hazelcast provides a platform that can handle multiple types of workloads for
building real-time applications.

* Stateful data processing over streaming data or data at rest
* Querying streaming and batch data sources directly using SQL
* Ingesting data through a library of connectors and serving it using
  low-latency SQL queries
* Pushing updates to applications on events
* Low-latency queue-based or pub-sub messaging  
* Fast access to contextual and transactional data via caching patterns such as
  read/write-through and write-behind
* Distributed coordination for microservices
* Replicating data from one region to another or between data centers in the
  same region

## Key Features

* [Stateful and fault-tolerant data processing and querying over data streams
  and data](#stateful-data-Processing) at rest using [SQL](https://docs.hazelcast.com/hazelcast/latest/sql/sql-overview) or dataflow API
* [A comprehensive library of connectors such as Kafka, Hadoop, S3, RDBMS, JMS
  and many more](https://docs.hazelcast.com/hazelcast/latest/integrate/connectors)
* Distributed messaging using [pub-sub](https://docs.hazelcast.com/hazelcast/latest/data-structures/topic.html) and [queues](https://docs.hazelcast.com/hazelcast/latest/data-structures/queue.html)
* [Distributed, partitioned, queryable key-value store with event listeners,
  which can also be used to store contextual data for enriching event streams
  with low latency](https://docs.hazelcast.com/hazelcast/latest/data-structures/map)
* Tight integration for deploying machine learning models with Python to a data
  processing pipeline
* Cloud-native, run everywhere architecture
* Zero-downtime operations with rolling upgrades
* At-least-once and exactly-once processing guarantees for stream processing
  pipelines
* Data replication between data centers and geographic regions using WAN 
* Microsecond performance for key-value point lookups and pub-sub
* Unique data processing architecture results in 99.99% latency of under 10ms
  for streaming queries with millions of events per second.
* Client libraries in [Java](https://github.com/hazelcast/hazelcast),
 [Python](https://github.com/hazelcast/hazelcast-python-client), [Node.js](https://github.com/hazelcast/hazelcast-nodejs-client), [.NET](https://github.com/hazelcast/hazelcast-csharp-client), [C++](https://github.com/hazelcast/hazelcast-cpp-client) and [Go](https://github.com/hazelcast/hazelcast-go-client)

### Stateful Data Processing

Hazelcast has a built-in data processing engine called
[Jet](https://docs.hazelcast.com/hazelcast/latest/pipelines/overview#what-is-the-jet-engine), which can be used to build both streaming/real-time
and batch/static data pipelines that are elastic. A single node of Hazelcast has been proven to [aggregate 10 million
events per second](https://foojay.io/today/sub-10-ms-latency-in-java-concurrent-gc-with-green-threads/) with
latency under 10 milliseconds. A cluster of Hazelcast nodes can process [billion
events per
second](https://hazelcast.com/blog/billion-events-per-second-with-millisecond-latency-streaming-analytics-at-giga-scale/).

## Get Started

Follow the [Getting Started
Guide](https://docs.hazelcast.com/hazelcast/latest/getting-started/install-hazelcast)
to install and start using Hazelcast.

## Documentation

Read the [documentation](https://docs.hazelcast.com/) for
in-depth details about how to install Hazelcast and an overview of the features.

## Get Help

You can use [Slack](https://slack.hazelcast.com/) for getting help with Hazelcast.

## How to Contribute

Thanks for your interest in contributing! The easiest way is to just send a pull
request.

### Building From Source

Building Hazelcast requires at minimum JDK 17. Pull the latest source from the
repository and use Maven install (or package) to build:

```bash
$ git pull origin master
$ ./mvnw clean package -DskipTests
```

It is recommended to use the included Maven wrapper script.
It is also possible to use local Maven distribution with the same 
version that is used in the Maven wrapper script.

Additionally, there is a `quick` build activated by setting the `-Dquick` system
property that skips validation tasks for faster local builds (e.g. tests, checkstyle
validation, javadoc, source plugins etc) and does not build `extensions` and `distribution` 
modules.

### Testing

Take into account that the default build executes thousands of tests which may
take a considerable amount of time. Hazelcast has 3 testing profiles:

* Default: 
  ```bash
  ./mvnw test
  ```
to run quick/integration tests (those can be run
  in parallel without using network by using `-P parallelTest` profile).
* Slow Tests: 
  ```bash
  ./mvnw test -P nightly-build
  ```
to run tests that are either slow
  or cannot be run in parallel.
* All Tests:
  ```bash
  ./mvnw test -P all-tests
  ```
to run all tests serially using
  network.

Some tests require Docker to run. Set `-Dhazelcast.disable.docker.tests` system property to ignore them.

When developing a PR it is sufficient to run your new tests and some 
related subset of tests locally. Our PR builder will take care of running
the full test suite.

## License

Source code in this repository is covered by one of two licenses:

 * [Apache License 2.0](https://docs.hazelcast.com/hazelcast/latest/index.html#licenses-and-support)
 * [Hazelcast Community
    License](http://hazelcast.com/hazelcast-community-license)

The default license throughout the repository is Apache License 2.0 unless the
header specifies another license.

## Acknowledgments
We owe (the good parts of) our CLI tool's user experience to
[picocli](https://picocli.info/).

## Copyright

Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
