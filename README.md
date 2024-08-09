# Hazelcast

[![Slack](https://img.shields.io/badge/slack-chat-green.svg)](https://slack.hazelcast.com/) 
[![javadoc](https://javadoc.io/badge2/com.hazelcast/hazelcast/latest/javadoc.svg)](https://javadoc.io/doc/com.hazelcast/hazelcast/latest)
[![Docker pulls](https://img.shields.io/docker/pulls/hazelcast/hazelcast)](https://hub.docker.com/r/hazelcast/hazelcast)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=hazelcast_hazelcast&metric=alert_status)](https://sonarcloud.io/dashboard?id=hazelcast_hazelcast)

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

You can use [Slack](https://slack.hazelcast.com/) for getting help with Hazelcast

## How to Contribute

Thanks for your interest in contributing! The easiest way is to just send a pull
request. Have a look at the
[issues](https://github.com/hazelcast/hazelcast-jet/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
marked as good first issue for some guidance.

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

## Trigger Phrases in the Pull Request Conversation

When you create a pull request (PR), it must pass a build-and-test
procedure. Maintainers will be notified about your PR, and they can
trigger the build using special comments. These are the phrases you may
see used in the comments on your PR:

* `run-lab-run` - run the default PR builder
* `run-lts-compilers` - compiles the sources with JDK 17 and JDK 21 (without running tests)
* `run-ee-compile` - compile hazelcast-enterprise with this PR
* `run-ee-tests` - run tests from hazelcast-enterprise with this PR
* `run-windows` - run the EE and OS tests on a Windows machine (HighFive is not supported here)
  * `run-windows-os` - run the OS tests
  * `run-windows-ee` - run the EE tests
* `run-cdc-debezium-tests` - run all tests in the
  `extensions/cdc-debezium` module
* `run-cdc-mysql-tests` - run all tests in the `extensions/cdc-mysql`
  module
* `run-cdc-postgres-tests` - run all tests in the
  `extensions/cdc-postgres` module
* `run-mongodb-tests` - run all tests in the `extensions/mongodb` module
* `run-s3-tests` - run all tests in the `extensions/s3` module
* *`run-nightly-tests` - run nightly (slow) tests. WARNING: Use with care as this is a resource consuming task.*
* *`run-ee-nightly-tests` - run nightly (slow) tests from hazelcast-enterprise. WARNING: Use with care as this is a resource consuming task.*
* `run-sql-only` - run default tests in `hazelcast-sql`, `hazelcast-distribution`, and `extensions/mapstore` modules
* `run-docs-only` - do not run any tests, check that only files with `.md`, `.adoc` or `.txt` suffix are added in the PR
* `run-sonar` - run SonarCloud analysis
* `run-arm64` - run the tests on arm64 machine

Where not indicated, the builds run on a Linux machine with Oracle JDK 17.

### Creating PRs for Hazelcast SQL

When creating a PR with changes located in the `hazelcast-sql` module and nowhere else,
you can label your PR with `SQL-only`. This will change the standard PR builder to one that
will only run tests related to SQL (see `run-sql-only` above), which will significantly shorten
the build time vs. the default PR builder. **NOTE**: this job will fail if you've made changes
anywhere other than `hazelcast-sql`.

### Creating PRs which contain only documentation

When creating a PR which changes only documentation (files with suffix `.md` or `.adoc`) it 
makes no sense to run tests. For that case the label `docs-only` can be used. The job will fail 
in case you've made other changes than in `.md`, `.adoc` or `.txt` files.

## License

Source code in this repository is covered by one of two licenses:

 * [Apache License 2.0](https://docs.hazelcast.com/hazelcast/latest/index.html#licenses-and-support)
 * [Hazelcast Community
    License](http://hazelcast.com/hazelcast-community-license)

The default license throughout the repository is Apache License 2.0 unless the
header specifies another license.

## Acknowledgments
[![](https://www.yourkit.com/images/yklogo.png)](http://www.yourkit.com/)

Thanks to [YourKit](http://www.yourkit.com/) for supporting open source software
by providing us a free license for their Java profiler.

We owe (the good parts of) our CLI tool's user experience to
[picocli](https://picocli.info/).

## Copyright

Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
