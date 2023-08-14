# Support for Kafka Connect Source Connectors

### Table of Contents

* [Background](#background)
  * [Description](#description)
    * [Why](#why)
    * [How (short plan)](#how--short-plan-)
    * [Goals](#goals)
    * [Non-goals](#non-goals)
  * [What is the expected outcome?](#what-is-the-expected-outcome)
  * [Terminology](#terminology)
* [Functional Design](#functional-design)
  * [Summary of Functionality](#summary-of-functionality)
* [User Interaction](#user-interaction)
  * [API design and/or Prototypes](#api-design-andor-prototypes)
  * [Client Related Changes](#client-related-changes)
* [Technical Design](#technical-design)
  * [Fault-Tolerance](#fault-tolerance)
  * [Questions about the change](#questions-about-the-change)
  * [Questions about performance](#questions-about-performance)
  * [Stability questions](#stability-questions)
  * [Security questions](#security-questions)
  * [Observability and usage questions](#observability-and-usage-questions)
* [Testing Criteria](#testing-criteria)

|                                |                                                           |
|--------------------------------|-----------------------------------------------------------|
| Related Jira                   | [HZ-1510](https://hazelcast.atlassian.net/browse/HZ-1510) |
| Related Github issues          | None                                                      |
| Document Status / Completeness | DRAFT                                                     |
| Requirement owner              | Nandan Kidambi                                            |
| Developer(s)                   | Łukasz Dziedziul                                          |
| Quality Engineer               | Ondřej Lukáš                                              |
| Support Engineer               | _TBD_                                                     |
| Technical Reviewers            | František Hartman                                         |
| Simulator or Soak Test PR(s)   | _TBD_                                                     |

### Background

#### Description

From the [Confluent documentation](https://docs.confluent.io/current/connect/index.html):
> Kafka Connect, an open source component of Kafka, is a framework for connecting
> Kafka with external systems such as databases, key-value stores, search indexes,
> and file systems. Using Kafka Connect you can use existing connector
> implementations for common data sources and sinks to move data into and out of
> Kafka.

We want to take an existing Kafka Connect source connectors and use them as a source for Hazelcast Jet
without the need to have a Kafka deployment. Hazelcast Jet will drive the
Kafka Connect connector and bring the data from external systems to the pipeline
directly.

##### Why

- Reusing existing Kafka Connect ecosystem will give Hazelcast Platform way more broad range of external sources.
  Currently, there are 130 Kafka Connect Source connectors available (e.g. Neo4j, Couchbase, Scylla, Sap, Redis),
  see https://www.confluent.io/hub.
  Hazelcast officially has 30 dedicated connectors: https://docs.hazelcast.com/hazelcast/latest/integrate/connectors
- To take our connectors' maintenance burden off our shoulders.
- To make Hazelcast platform a drop-in replacement (as fair as possible) for simple use of Kafka Connect/Kafka Streams.
- To provide more seamless migration from Kafka to Hazelcast
- Hazelcast will be more user-friendly by providing more low-code integrations to the table.

##### How (short plan)

Revamp [existing implementation](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/kafka-connect)
of a custom Hazelcast source connector which can load Kafka Source Connectors and run it on Jet machinery without need
for the Kafka cluster.

##### Goals

- Use Kafka Connect source connectors as data source in Jet pipelines

##### Non-goals

- Support Kafka Connect sink connectors (maybe in the next phase?)
- Support Kafka Connect converters
- Support Kafka Connect transforms

#### What is the expected outcome?

- Attract new customers by making migration from Kafka easier and by providing more ways of integration
  with other 3rd party systems.
- Potentially we could free customers using Kafka Connect from Kafka Infrastructure

#### Terminology

| Term                   | Definition                                                                                                                                                                        |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Kafka Source Connector | Kafka Connect component ingesting data from an external system into Kafka topics.                                                                                                 |
| Kafka Source Connector | Kafka Connect component writing data from Kafka topics to external systems.                                                                                                       |
| Kafka Transforms       | Kafka Connect component modifying data from a source connector before it is written to Kafka, and modifying data read from Kafka before it’s written to the sink connector.       |
| Kafka Converter        | Kafka Connect component serializing the data when storing record from the source connector to the Kafka topic and deserializing from the topic into this internal representation. |

### Functional Design

#### Summary of Functionality

Provide a list of functions user(s) can perform.

- Use prebuilt Kafka Connectors from https://www.confluent.io/hub/ as a source of data in the pipeline submitted to the
  Platform

### User Interaction

#### API design and/or Prototypes

To use any Kafka Connect Connector as a source in your pipeline you need to create a source by
calling `KafkaConnectSources.connect()`
method with the Properties object. After that you can use your pipeline like any other source in the Jet pipeline.
The source will emit items in `SourceRecord` type from Kafka Connect API, where you can access the key and value along
with their corresponding schemas. Hazelcast Jet will instantiate a single task for the specified source in the cluster.
You need to make sure the source connector is available on the classpath, either by putting its jar to the classpath of
the members or by uploading the connector jar as a part for the job config.

Besides that you need to provide set of the properties used by the connector. Some of them are
[common](https://docs.confluent.io/platform/current/installation/configuration/connect/source-connect-configs.html#kconnect-long-source-configuration-properties-for-cp)
while the other connector-specific. Refer to the individual connector documentation for connector-specific configuration
properties.

Only `name` and `connector.class` properties are mandatory.

Example pipeline using Kafka-based `RandomSourceConnector` to generate random numbers

```java
        Properties randomProperties = new Properties();
        //mandatory properties
        randomProperties.setProperty("name", "random-source-connector");
        randomProperties.setProperty("connector.class", "sasakitoa.kafka.connect.random.RandomSourceConnector");
        
        //common properties
        randomProperties.setProperty("tasks.max", "1");
        
        //connector-specific properties
        randomProperties.setProperty("generator.class", "sasakitoa.kafka.connect.random.generator.RandomInt");
        randomProperties.setProperty("messages.per.second", "1000");
        randomProperties.setProperty("topic", "test");
        randomProperties.setProperty("task.summary.enable", "true");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaConnectSources.connect(randomProperties))
                .withoutTimestamps()
                .map(record -> Values.convertToString(record.valueSchema(), record.value()))
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(Objects.requireNonNull(this.getClass()
                .getClassLoader()
                .getResource("random-connector-1.0-SNAPSHOT.jar"))
        .getPath()
        );

        Job job = createHazelcastInstance().getJet().newJob(pipeline, jobConfig);
```

#### Client Related Changes

No changes required

### Technical Design

In the `hazelcast-jet-contrib` repository we already have an initial implementation of the `KafkaConnectSources`.

`KafkaConnectSources` instantiates the `KafkaConnectSource`, which is used then to create as task object which performs
the actual ingestion logic.
Everything is orchestrated by the `SourceBuilder`
methods: `createFn`, `fillBufferFn`, `createSnapshotFn`, `restoreSnapshotFn`, `destroyFn`.

The current implementation uses `SourceBuilder` to create a stream source of `SourceRecord`.
`SourceRecord` is Kafka data type returned by the Kafka Source Connectors for further processing.

The plan is to move the code to the main hazelcast repository and make it production ready with the following steps:

- Move Kafka Connect to Hazelcast Extensions module
- Add Kafka connect source scaling (currently Hazelcast Platform will instantiate a single task for the specified source
  in the cluster)
- Add unit tests for Kafka connect source
- Add integration tests for Kafka connect source
- Add documentation to official hazelcast documentation

There are also nice to have features:

- Expose Kafka connect as a SQL mapping
- Use custom processor classpath if needed
- Add Kafka connect metrics
- Add benchmarks for Kafka connect source
- Add Soak tests for Kafka connect source

#### Fault-Tolerance

The Kafka Connect connectors driven by Jet are participating to store their state snapshots (e.g partition offsets +
any metadata which they might have to recover/restart) in Jet. This way when the job is restarted they can recover
their state and continue to consume from where they left off. Since implementations may vary between Kafka Connect
modules, each will have different behaviors when there is a failure. Please refer to the documentation of Kafka Connect
connector of your choice for detailed information.

#### Questions about the change

- What components in Hazelcast need to change? How do they change?
  * N/A

- How does this work in an on-prem deployment?
  * N/A

- How about on AWS and Kubernetes, platform operator?
  * As long as necessary jars are on the classpath it doesn't have deployment requirements.

- How does this work in Cloud Viridan clusters?
  * Same as above, it's code-configured.

- How does the change behave in mixed-version deployments? During a version upgrade? Which migrations are needed?
  * It's a new type of the Jet sources, no backward compatibility to handle.

- What are the possible interactions with other features or sub-systems inside Hazelcast? How does the behavior of other
  code change implicitly as a result of the changes outlined in the design document?
  * N/A

- What are the edge cases? What are example uses or inputs that we think are uncommon but are still possible and thus
  need to be handled? How are these edge cases handled?
  * There could be some incompatibilities with more advanced Kafka Source connectors, but first we need to test some of
    them to discover the problems

- What are the effect of possible mistakes by other Hazelcast team members trying to use the feature in their own code?
  How does the change impact how they will troubleshoot things?
  * It won't be reusable in other context. Troubleshooting will be the same as any other source/sink/connector for Jet.

- Mention alternatives, risks and assumptions. Why is this design the best in the space of possible designs? What other
  designs have been considered and what is the rationale for not choosing them?

  * Other alternatives:
    - leave implementation to users (bad: requires code duplication, no official support is also not good)
    - keep connector in `hazelcast-jet-contrib` repository - but it's not as well tested as main repo and less visible
    - Use Kafka Infrastructure and Hazelcast Kafka Connector, although it requires more infrastructure effort.
      See https://docs.hazelcast.com/hazelcast/latest/integrate/kafka-connector

- The most common mistakes by other Hazelcast users I can think of are:
  - property misconfiguration
  - fail or forget to upload the connector jar

#### Questions about performance

- Does the change impact performance? How?
  * Won't affect performance of other subsystems

- How is resource usage affected for “large” loads? For example, what do we expect to happen when there are 100000
  items/entries? 100000 data structures? 1000000 concurrent operations?
  * It depends on Kafka source connector implementation

#### Stability questions

- Can this new functionality affect the stability of a node or the entire cluster? How does the behavior of a node
  or a cluster degrade if there is an error in the implementation?
  * The same as any other jet job can affect the cluster, most probably it can affect stability if too much data is
    ingested and there's not enough memory.
- Can the new functionality affect clusters which are not explicitly using it?
  * No

#### Security questions

- Does the change concern authentication or authorization logic? If so, mention this explicitly tag the relevant
  security-minded reviewer as reviewer to the design document.
  * N/A
- Does the change create a new way to communicate data over the network? What rules are in place to ensure that this
  cannot be used by a malicious user to extract confidential data? -
  * Depends on the used Kafka source connector which can connect to external systems.
- Is there telemetry or crash reporting? What mechanisms are used to ensure no sensitive data is accidentally exposed?
  * Same as any Jet source.

#### Observability and usage questions

- Is the change affecting asynchronous / background subsystems?
  * No

- If so, how can users and our team observe the run-time state via tracing? - _TBD_

  - Is usage of the new feature observable in telemetry? If so, mention where in the code telemetry counters or
    metrics would be added. - _TBD_

  - What might be the valuable metrics that could be shown for this feature in Management Center and/or Viridan
    Control Plane? - number of events, check metrics infrastructure, check kafka connect metrics and monitoring that we
    are missing
  - Should this feature be configured, enabled/disabled or managed from the Management Center? How do you think your
    change affects Management Center? - N/A
  - Does the feature require or allow runtime changes to the member configuration (XML/YAML/programmatic)? N/A

- Which other inspection APIs exist?
  * Standard Jet job status APIs.

- Are there new APIs, or API changes (either internal or external)?
  * Yes, new Source
- How would you document the new APIs? Include example usage.
  * Documentation in Javadocs
  * How-to guide in documentation.
- Which principles did you apply to ensure the APIs are consistent with other related features / APIs? (Cross-reference
  other APIs that are similar or related, for comparison.)
  * Processors will be done in similar manner to other sources.

- Is the change visible to users of Hazelcast or operators who run Hazelcast clusters?
  * No
- Are there any user experience (UX) changes needed as a result of this change?
  * No
- Are the UX changes necessary or clearly beneficial? (Cross-reference the motivation section.)
  * No
- Which principles did you apply to ensure the user experience (UX) is consistent with other related features? (
  Cross-reference other features that have related UX, for comparison.)
  * Use same structure as other connectors.
- Is usage of the new feature observable in telemetry? If so, mention where in the code telemetry counters or metrics
  would be added.
  * TBD - maybe number of events/source records and other based on existing Kafka Connect metrics.
- Should this feature be configured, enabled/disabled or managed from the Management Center? How do you think your
  change affects Management Center?
  * No, it's code-only feature.
- Does the feature require or allow runtime changes to the member configuration (XML/YAML/programmatic)?
  * No
- Are usage statistics for this feature reported in Phone Home? If not, why?
  * TBD

### Testing Criteria

Describe testing approach to developed functionality

- Soak testing for memory leaks and stable performance - _TBD_
- Security related tests - N/A
- Create integration tests with some typical/popular Kafka Connect Source connectors

### Resources

- [TDD on GitHub: hazelcast/hazelcast/pull/23303](https://github.com/hazelcast/hazelcast/pull/23303)
- [Hazelcast: Creating a Custom Streaming Source](https://docs.hazelcast.com/hazelcast/latest/pipelines/custom-stream-source)
- [Kafka Connect Concepts | Confluent Documentation](https://docs.confluent.io/platform/current/connect/concepts.html#kconnect-long-concepts)
- [Kafka Connect | Confluent Documentation](https://docs.confluent.io/platform/current/connect/index.html)
- [Kafka Connect Deep Dive – Converters and Serialization Explained | Confluent](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/)
- [Guide for Kafka Connector Developers | Confluent Documentation](https://docs.confluent.io/platform/current/connect/devguide.html)
- [GitHub: hazelcast/hazelcast-jet-contrib/kafka-connect](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/kafka-connect)
- [GitHub: debezium/debezium/debezium-embedded](https://github.com/debezium/debezium/tree/main/debezium-embedded)

