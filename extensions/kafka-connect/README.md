# Kafka Connect Connector

From the [Confluent documentation](https://docs.confluent.io/current/connect/index.html):
> Kafka Connect, an open source component of Kafka, is a framework for connecting
> Kafka with external systems such as databases, key-value stores, search indexes,
> and file systems. Using Kafka Connect you can use existing connector
> implementations for common data sources and sinks to move data into and out of
> Kafka.

Take an existing Kafka Connect source and use it as a source for Hazelcast Jet
without the need to have a Kafka deployment. Hazelcast Jet will drive the
Kafka Connect connector and bring the data from external systems to the pipeline
directly.

## Connector Attributes

### Source Attributes

|  Atrribute  | Value |
|:-----------:|-------|
| Has Source  | Yes   |
|    Batch    | Yes   |
|   Stream    | Yes   |
| Distributed | No    |

### Sink Attributes

|  Atrribute  | Value |
|:-----------:|-------|
|  Has Sink   | No    |
| Distributed | No    |

## Getting Started

### Installing

The Kafka Connect Connector artifacts are published on the Maven repositories.

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-kafka-connect</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle:

```
compile group: 'com.hazelcast.jet', name: 'hazelcast-jet-kafka-connect', version: ${version}
```

### Usage

To use any Kafka Connect Connector as a source in your pipeline you need to
create a source by calling `KafkaConnectSources.connect()` method with the
`Properties` object. After that you can use your pipeline like any other source
in the Jet pipeline. The source will emit items in `SourceRecord` type from
Kafka Connect API, where you can access the key and value along with their
corresponding schemas. Hazelcast Jet will instantiate a single task for the
specified source in the cluster.

Following is an example pipeline which stream events from RabbitMQ, maps the
values to their string representation and logs them.

Beware the fact that you'll need to attach the Kafka Connect Connector of your
choice with the job that you are submitting.

```java
        Properties properties = new Properties();
        properties.setProperty("name","rabbitmq-source-connector");
        properties.setProperty("connector.class","com.github.jcustenborder.kafka.connect.rabbitmq.RabbitMQSourceConnector");
        properties.setProperty("kafka.topic","messages");
        properties.setProperty("rabbitmq.queue","test-queue");
        properties.setProperty("rabbitmq.host","???");
        properties.setProperty("rabbitmq.port","???");
        properties.setProperty("rabbitmq.username","???");
        properties.setProperty("rabbitmq.password","???");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaConnectSources.connect(properties))
        .withoutTimestamps()
        .map(record->Values.convertToString(record.valueSchema(),record.value()))
        .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip("/path/to/kafka-connect-rabbitmq-0.0.2-SNAPSHOT.zip");

        Job job=createHazelcastInstance().getJet().newJob(pipeline,jobConfig);
        job.join();
```

The pipeline will output records like the following:

```
INFO: [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] Output to ordinal 0: 
{
   "consumerTag":"amq.ctag-06l2oPQOnzjaGlAocCTzwg",
   "envelope":{
      "deliveryTag":100,
      "isRedeliver":false,
      "exchange":"ex",
      "routingKey":"test"
   },
   "basicProperties":{
      "contentType":"text/plain",
      "contentEncoding":"UTF-8",
      "headers":{

      },
      "deliveryMode":null,
      "priority":null,
      "correlationId":null,
      "replyTo":null,
      "expiration":null,
      "messageId":null,
      "timestamp":null,
      "type":null,
      "userId":"guest",
      "appId":null
   },
   "body":"Hello World!"
}
```

P.S. The record has been pretty printed for clarity.

### Fault-Tolerance

The Kafka Connect connectors driven by Jet are participating to store their state
snapshots (e.g partition offsets + any metadata which they might have to
recover/restart) in Jet. This way when the job is restarted they can recover
their state and continue to consume from where they left off. Since implementations
may vary between Kafka Connect modules, each will have different
behaviors when there is a failure. Please refer to the documentation of Kafka
Connect connector of your choice for detailed information.

### Running the tests

To run the tests run the command below:

```
./gradlew test
```
