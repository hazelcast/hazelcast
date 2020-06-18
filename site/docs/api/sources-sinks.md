---
title: Sources and Sinks
description: Birds-eye view of all pre-defined sources available in Jet.
---

Hazelcast Jet comes out of the box with many different sources and sinks
that you can work with, that are also referred to as _connectors_.

## Files

File sources generally involve reading a set of (as in "multiple") files
from either a local/network disk or a distributed file system such as
Amazon S3 or Hadoop. Most file sources and sinks are batch oriented, but
the sinks that support _rolling_ capability can also be used as sinks in
streaming jobs.

### Local Disk

The simplest file source is designed to work with both local and network
file systems. This source is text-oriented and reads the files line by
line and emits a record per line.

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.files("/home/data/web-logs"))
 .map(line -> LogParser.parse(line))
 .filter(log -> log.level().equals("ERROR"))
 .writeTo(Sinks.logger());
```

#### JSON Files

For JSON files, the source expects the content of the files as
[streaming JSON](https://en.wikipedia.org/wiki/JSON_streaming) content,
where each JSON string is separated by a new-line. The JSON string
itself can span on multiple lines. The source converts each JSON string
to an object of given type or to a `Map` if no type is specified:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.json("/home/data/people", Person.class))
 .filter(person -> person.location().equals("NYC"))
 .writeTo(Sinks.logger());
```

Jet uses the lightweight JSON library `jackson-jr` to parse the given
input or to convert the given objects to JSON string. You can use
[Jackson Annotations](https://github.com/FasterXML/jackson-annotations/wiki/Jackson-Annotations)
by adding `jackson-annotations` library to the classpath, for example:

```java
public class Person {

    private long personId;
    private String name;

    @JsonGetter("id")
    public long getPersonId() {
      return this.personId;
    }

    @JsonSetter("id")
    public void setPersonId(long personId) {
      this.personId = personId;
    }

    public String getName() {
       return name;
    }

    public void setName(String name) {
      this.name = name;
    }
}
```

#### CSV

For CSV files or for parsing files in other custom formats it's possible
to use the `filesBuilder` source:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.filesBuilder(sourceDir).glob("*.csv").build(path ->
    Files.lines(path).skip(1).map(SalesRecordLine::parse))
).writeTo(Sinks.logger());
```

#### Data Locality for Files

For a local file system, the sources expect to see on each node just the
files that node should read. You can achieve the effect of a distributed
source if you manually prepare a different set of files on each node.
For shared file system, the sources can split the work so that each node
will read a part of the files by configuring the option
`FilesBuilder.sharedFileSystem()`.

#### File Sink

The file sink, like the source works with text and creates a line of
output for each record. When the rolling option is used it will roll the
filename to a new one once the criteria is met. It supports rolling by
size or date. The following will roll to a new file every hour:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(100))
 .withoutTimestamps()
 .writeTo(Sinks.filesBuilder("out")
 .rollByDate("YYYY-MM-dd.HH")
 .build());
```

To write JSON files, you can use `Sinks.json` or `Sinks.filesBuilder`
with `JsonUtil.toJson` as `toStringFn`. Sink converts each item to JSON
string and writes it as a new line to the file:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(100))
 .withoutTimestamps()
 .writeTo(Sinks.json("out"));
```

Each node will write to a unique file with a numerical index. You can
achieve the effect of a distributed sink if you manually collect all the
output files on all members and combine their contents.

The sink also supports exactly-once processing and can work
transactionally.

#### File Watcher

File watcher is a streaming file source, where only the new files or
appended lines are emitted. If the files are modified in more complex
ways, the behavior is undefined.

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.fileWatcher("/home/data"))
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

You can create streaming file source for JSON files too:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.jsonWatcher("/home/data", Person.class))
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

### Apache Avro

[Apache Avro](https://avro.apache.org/) is a binary data storage format
which is schema based. The connectors are similar to the local file
connectors, but work with binary files stored in _Avro Object Container
File_ format.

To use the Avro connector, you need to copy the `hazelcast-jet-avro`
module from the `opt` folder to the `lib` folder and add the following
dependency to your application:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-avro:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.hazelcast.jet</groupId>
  <artifactId>hazelcast-jet-avro</artifactId>
  <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

With Avro sources, you can use either the `SpecificReader` or
`DatumReader` depending on the data type:

```java
Pipeline p = Pipeline.create();
p.readFrom(AvroSources.files("/home/data", Person.class))
 .filter(person -> person.age() > 30)
 .writeTo(Sinks.logger());
```

The sink expects a schema and the type to be written:

```java
p.writeTo(AvroSinks.files(DIRECTORY_NAME, Person.getClassSchema()), Person.class))
```

### Hadoop InputFormat/OutputFormat

You can use Hadoop connector to read/write files from/to Hadoop
Distributed File System (HDFS), local file system, or any other system
which has Hadoop connectors, including various cloud storages. Jet was
tested with:

* Amazon S3
* Google Cloud Storage
* Azure Cloud Storage
* Azure Data Lake

The Hadoop source and sink require a configuration object of type
[Configuration](https://hadoop.apache.org/docs/r2.10.0/api/org/apache/hadoop/conf/Configuration.html)
which supplies the input and output paths and formats. They don’t
actually create a MapReduce job, this config is simply used to describe
the required inputs and outputs. You can share the same `Configuration`
instance between several source/sink instances.

For example, to do a canonical word count on a Hadoop data source,
we can use the following pipeline:

```java
Job job = Job.getInstance();
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
TextInputFormat.addInputPath(job, new Path("input-path"));
TextOutputFormat.setOutputPath(job, new Path("output-path"));
Configuration configuration = job.getConfiguration();

Pipeline p = Pipeline.create();
p.readFrom(HadoopSources.inputFormat(configuration, (k, v) -> v.toString()))
 .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
 .groupingKey(word -> word)
 .aggregate(AggregateOperations.counting())
 .writeTo(HadoopSinks.outputFormat(configuration));
```

The Hadoop source and sink will use either the new or the old MapReduce
API based on the input format configuration.

Each processor will write to a different file in the output folder
identified by the unique processor id. The files will be in a temporary
state until the job is completed and will be committed when the job is
complete. For streaming jobs, they will be committed when the job is
cancelled. We have plans to introduce a rolling sink for Hadoop in the
future to have better streaming support.

#### Data Locality

Jet will split the input data across the cluster, with each processor
instance reading a part of the input. If the Jet nodes are co-located
with the Hadoop data nodes, then Jet can make use of data locality by
reading the blocks locally where possible. This can bring a significant
increase in read throughput.

#### Serialization and Writables

Hadoop types implement their own serialization mechanism through the use
of `Writable` types. Jet provides an adapter to register a `Writable`
for [Hazelcast serialization](serialization) without having to write
additional serialization code. To use this adapter, you can register
your own `Writable` types by extending `WritableSerializerHook` and
registering the hook.

#### Hadoop Classpath

To use the Hadoop connector, you need to copy the `hazelcast-jet-hadoop`
module from the `opt` folder to the `lib` folder and add the following
dependency to your application:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-hadoop:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.hazelcast.jet</groupId>
  <artifactId>hazelcast-jet-hadoop</artifactId>
  <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

When submitting Jet jobs using Hadoop, sending Hadoop JARs should be
avoided and instead the Hadoop classpath should be used. Hadoop JARs
contain some JVM hooks and can keep lingering references inside the JVM
long after the job has ended, causing memory leaks.

To obtain the hadoop classpath, use the `hadoop classpath` command and
append the output to the `CLASSPATH` environment variable before
starting Jet.

### Amazon S3

The Amazon S3 connectors are text-based connectors that can read and
write files to Amazon S3 storage.

The connectors expect the user to provide either an `S3Client` instance
or credentials (or using the default ones) to create the client. The
source and sink assume the data is in the form of plain text and
emit/receive data items which represent individual lines of text.

```java
AwsBasicCredentials credentials = AwsBasicCredentials.create("accessKeyId", "accessKeySecret");
S3Client s3 = S3Client.builder()
    .credentialsProvider(StaticCredentialsProvider.create(credentials))
    .build();

Pipeline p = Pipeline.create();
p.readFrom(S3Sources.s3(singletonList("input-bucket"), "prefix",
    () -> S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(credentials)).build())
 .filter(line -> line.contains("ERROR"))
 .writeTo(Sinks.logger());
```

The S3 sink works similar to the local file sink, writing a line to the
output for each input item:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("the", "brown", "fox"))
 .writeTo(S3Sinks.s3("output-bucket", () -> S3Client.create()));
```

The sink creates an object in the bucket for each processor instance.
Name of the file will include a user provided prefix (if defined),
followed by the processor’s global index. For example the processor
having the index `2` with prefix `my-object-` will create the object
`my-object-2`.

S3 sink uses the multi-part upload feature of S3 SDK. The sink buffers
the items to parts and uploads them after buffer reaches to the
threshold. The multi-part upload is completed when the job completes and
makes the objects available on the S3. Since a streaming jobs never
complete, S3 sink is not currently applicable to streaming jobs.

To use the S3 connector, you need to add the `hazelcast-jet-s3`
module to the `lib` folder and the following dependency to your
application:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-s3:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.hazelcast.jet</groupId>
  <artifactId>hazelcast-jet-s3</artifactId>
  <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Messaging Systems

Messaging systems allow multiple application to communicate
asynchronously without a direct link between them. These types of
systems are a great fit for a stream processing engine like Jet since
Jet is able to consume messages from these systems and process them in
real time.

### Apache Kafka

Apache Kafka is a popular distributed, persistent log store which is a
great fit for stream processing systems. Data in Kafka is structured
as _topics_ and each topic consists of one or more partitions, stored in
the Kafka cluster.

To read from Kafka, the only requirements are to provide deserializers
and a topic name:

```java
Properties props = new Properties();
props.setProperty("bootstrap.servers", "localhost:9092");
props.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
props.setProperty("auto.offset.reset", "earliest");

Pipeline p = Pipeline.create();
p.readFrom(KafkaSources.kafka(props, "topic"))
 .withNativeTimestamps(0)
 .writeTo(Sinks.logger());
```

The topics and partitions are distributed across the Jet cluster, so
that each node is responsible for reading a subset of the data.

When used as a sink, then the only requirements are the serializers:

```java
Properties props = new Properties();
props.setProperty("bootstrap.servers", "localhost:9092");
props.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());

Pipeline p = Pipeline.create();
p.readFrom(Sources.files("home/logs"))
 .map(line -> LogParser.parse(line))
 .map(log -> entry(log.service(), log.message()))
 .writeTo(KafkaSinks.kafka(props, "topic"));
```

To use the Kafka connector, you need to copy the `hazelcast-jet-kafka`
module from the `opt` folder to the `lib` folder and add the following
dependency to your application:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy

compile 'com.hazelcast.jet:hazelcast-jet-kafka:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.hazelcast.jet</groupId>
  <artifactId>hazelcast-jet-kafka</artifactId>
  <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Fault-tolerance

One of the most important features of using Kafka as a source is that
it's possible to replay data - which enables fault-tolerance. If the job
has a processing guarantee configured, then Jet will periodically save
the current offsets internally and then replay from the saved offset
when the job is restarted. In this mode, Jet will manually track and
commit offsets, without interacting with the consumer groups feature of
Kafka.

If processing guarantee is disabled, the source will start reading from
default offsets (based on the `auto.offset.reset property`). You can
enable offset committing by assigning a `group.id`, enabling auto offset
committing using `enable.auto.commit` and configuring
`auto.commit.interval.ms` in the given properties. Refer to
[Kafka documentation](https://kafka.apache.org/22/documentation.html)
for the descriptions of these properties.

#### Transactional guarantees

As a sink, it provides exactly-once guarantees at the cost of using
Kafka transactions: Jet commits the produced records after each snapshot
is completed. This greatly increases the latency because consumers see
the records only after they are committed.

If you use at-least-once guarantee, records are visible immediately, but
in the case of a failure some records could be duplicated. You
can also have the job in exactly-once mode and decrease the guarantee
just for a particular Kafka sink.

#### Schema Registry

Kafka is often used together with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html)
as a repository of types. The use of the schema registry is done through
adding it to the `Properties` object and using the `KafkaAvroSerializer/Deserializer`
if Avro is being used:

```java
properties.put("value.deserializer", KafkaAvroDeserializer.class);
properties.put("specific.avro.reader", true);
properties.put("schema.registry.url", schemaRegistryUrl);
```

Keep in mind that once the record deserialized, Jet still needs to know
how to serialize/deserialize the record internally. Please refer to the
[Serialization](serialization) section for details.

#### Version Compatibility

The Kafka sink and source are based on version 2.2.0, this means Kafka
connector will work with any client and broker having version equal to
or greater than 1.0.0.

### JMS

JMS (Java Message Service) is a standard API for communicating with
various message brokers using the queue or publish-subscribe patterns.

There are several brokers that implement the JMS standard, including:

* Apache ActiveMQ and ActiveMQ Artemis
* Amazon SQS
* IBM MQ
* RabbitMQ
* Solace
* ...

Jet is able to utilize these brokers both as a source and a sink through
the use of the JMS API.

To use a JMS broker, such as ActiveMQ, you need the client libraries
either on the classpath (by putting them into the `lib` folder) of the
node or submit them with the job. The Jet JMS connector is a part of the
`hazelcast-jet` module, so requires no other dependencies than the
client jar.

#### JMS Source Connector

A very simple pipeline which consumes messages from a given ActiveMQ
queue and then logs them is given below:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.jmsQueue("queueName",
        () -> new ActiveMQConnectionFactory("tcp://localhost:61616")))
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

For a topic you can choose whether the consumer is durable or shared.
You need to use the `consumerFn` to create the desired consumer using a
JMS `Session` object.

If you create a shared consumer, you need to let Jet know by calling
`sharedConsumer(true)` on the builder. If you don't do this, only one
cluster member will actually connect to the JMS broker and will receive
all of the messages. We always assume a shared consumer for queues.

If you create a non-durable consumer, the fault-tolerance features won't
work since the JMS broker won't track which messages were delivered to
the client and which not.

Below is a simple example to create a non-durable non-shared topic
source:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.jmsTopic("topic",
        () -> new ActiveMQConnectionFactory("tcp://localhost:61616")))
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

Here is a more complex example that uses a shared, durable consumer:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources
        .jmsTopicBuilder(() ->
                new ActiveMQConnectionFactory("tcp://localhost:61616"))
        .sharedConsumer(true)
        .consumerFn(session -> {
            Topic topic = session.createTopic("topic");
            return session.createSharedDurableConsumer(topic, "consumer-name");
        })
        .build())
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

#### Source fault tolerance

The source connector is fault-tolerant with the exactly-once guarantee
(except for the non-durable topic consumer). Fault tolerance is achieved
by acknowledging the consumed messages only after they were fully
processed by the downstream stages. Acknowledging is done once per
snapshot, you need to enable the processing guarantee in the
`JobConfig`.

In the exactly-once mode the processor saves the IDs of the messages
processed since the last snapshot into the snapshotted state. Therefore
this mode will not work if your messages don't have the JMS Message ID
set (it is an optional feature of JMS). In this case you need to set
`messageIdFn` on the builder to extract the message ID from the payload.
If you don't have a message ID to use, you must reduce the source
guarantee to at-least-once:

```java
p.readFrom(Sources.jmsTopicBuilder(...)
        .maxGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
        ...
```

In the at-least-once mode messages are acknowledged in the same way as
in the exactly-once mode, but message IDs are not saved to the snapshot.

If you have no processing guarantee enabled, the processor will consume
the messages in the `DUPS_OK_ACKNOWLEDGE` mode.

#### JMS Sink Connector

The JMS sink uses the supplied function to create a `Message` object for
each input item. The following code snippets show writing to a JMS queue
and a JMS topic using the ActiveMQ JMS client.

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.list("inputList"))
 .writeTo(Sinks.jmsQueue("queue",
         () -> new ActiveMQConnectionFactory("tcp://localhost:61616"))
 );
```

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.list("inputList"))
 .writeTo(Sinks.jmsTopic("topic",
        () -> new ActiveMQConnectionFactory("tcp://localhost:61616"))
 );
```

#### Fault Tolerance

The JMS sink supports the exactly-once guarantee. It uses two-phase XA
transactions, messages are committed consistent with the last state
snapshot. This greatly increases the latency, it is determined by the
snapshot interval: messages are visible to consumers only after the
commit. In order to make it work, the connection factory you provide has
to implement `javax.jms.XAConnectionFactory`, otherwise the job will not
start.

If you want to avoid the higher latency, decrease the overhead
introduced by the XA transactions, if your JMS implementation doesn't
support XA transactions or if you just don't need the guarantee, you can
reduce it just for the sink:

```java
stage.writeTo(Sinks
         .jmsQueueBuilder(() -> new ActiveMQConnectionFactory("tcp://localhost:61616"))
         // decrease the guarantee for the sink
         .exactlyOnce(false)
         .build());
```

In the at-least-once mode or if no guarantee is enabled, the transaction
is committed after each batch of messages: transactions are used for
performance as this is JMS' way to send messages in batches. Batches are
created from readily available messages so they incur minimal extra
latency.

##### Note

The XA transactions are implemented incorrectly in some brokers.
Specifically a prepared transaction is sometimes rolled back when the
client disconnects. The issue is tricky because the integration will
work during normal operation and the problem will only manifest if the
job crashes in a specific moment. Jet will even not detect it, only some
messages will be missing from the sink. To test your broker we provide a
tool, please go to [XA
tests](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/xa-test)
to get more information. This only applies to JMS sink, the source
doesn't use XA transactions.

#### Connection Handling

The JMS source and sink open one connection to the JMS server for each
member and each vertex. Then each parallel worker of the source creates
a session and a message consumer/producer using that connection.

IO failures are generally handled by the JMS client and do not cause the
connector to fail. Most of the clients offer a configuration parameter
to enable auto-reconnection, refer to the specific client documentation
for details.

### Apache Pulsar

>This connector is currently under incubation. For more
>information and examples, please visit the [GitHub repository](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/pulsar).

## In-memory Data Structures

Jet comes out of the box with some [in-memory distributed data
structures](data-structures) which can be used as a data source or a
sink. These sources are useful for caching sources or results to be used
for further processing, or acting as a glue between different data
pipelines.

### IMap

[IMap](data-structures) is a distributed in-memory key-value data
structure with a rich set of features such as indexes, querying and
persistence. With Jet, it can be used as both a batch or streaming data
source.

As a batch data source, it's very easy to use without the need for any other
configuration:

```java
IMap<String, User> userCache = jet.getMap("usersCache")
Pipeline p = Pipeline.create();
p.readFrom(Sources.map(userCache));
 .writeTo(Sinks.logger()));
```

#### Event Journal

The map can also be used as a streaming data source by utilizing its so
called _event journal_. The journal for a map is by default not enabled,
but can be explicitly enabled with a configuration option in
`hazelcast.yaml`:

```yaml
hazelcast:
  map:
    name_of_map:
      event-journal:
        enabled: true
        capacity: 100000
        time-to-live-seconds: 10
```

We can then modify the previous pipeline to instead stream the changes:

```java
IMap<String, User> userCache = jet.getMap("usersCache")
Pipeline p = Pipeline.create();
p.readFrom(Sources.mapJournal(userCache, START_FROM_OLDEST))
 .withIngestionTimestamps()
 .writeTo(Sinks.logger()));
```

By default, the source will only emit `ADDED` or `UPDATED` events and
the emitted object will have the key and the new value. You can change
to listen for all events by adding additional parameters to the source.

The event journal is fault tolerant and supports exactly-once
processing.

The capacity of the event journal is also an important consideration, as
having too little capacity will cause events to be dropped. Consider
also the capacity is for all the partition and not shared per partition.
For example, if there's many updates to just one key, with the default
partition count of `271` and journal size of `100,000` the journal only
has space for `370` events per partitions.

For a full example, please see the [Stream Changes From IMap tutorial.](../how-tos/stream-imap)

#### Map Sink

By default, map sink expects items of type `Entry<Key, Value>` and will
simply replace the previous entries, if any. However there's variants of
this that allow you to do atomic updates to existing entries in the map
by making use `EntryProcessor` objects.

The updating sinks come in three variants:

1. `mapWithMerging`, where you provide a function that computes the map
   value from the stream item and a merging function that gets called
   only if a value already exists in the map. This is similar to the way
   standard `Map.merge` method behaves. Here’s an example that
   concatenates String values:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.<String, User>map("userCache"))
 .map(user -> entry(user.country(), user))
 .writeTo(Sinks.mapWithMerging("usersByCountry",
    e -> e.getKey(),
    e -> e.getValue().name(),
    (oldValue, newValue) -> oldValue + ", " + newValue)
  );
```

2. `mapWithUpdating`, where you provide a single updating function that
   always gets called. It will be called on the stream item and the
   existing value, if any. This can be used to add details to an
   existing object for example. This is similar to the way standard
   `Map.compute` method behaves. Here's an example that only updates a
   field:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.<String, User>map("userCacheDetails"))
 .writeTo(Sinks.mapWithUpdating("userCache",
    e -> e.getKey(),
    (oldValue, entry) -> (oldValue != null ? oldValue.setDetails(entry.getValue) : null)
  );
```

3. `mapWithEntryProcessor`, where you provide a function that returns a
   full-blown `EntryProcessor` instance that will be submitted to the
   map. This is the most general variant. This example takes the
   values of the map and submits an entry processor that increments the
   values by 5:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.<String, Integer>map("input"))
 .writeTo(Sinks.mapWithEntryProcessor("output",
    entry -> entry.getKey(),
    entry -> new IncrementEntryProcessor())
  );

static class IncrementEntryProcessor implements EntryProcessor<String, Integer, Integer> {
    @Override
    public Integer process(Entry<String, Integer> entry) {
        return entry.setValue(entry.getValue() + 5);
    }
}
```

#### Predicates and Projections

If your use case calls for some filtering and/or transformation of the
data you retrieve, you can optimize the pipeline by providing a
filtering predicate and an arbitrary transformation function to the
source connector itself and they’ll get applied before the data is
processed by Jet. This can be advantageous especially in the cases when
the data source is in another cluster. See the example below:

```java
IMap<String, Person> personCache = jet.getMap("personCache");
Pipeline p = Pipeline.create();
p.readFrom(Sources.map(personCache,
    Predicates.greaterEqual("age", 21),
    Projections.singleAttribute("name"))
);
```

### ICache

ICache is mostly equivalent to IMap, the main difference being that it's
compliant with the JCache standard API. As a sink, since `ICache`
doesn't support entry processors, only the default variant is available.

### IList

`IList` is a simple data structure which is ordered, and not
partitioned. All the contents of the `IList` will reside only on one
member.

The API for it is very limited, but is useful for simple prototyping:

```java
IList<Integer> inputList = jet.getList("inputList");
for (int i = 0; i < 10; i++) {
    inputList.add(i);
}

Pipeline p = Pipeline.create();
p.readFrom(Sources.list(inputList))
 .map(i -> "item-" + i)
 .writeTo(Sinks.list("resultList"));
```

List isn't suitable to use as a streaming sink because items are always
appended and eventually the member will run out of memory.

### Reliable Topic

Reliable Topic provides a simple pub/sub messaging API which can be
used as a data sink within Jet.

```java
jet.getReliableTopic("topic")
   .addMessageListener(message -> System.out.println(message));

Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(100))
  .withIngestionTimestamps()
  .writeTo(Sinks.reliableTopic("topic"));
```

A simple example is supplied above. For a more advanced version, also
see [Observables](#observable)

### Same vs. Different Cluster

It's possible to use the data structures that are part of the same Jet
cluster, and share the same memory and computation resources with
running jobs. For a more in-depth discussion on this topic, please see
the [In-memory Storage](../architecture/in-memory-storage) section.

Alternatively, Jet can also read from or write to data structures from
other Hazelcast or Jet clusters, using the _remote_ sinks and sources.
When reading or writing to remote sources, Jet internally creates a
client using the supplied configuration and will create connections to
the other cluster.

```java
ClientConfig cfg = new ClientConfig();
cfg.setClusterName("cluster-name");
cfg.getNetworkConfig().addAddress("node1.mydomain.com", "node2.mydomain.com");

Pipeline p = Pipeline.create();
p.readFrom(Sources.remoteMap("inputMap", cfg));
...
```

## Databases

Jet supports a wide variety of relational and NoSQL databases as a data
source or sink. While most traditional databases are batch oriented,
there's emerging techniques that allow to bridge the gap to streaming
which we will explore.

### JDBC

JDBC is a well-established database API supported by every major
relational (and many non-relational) database implementations including
Oracle, MySQL, PostgreSQL, Microsoft SQL Server. They provide libraries
called _JDBC drivers_ and every major database vendor will have this
driver available for either download or in a package repository such as
maven.

Jet is able to utilize these drivers both for sources and sinks and the
only step required is to add the driver to the `lib` folder of Jet or
submit the driver JAR along with the job.

In the simplest form, to read from a database you simply need to pass
a query:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.jdbc("jdbc:mysql://localhost:3306/mysql",
    "SELECT * FROM person",
    resultSet -> new Person(resultSet.getInt(1), resultSet.getString(2))
)).writeTo(Sinks.logger());
```

Jet is also able to distribute a query across multiple nodes by
customizing the filtering criteria for each node:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.jdbc(
    () -> DriverManager.getConnection("jdbc:mysql://localhost:3306/mysql"),
    (con, parallelism, index) -> {
        PreparedStatement stmt = con.prepareStatement(
              "SELECT * FROM person WHERE MOD(id, ?) = ?)");
        stmt.setInt(1, parallelism);
        stmt.setInt(2, index);
        return stmt.executeQuery();
    },
    resultSet -> new Person(resultSet.getInt(1), resultSet.getString(2))
)).writeTo(Sinks.logger());
```

The JDBC source only works in batching mode, meaning the query is only
executed once, for streaming changes from the database you can follow the
[Change Data Capture tutorial](../tutorials/cdc.md).

#### JDBC Data Sink

Jet is also able to output the results of a job to a database using the
JDBC driver by using an update query.

The supplied update query should be a parameterized query where the
parameters are set for each item:

```java
Pipeline p = Pipeline.create();
p.readFrom(KafkaSources.<Person>kafka(.., "people"))
 .writeTo(Sinks.jdbc(
         "REPLACE INTO PERSON (id, name) values(?, ?)",
         DB_CONNECTION_URL,
         (stmt, item) -> {
             stmt.setInt(1, item.id);
             stmt.setString(2, item.name);
         }
));
```

JDBC sink will automatically try to reconnect during database
connectivity issues and is suitable for use in streaming jobs. If you
want to avoid duplicate writes to the database, then a suitable
_insert-or-update_ statement should be used instead of `INSERT`, such as
`MERGE` or `REPLACE` or `INSERT .. ON CONFLICT ..`.

#### Fault tolerance

The JDBC sink supports the exactly-once guarantee. It uses two-phase XA
transactions, the DML statements are committed consistently with the
last state snapshot. This greatly increases the latency, it is
determined by the snapshot interval: messages are visible to consumers
only after the commit. In order to make it work, instead of the JDBC URL
you have to use the variant with `Supplier<CommonDataSource>` and it
must return an instance of `javax.sql.XADataSource`, otherwise the job
will not start.

Here is an example for PostgreSQL:

```java
stage.writeTo(Sinks.jdbc("INSERT INTO " + tableName + " VALUES(?, ?)",
         () -> {
                 BaseDataSource dataSource = new PGXADataSource();
                 dataSource.setUrl("localhost:5432");
                 dataSource.setUser("user");
                 dataSource.setPassword("pwd");
                 dataSource.setDatabaseName("database1");
                 return dataSource;
         },
         (stmt, item) -> {
             stmt.setInt(1, item.getKey());
             stmt.setString(2, item.getValue());
         }
 ));
```

##### Note

The XA transactions are implemented incorrectly in some databases.
Specifically a prepared transaction is sometimes rolled back when the
client disconnects. The issue is tricky because the integration will
work during normal operation and the problem will only manifest if the
Jet job crashes in a specific moment. Jet will even not detect it, only
some records will be missing from the target database. To test your
broker we provide a tool, please go to [XA
tests](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/xa-test)
to get more information. This only applies to the JDBC sink, the source
doesn't use XA transactions.

### Change Data Capture (CDC)

Change Data Capture (CDC) refers to the process of observing changes
made to a database and extracting them in a form usable by other
systems, for the purposes of replication, analysis and many more.

Change Data Capture is especially important to Jet, because it allows
for the _streaming of changes from databases_, which can be efficiently
processed by Jet.

Implementation of CDC in Jet is based on
[Debezium](https://debezium.io/). Jet offers a generic Debezium source
which can handle CDC events from [any database supported by
Debezium](https://debezium.io/documentation/reference/1.1/connectors/index.html),
but we're also striving to make CDC sources first class citizens in Jet.
The one for MySQL is already one (since Jet version 4.2).

Setting up a streaming source of CDC data is just the matter of pointing
it at the right database via configuration:

```java
Pipeline pipeline = Pipeline.create();
pipeline.readFrom(
    MySqlCdcSources.mysql("customers")
            .setDatabaseAddress("127.0.0.1")
            .setDatabasePort(3306)
            .setDatabaseUser("debezium")
            .setDatabasePassword("dbz")
            .setClusterName("dbserver1")
            .setDatabaseWhitelist("inventory")
            .setTableWhitelist("inventory.customers")
            .build())
    .withNativeTimestamps(0)
    .writeTo(Sinks.logger());
```

(For an example of how to actually make use of CDC data see [our
tutorial](../tutorials/cdc)).

In order to make it work though, the databases need to be properly
configured too, have features essential for CDC enabled. For details see
the [CDC Deployment Guide](../operations/cdc.md).

#### CDC Connectors

As of Jet version 4.2 we have following types of CDC sources:

* [DebeziumCdcSources](/javadoc/{jet-version}/com/hazelcast/jet/cdc/DebeziumCdcSources.html):
  generic source for all databases supported by Debezium
* [MySqlCdcSources](/javadoc/{jet-version}/com/hazelcast/jet/cdc/MySqlCdcSources.html):
  specific, first class Jet CDC source for MySQL databases (also based
  on Debezium, but benefiting the full range of convenience Jet can
  additionally provide)

#### CDC Fault Tolerance

CDC sources offer at least-once processing guaranties. The source
periodically saves the database write ahead log offset for which it had
dispatched events and in case of a failure/restart it will replay all
events since the last successfully saved offset.

Unfortunately however there are no guaranties that the last saved offset
is still in the database changelog. Such logs are always finite and
depending on the DB configuration can be relatively short, so if the
CDC source has to replay data for a long period of inactivity, then
there can be loss. With careful management though we can say that
at-least once guaranties can practially be provided.

#### CDC Sinks

Change data capture is a source-side functionality in Jet, but we also
offer some specialized sinks that simplify applying CDC events to an
IMap, which gives you the ability to reconstruct the contents of the
original database table. The sinks expect to receive `ChangeRecord`
objects and apply your custom functions to them that extract the key and
the value that will be applied to the target IMap.

For example, a sink mapping CDC data to a `Customer` class and
maintaining a map view of latest known email addresses per customer
(identified by ID) would look like this:

```java
Pipeline p = Pipeline.create();
p.readFrom(source)
 .withoutTimestamps()
 .writeTo(CdcSinks.map("customers",
    r -> r.key().toMap().get("id"),
    r -> r.value().toObject(Customer.class).email));
```

> NOTE: The key and value functions have certain limitations. They can
> be used to map only to objects which the IMDG backend can deserialize,
> which unfortunately doesn't include user code submitted as a part of
> the Jet job. So in the above example it's OK to have `String` email
> values, but we wouldn't be able to use `Customer` directly. Hopefully
> future Jet versions will address this problem.

### Elasticsearch

Elasticsearch is a popular fulltext search engine. Hazelcast Jet can
use it both as a source and a sink.

#### Dependency

To use the Elasticsearch connector, you need to copy the
`hazelcast-jet-elasticsearch-7` module from the `opt` folder to the
`lib` folder and add the following dependency to your application:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-elasticsearch-7:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.hazelcast.jet</groupId>
  <artifactId>hazelcast-jet-elasticsearch-7</artifactId>
  <version>{jet-version}</version>
</dependency>
```

> For Elasticsearch version 6 and 5 there are separate modules
> `hazelcast-jet-elasticsearch-6` and `hazelcast-jet-elasticsearch-5`.
> Each module includes Elasticsearch client compatible with given major
> version of Elasticsearch. The connector API is the same between
> different versions, apart from a few minor differences where we
> surface the API of Elasticsearch client. See the JavaDoc for any
> such differences.

#### Source

The Elasticsearch connector source provides a builder and several
convenience factory methods. Most commonly one needs to provide:

* A client supplier function, which returns a configured instance of
 RestClientBuilder (see [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-initialization.html#java-rest-low-usage-initialization)),
* A search request supplier specifying a query to Elasticsearch,
* A mapping function from `SearchHit` to a desired type.

Example using a factory method:

```java
BatchSource<String> elasticSource = ElasticSources.elasticsearch(
    () -> client("user", "password", "host", 9200),
    () -> new SearchRequest("my-index"),
    hit -> (String) hit.getSourceAsMap().get("name")
);
```

For all configuration options use the builder:

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
```

By default, the connector uses a single scroll to read data from
Elasticsearch - there is only a single reader on a single node in the
whole cluster.

Slicing can be used to parallelize reading from an index with more
shards. Number of slices equals to globalParallelism.

If Hazelcast Jet nodes and Elasticsearch nodes are located on the same
machines then the connector will use co-located reading, avoiding the
overhead of physical network.

#### Sink

The Elasticsearch connector sink provides a builder and several
convenience factory methods. Most commonly you need to provide:

* A client supplier, which returns a configured instance of
 RestHighLevelClient (see
 [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-initialization.html#java-rest-low-usage-initialization)),

* A mapping function to map items from the pipeline to an instance of
 one of `IndexRequest`, `UpdateRequest` or `DeleteRequest`.

* Suppose type of the items in the pipeline is `Map<String, Object>`, the
 sink can be created using

```java
Sink<Map<String, Object>> elasticSink = ElasticSinks.elasticsearch(
    () -> client("user", "password", "host", 9200),
    item -> new IndexRequest("my-index").source(item)
);
```

For all configuration options use the builder:

```java
Sink<Map<String, Object>> elasticSink = new ElasticSinkBuilder<Map<String, Object>>()
    .name("elastic-sink")
    .clientFn(() -> RestClient.builder(new HttpHost(
            "localhost", 9200
    )))
    .bulkRequestSupplier(BulkRequest::new)
    .mapToRequestFn((map) -> new IndexRequest("my-index").source(map))
    .optionsFn(request -> RequestOptions.DEFAULT)
    .build();
```

The Elasticsearch sink doesn't implement co-located writing. To achieve
maximum write throughput provide all nodes to the `RestClient`
and configure parallelism.

### MongoDB

>This connector is currently under incubation. For more
>information and examples, please visit the [GitHub repository](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/mongodb).

### InfluxDB

>This connector is currently under incubation. For more
>information and examples, please visit the [GitHub repository](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/influxdb).

### Redis

>This connector is currently under incubation. For more
>information and examples, please visit the [GitHub repository](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/redis).

## Miscellaneous

### Test Sources

Test sources make it convenient to get started with Jet without having
to use an actual data source. They can also be used for unit testing
different pipelines where you can expect a more deterministic import.

#### Batch

The `items` source offers a simple batch source where the supplied list
of items are output:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items(1, 2, 3, 4))
 .writeTo(Sinks.logger());
```

This pipeline will emit the following items, and then the job will terminate:

```text
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 1
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 2
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 3
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 4
```

#### Streaming

The test streaming source emits an infinite stream of `SimpleEvent`s at
the requested rate (in this case, 10 items per second):

```java
p.readFrom(TestSources.itemStream(10))
 .withNativeTimestamp(0)
 .writeTo();
```

After submitting this job, you can expect infinite output like:

```text
12:33:36.774 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:36.700, sequence=0)
12:33:36.877 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:36.800, sequence=1)
12:33:36.976 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:36.900, sequence=2)
12:33:37.074 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:37.000, sequence=3)
12:33:37.175 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:37.100, sequence=4)
12:33:37.274 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:37.200, sequence=5)
```

Each `SimpleEvent` has a sequence which is monotonically increased and
also a timestamp which is derived from `System.currentTimeMillis()`.
For more information using these sources in a testing environment, refer
to the [Testing](testing) section.

### Observables

A Jet pipeline always expects to write the results somewhere. Sometimes
the job submitter is different than the one reading or processing the
results of a pipeline, but sometimes it can be the same, for example if the
job is a simple ad-hoc query. In this case Jet offers a special type of
construct called an `Observable`, which can be used as a sink.

For example, imagine the following pipeline:

```java
JetInstance jet = Jet.bootstrappedInstance();
Observable<SimpleEvent> observable = jet.newObservable();
observable.addObserver(e -> System.out.println("Printed from client: " + e));

Pipeline pipeline = p.create();
p.readFrom(TestSources.itemStream(5))
 .withIngestionTimestamps()
 .writeTo(Sinks.observable(observable));
try {
  jet.newJob(pipeline).join();
} finally {
  observable.destroy();
}
```

When you run this pipeline, you'll see the following output:

```text
Printed from client: SimpleEvent(timestamp=12:36:53.400, sequence=28)
Printed from client: SimpleEvent(timestamp=12:36:53.600, sequence=29)
Printed from client: SimpleEvent(timestamp=12:36:53.800, sequence=30)
Printed from client: SimpleEvent(timestamp=12:36:54.000, sequence=31)
Printed from client: SimpleEvent(timestamp=12:36:54.200, sequence=32)
Printed from client: SimpleEvent(timestamp=12:36:54.400, sequence=33)
Printed from client: SimpleEvent(timestamp=12:36:54.600, sequence=34)
Printed from client: SimpleEvent(timestamp=12:36:54.800, sequence=35)
Printed from client: SimpleEvent(timestamp=12:36:55.000, sequence=36)
```

You can see that the printed output is actually on the client, and not
on the server. Jet internally uses Hazelcast's `Ringbuffer` to create a
temporary buffer to write the results into and these are then fetched by
the client:

>It's worth noting that `Ringbuffer` may lose events, if they
>are being produced at a higher-rate than the clients can consume it. There
>will be a warning logged in such cases. You can also configure the
>capacity using the `setCapacity()` method on the `Observable`.

`Observable` can also implement `onError` and `onComplete` methods to
get notified of job completion and errors.

#### Futures

`Observable` also support a conversion to a future to collect the
results.

For example, to collect the job results into a list, you can use the
following pattern:

```java
JetInstance jet = Jet.bootstrappedInstance();
Observable<String> observable = jet.newObservable();

Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("a", "b", "c", "d"))
 .writeTo(Sinks.observable(observable));

Future<List<String>> future = observable.toFuture(
    s -> s.collect(Collectors.toList())
);
jet.newJob(p);

try {
  List<String> results = future.get();
  for (String result : results) {
    System.out.println(result);
  }
} finally {
  observable.destroy();
}
```

#### Cleanup

As `Observable`s are backed by `Ringbuffer`s stored in the cluster which
should be cleaned up by the client, once they are no longer necessary
using the `destroy()` method. If the Observable isn’t destroyed, the
memory used by it will be not be recovered by the cluster. It's possible
to get a list of all observables using the
`JetInstance.getObservables()` method.

### Socket

The socket sources and sinks opens a TCP socket to the supplied address
and either read from or write to the socket. The sockets are text-based
and may only read or write text data.

A simple example of the source is below:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.socket("localhost", 8080, StandardCharsets.UTF_8))
 .withoutTimestamps()
 .map(line -> /* parse line */)
 .writeTo(Sinks.logger());
```

This will connect to a socket on port 8080 and wait to receive some
lines of text, which will be sent as an item for the next step in the
pipeline to process.

Please note that Jet itself will not create any server sockets, this
should be handled outside of the Jet process itself.

When used as a sink, it will send a line of text for each input item,
similar to how the source works:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.itemStream(10))
 .withoutTimestamps()
 .map(e -> e.toString())
 .writeTo(Sinks.socket("localhost", 8080));
```

Any disconnections for both source and sink will cause the job to fail,
so this source is mostly aimed for simple IPC or testing.

### Twitter

>This connector is currently under incubation. For more
>information and examples, please visit the [GitHub repository](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/twitter).

## Summary

### Sources

Below is a summary of various sources and where to find them. Some
sources are batch and some are stream oriented. The processing guarantee
is only relevant for streaming sources, as batch jobs should just be
restarted in face of an intermittent failure.

|source|artifactId (module)|batch/stream|guarantee|
|:-----|:------------------|:-----------|:--------|
|`AvroSources.files`|`hazelcast-jet-avro (avro)`|batch|N/A|
|`DebeziumCdcSources.debezium`|`hazelcast-jet-cdc-debezium (cdc-debezium)`|stream|at-least-once|
|`ElasticSources.elastic`|`hazelcast-jet-elasticsearch-5 (elasticsearch-5)`|batch|N/A|
|`ElasticSources.elastic`|`hazelcast-jet-elasticsearch-6 (elasticsearch-6)`|batch|N/A|
|`ElasticSources.elastic`|`hazelcast-jet-elasticsearch-7 (elasticsearch-7)`|batch|N/A|
|`HadoopSources.inputFormat`|`hazelcast-jet-hadoop (hadoop)`|batch|N/A|
|`KafkaSources.kafka`|`hazelcast-jet-kafka (kafka)`|stream|exactly-once|
|`MySqlCdcSources.mysql`|`hazelcast-jet-cdc-mysql (cdc-mysql)`|stream|at-least-once|
|`PulsarSources.pulsarConsumer`|`hazelcast-jet-contrib-pulsar`|stream|N/A|
|`PulsarSources.pulsarReader`|`hazelcast-jet-contrib-pulsar`|stream|exactly-once|
|`S3Sources.s3`|`hazelcast-jet-s3 (s3)`|batch|N/A|
|`Sources.cache`|`hazelcast-jet`|batch|N/A|
|`Sources.cacheJournal`|`hazelcast-jet`|stream|exactly-once|
|`Sources.files`|`hazelcast-jet`|batch|N/A|
|`Sources.fileWatcher`|`hazelcast-jet`|stream|none|
|`Sources.json`|`hazelcast-jet`|batch|N/A|
|`Sources.jsonWatcher`|`hazelcast-jet`|stream|none|
|`Sources.jdbc`|`hazelcast-jet`|batch|N/A|
|`Sources.jmsQueue`|`hazelcast-jet`|stream|exactly-once|
|`Sources.list`|`hazelcast-jet`|batch|N/A|
|`Sources.map`|`hazelcast-jet`|batch|N/A|
|`Sources.mapJournal`|`hazelcast-jet`|stream|exactly-once|
|`Sources.socket`|`hazelcast-jet`|stream|none|
|`TestSources.items`|`hazelcast-jet`|batch|N/A|
|`TestSources.itemStream`|`hazelcast-jet`|stream|none|

### Sinks

Below is a summary of various sinks and where to find them. All sources
may operate in batch mode, but only some of them are suitable for
streaming jobs, this is indicated below. As with sources, the processing
guarantee is only relevant for streaming jobs. All streaming sinks by
default support at-least-once guarantee, but only some of them support
exactly-once. If using idempotent updates, you can ensure exactly-once
processing even with at-least-once sinks.

|sink|artifactId (module)|streaming support|guarantee|
|:---|:------------------|:--------------|:-------------------|
|`AvroSinks.files`|`hazelcast-jet-avro (avro)`|no|N/A|
|`CdcSinks.map`|`hazelcast-jet-cdc-debezium (cdc-debezium)`|yes|at-least-once|
|`ElasticSinks.elastic`|`hazelcast-jet-elasticsearch-5 (elasticsearch-5)`|yes|at-least-once|
|`ElasticSinks.elastic`|`hazelcast-jet-elasticsearch-6 (elasticsearch-6)`|yes|at-least-once|
|`ElasticSinks.elastic`|`hazelcast-jet-elasticsearch-7 (elasticsearch-7)`|yes|at-least-once|
|`HadoopSinks.outputFormat`|`hazelcast-jet-hadoop (hadoop)`|no|N/A|
|`KafkaSinks.kafka`|`hazelcast-jet-kafka (kafka)`|yes|exactly-once|
|`PulsarSources.pulsarSink`|`hazelcast-jet-contrib-pulsar`|yes|at-least-once|
|`S3Sinks.s3`|`hazelcast-jet-s3 (s3)`|no|N/A|
|`Sinks.cache`|`hazelcast-jet`|yes|at-least-once|
|`Sinks.files`|`hazelcast-jet`|yes|exactly-once|
|`Sinks.json`|`hazelcast-jet`|yes|exactly-once|
|`Sinks.jdbc`|`hazelcast-jet`|yes|exactly-once|
|`Sinks.jmsQueue`|`hazelcast-jet`|yes|exactly-once|
|`Sinks.list`|`hazelcast-jet`|no|N/A|
|`Sinks.map`|`hazelcast-jet`|yes|at-least-once|
|`Sinks.observable`|`hazelcast-jet`|yes|at-least-once|
|`Sinks.reliableTopic`|`hazelcast-jet`|yes|at-least-once|
|`Sinks.socket`|`hazelcast-jet`|yes|at-least-once|

## Custom Sources and Sinks

If Jet doesn’t natively support the data source/sink you need, you can
build a connector for it yourself by using the
[SourceBuilder](/javadoc/{jet-version}/com/hazelcast/jet/pipeline/SourceBuilder.html)
and
[SinkBuilder](/javadoc/{jet-version}/com/hazelcast/jet/pipeline/SinkBuilder.html).

### SourceBuilder

To make a custom source connector you need two basic ingredients:

* a _context_ object that holds all the resources and state you need
  to keep track of
* a stateless function, _`fillBufferFn`_, taking two parameters: the
  state object and a buffer object provided by Jet

Jet repeatedly calls `fillBufferFn` whenever it needs more data items.
Optimally, the function will fill the buffer with the items it can
acquire without blocking. A hundred items at a time is enough to
eliminate any per-call overheads within Jet. The function is allowed to
block as well, but taking longer than a second to complete can have
negative effects on the overall performance of the processing pipeline.

In the following examples we build a simple batch source that emits
the lines of a file:

```java
BatchSource<String> fileSource = SourceBuilder
    .batch("file-source", x -> new BufferedReader(new FileReader("input.txt")))
    .<String>fillBufferFn((in, buf) -> {
        String line = in.readLine();
        if (line != null) {
            buf.add(line);
        } else {
            buf.close();
        }
    })
    .destroyFn(BufferedReader::close)
    .build();
```

For a more involved example (which reads data in _batches_ for
efficiency, deals with _unbounded_ data, emits _timestamps_, is
_distributed_ and _fault tolerant_ see the
[Custom Batch Sources](../how-tos/custom-batch-source.md
) and
[Custom Stream Sources](../how-tos/custom-stream-source.md)
tutorials).

### SinkBuilder

To make your custom sink connector you need two basic ingredients:

* a _context_ object that holds all the resources and state you need
  to keep track of
* a stateless function, _`receiveFn`_, taking two parameters: the state
  object and a data item sent to the sink

In the following example we build a simple sink which writes the
`toString()` form of `Object`s to a file:

```java
Sink<Object> sink = sinkBuilder(
    "file-sink", x -> new PrintWriter(new FileWriter("output.txt")))
    .receiveFn((out, item) -> out.println(item.toString()))
    .destroyFn(PrintWriter::close)
    .build();
```

For a more involved example, covering issues like _batching_,
_distributiveness_ and _fault tolerance_, see the
[Custom Sinks](../how-tos/custom-sink.md) tutorial).
