# Hazelcast Jet Code Samples

A repository of code samples for Hazelcast Jet. The samples show you how
to use the Pipeline API to solve a range of use cases, how to integrate
Jet with other systems and how to connect to various data sources (both
from a Hazelcast IMDG and 3rd-party systems). There is also a folder with
samples using the Core API.

## Stream Aggregation

### [Sliding Window](sliding-windows/src/main/java/com/hazelcast/jet/examples/slidingwindow/StockExchange.java)
  - apply a sliding window
  - perform basic aggregation (counting)
  - print the results on the console

### [Sliding Window with Nested Aggregation](sliding-windows/src/main/java/com/hazelcast/jet/examples/slidingwindow/TopNStocks.java)
  - like the above, plus:
  - add a second-level aggregation stage to find top/bottom N results

### [Session Window](session-windows/src/main/java/com/hazelcast/jet/examples/sessionwindow/SessionWindow.java)
  - apply a session window
  - use a custom Core API processor as the event source
  - perform a composite aggregate operation (apply two aggregate functions
    in parallel).
  - print the results on the console

### [Early Window Results](early-window-results/src/main/java/com/hazelcast/jet/examples/earlyresults/TradingVolumeOverTime.java)
  - use the `SourceBuilder` to create a mock source of trade events from a
    stock market
  - apply a tumbling window, configure to emit early results
  - aggregate by summing a derived value
  - present the results in a live GUI chart

### [Pattern Matching](pattern-matching/src/main/java/com/hazelcast/jet/examples/patternmatching/TransactionTracking.java)
  - use _stateful mapping_ on an event stream to track the state of many
    concurrent transactions, detect when a transaction is done, and compute
    its duration
  - open a GUI window that shows the transaction status

### [Rolling Aggregation](rolling-aggregation/src/main/java/com/hazelcast/jet/examples/rollingaggregation/TradingVolume.java)
  - use `SourceBuilder` to create a mock source of trade events from a stock
    market
  - simple rolling aggregation (summing the price)
  - keep updating the target map with the current values of aggregation
  - present the results in a live GUI chart

## Batch Aggregation

### [Word Count](wordcount/src/main/java/com/hazelcast/jet/examples/wordcount/WordCount.java)
  - use an `IMap` as the data source
  - stateless transforms to clean up the input (flatMap + filter)
  - perform basic aggregation (counting)
  - print a table of the most frequent words on the console using an `Observable`

### [Inverted Index with TF-IDF Scoring](tf-idf/src/main/java/com/hazelcast/jet/examples/tfidf/TfIdf.java)
  - serialize a small dataset to use as side input
  - fork a pipeline stage into two downstream stages
  - stateless transformations to clean up input
  - count distinct items
  - group by key, then group by secondary key
  - aggregate to a map of (secondary key -> result)
  - hash-join the forked stages
  - open an interactive GUI to try out the results

## Joins
### [Co-Group and Aggregate](co-group/src/main/java/com/hazelcast/jet/examples/cogroup/BatchCoGroup.java)
  - co-group three bounded data streams on a common key
  - for each distinct key, emit the co-grouped items in a 3-tuple of lists
  - store the results in an `IMap` and check they are as expected
### [Windowed Co-Group and Aggregate](co-group/src/main/java/com/hazelcast/jet/examples/cogroup/WindowedCoGroup.java)
  - use the Event Journal of an `IMap` as a streaming source
  - apply a sliding window
  - co-group three unbounded data streams on a common key
  - print the results on the console
### Hash Join
  - see [below](#enrich-using-hash-join)

## Data Enrichment
### [Enrich Using IMap](enrichment/src/main/java/com/hazelcast/jet/examples/enrichment/Enrichment.java)
  - the sample is in the `enrichUsingIMap()` method
  - use the Event Journal of an `IMap` as a streaming data source
  - apply the `mapUsingIMap` transform to fetch the enriching data from
    another `IMap`
  - enrich from two `IMap`s in two `mapUsingIMap` steps
  - print the results on the console
### [Enrich Using ReplicatedMap](enrichment/src/main/java/com/hazelcast/jet/examples/enrichment/Enrichment.java)
  - the sample is in the `enrichUsingReplicatedMap()` method
  - use the Event Journal of an `IMap` as a streaming data source
  - apply the `mapUsingReplicatedMap` transform to fetch the enriching data
    from another `IMap`
  - enrich from two `ReplicatedMap`s in two `mapUsingReplicatedMap` steps
  - print the results on the console
### [Enrich using gRPC](grpc/src/main/java/com/hazelcast/jet/examples/grpc/GRPCEnrichment.java)
  - prepare a data service: a gRPC-based network service
  - use the Event Journal of an `IMap` as a streaming data source
  - enrich the unbounded data stream by making async gRPC calls to the service
  - print the results on the console
### [Enrich Using Hash Join](enrichment/src/main/java/com/hazelcast/jet/examples/enrichment/Enrichment.java)
  - the sample is in the `enrichUsingHashJoin()` method
  - use the Event Journal of an `IMap` as a streaming data source
  - use a directory of files as a batch data source
  - hash-join an unbounded stream with two batch streams in one step
  - print the results on the console

## Return Results to the Caller
### [Basic Observables](return-results/src/main/java/com/hazelcast/jet/examples/returnresults/BasicObservable.java)
  - obtain an `Observable`
  - incorporate it in a streaming pipeline by wrapping it in a `Sink` 
  - register an `Observer` on it
  - execute the pipeline (streaming job)
  - observe the results as they show up in the `Observer`
  
### [Iterable results](return-results/src/main/java/com/hazelcast/jet/examples/returnresults/IterableResults.java)
  - obtain an `Observable`
  - use it as `Sink` in a batch job
  - get a result `Iterator` form of the `Observable`
  - execute the batch job
  - observe the results by iterating once execution has finished

### [Results as a Future](return-results/src/main/java/com/hazelcast/jet/examples/returnresults/FutureResults.java)
  - obtain an `Observable`
  - use it as `Sink` in a batch job
  - get the `CompletableFuture` form of the `Observable`
  - specify actions to be executed once the results are complete
  - execute the batch job
  - observe the results when they become available
  

## Job Management

- [Suspend/Resume a Job](job-management/src/main/java/com/hazelcast/jet/examples/jobmanagement/JobSuspendResume.java)
- [Restart/Rescale a Job](job-management/src/main/java/com/hazelcast/jet/examples/jobmanagement/JobManualRestart.java)
- [Inspect and Manage Existing Jobs](job-management/src/main/java/com/hazelcast/jet/examples/jobmanagement/JobTracking.java)
- [Idempotently Submit a Job](job-management/src/main/java/com/hazelcast/jet/examples/jobmanagement/ExclusiveJobExecution.java)
  - submit a job with the same name to two Jet members
  - result: only one job running, both clients get a reference to it

## Integration with Hazelcast IMDG
- [IMap as Source and Sink](imdg-connectors/src/main/java/com/hazelcast/jet/examples/imdg/MapSourceAndSinks.java)
- [IMap in a Remote IMDG as Source and Sink](imdg-connectors/src/main/java/com/hazelcast/jet/examples/imdg/RemoteMapSourceAndSink.java)
- [Projection and Filtering Pushed into the IMap Source](imdg-connectors/src/main/java/com/hazelcast/jet/examples/imdg/MapPredicateAndProjection.java)
- [ICache as Source and Sink](imdg-connectors/src/main/java/com/hazelcast/jet/examples/imdg/CacheSourceAndSink.java)
- [IList as Source and Sink](imdg-connectors/src/main/java/com/hazelcast/jet/examples/imdg/ListSourceAndSink.java)
- [Event Journal of IMap as a Stream Source](event-journal/src/main/java/com/hazelcast/jet/examples/eventjournal/MapJournalSource.java)
  - variant with [IMap in a remote cluster](event-journal/src/main/java/com/hazelcast/jet/examples/eventjournal/MapJournalSource.java)
- [Event Journal of ICache as a Stream Source](event-journal/src/main/java/com/hazelcast/jet/examples/eventjournal/CacheJournalSource.java)
  - variant with [ICache in a remote cluster](event-journal/src/main/java/com/hazelcast/jet/examples/eventjournal/CacheJournalSource.java)

## Integration with Other Systems

- [Kafka Source](kafka/src/main/java/com/hazelcast/jet/examples/kafka/KafkaSource.java)
  - variant with [Avro Serialization](kafka/src/main/java/com/hazelcast/jet/examples/kafka/avro/KafkaAvroSource.java)
  - variant with [JSON Serialization](kafka/src/main/java/com/hazelcast/jet/examples/kafka/json/KafkaJsonSource.java)
- [Kafka Sink](kafka/src/main/java/com/hazelcast/jet/examples/kafka/KafkaSink.java)
- [Hadoop Distributed File System (HDFS) Source and Sink](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/HadoopWordCount.java)
  - variant with [Avro Serialization](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/avro/HadoopAvro.java)
- [JDBC Source](jdbc/src/main/java/com/hazelcast/jet/examples/jdbc/JdbcSource.java)
- [JDBC Sink](jdbc/src/main/java/com/hazelcast/jet/examples/jdbc/JdbcSink.java)
- [Change Data Capture](cdc/src/main/java/com/hazelcast/jet/examples/cdc/Cache.java)
- [JMS Queue Source and Sink](jms/src/main/java/com/hazelcast/jet/examples/jms/JmsQueueSample.java)
- [JMS Topic Source and Sink](jms/src/main/java/com/hazelcast/jet/examples/jms/JmsTopicSample.java)
- [Python Mapping Function](python/src/main/java/com/hazelcast/jet/examples/python/Python.java)
- [TCP/IP Socket Source](sockets/src/main/java/com/hazelcast/jet/examples/sockets/StreamTextSocket.java)
- [TCP/IP Socket Sink](sockets/src/main/java/com/hazelcast/jet/examples/sockets/WriteTextSocket.java)
- [CSV Batch Source](files/src/main/java/com/hazelcast/jet/examples/files/SalesCsvAnalyzer.java)
  - use Jet to analyze sales transactions from CSV file 
- [JSON Batch Source](files/src/main/java/com/hazelcast/jet/examples/files/SalesJsonAnalyzer.java)
  - use Jet to analyze sales transactions from JSON file 
- [File Batch Source](files/src/main/java/com/hazelcast/jet/examples/files/AccessLogAnalyzer.java)
  - use Jet to analyze an HTTP access log file
  - variant with [Avro serialization](files/src/main/java/com/hazelcast/jet/examples/files/avro/AvroSource.java)
- [File Streaming Source](files/src/main/java/com/hazelcast/jet/examples/files/AccessLogStreamAnalyzer.java)
  - analyze the data being appended to log files while the Jet job is
    running
- [File Sink](files/src/main/java/com/hazelcast/jet/examples/files/AccessLogAnalyzer.java)
  - variant with [Avro serialization](files/src/main/java/com/hazelcast/jet/examples/files/avro/AvroSink.java)
- [Amazon AWS S3 Source and Sink](files/src/main/java/com/hazelcast/jet/examples/files/s3/S3WordCount.java)
- [Hadoop Source and Sink](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/HadoopWordCount.java)
    - variant with [Avro serialization](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/avro/HadoopAvro.java)
    - variant with [Parquet format](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/parquet/HadoopParquet.java)
    - variant with [Amazon S3](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/cloud/AmazonS3.java)
    - variant with [Azure Cloud Storage](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/cloud/AzureCloudStorage.java)
    - variant with [Azure Data Lake](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/cloud/AzureDataLake.java)
    - variant with [Google Cloud Storage](hadoop/src/main/java/com/hazelcast/jet/examples/hadoop/cloud/GoogleCloudStorage.java)

## Custom Sources and Sinks
- [Custom Source](source-sink-builder/src/main/java/com/hazelcast/jet/examples/sourcebuilder/HttpSource.java):
  - start an Undertow HTTP server that collects basic JVM stats
  - construct a custom Jet source based on Java 11 HTTP client
  - apply a sliding window
  - compute linear trend of the JVM metric provided by the HTTP server
  - present the results in a live GUI chart
- [Custom Sink](source-sink-builder/src/main/java/com/hazelcast/jet/examples/sinkbuilder/TopicSink.java)
  - construct a custom Hazelcast `ITopic` sink

## Integration with Frameworks and Libraries

### Spring Framework
- [Annotation-Based Spring Context](spring/src/main/java/com/hazelcast/jet/examples/spring/AnnotationBasedConfigurationSample.java)
  - use programmatic Jet configuration in a Spring Application Context
    class
  - annotation-based dependency injection into a Jet Processor
- [XML-Based Spring Context](spring/src/main/java/com/hazelcast/jet/examples/spring/XmlConfigurationSample.java)
  - configure Jet as a Spring bean in
    [application-context.xml](spring/src/main/resources/application-context.xml)
  - XML-based dependency injection into a Jet Processor
- [XML-Based Dependency Injection into a Jet
  Processor](spring/src/main/java/com/hazelcast/jet/examples/spring/XmlConfigurationSample.java)
  - [configure Jet](spring/src/main/resources/application-context-with-schema.xml)
  as a Spring bean using Jet's XML Schema for Spring Configuration
  - XML-based dependency injection into a Jet Processor
- [Spring Boot App that Runs a Jet
  Job](spring-boot/src/main/java/com/hazelcast/jet/examples/spring/SpringBootSample.java)

### Protocol Buffers
- [Protobuf Serializer Adapter](protobuf/src/main/java/com/hazelcast/jet/examples/protobuf/ProtobufSerializerAdapter.java)
- [Protobuf Serializer Hook Adapter](protobuf/src/main/java/com/hazelcast/jet/examples/protobuf/ProtobufSerializerHookAdapter.java)
