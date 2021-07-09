---
title: 009 - Apache Pulsar Connector
description: Jet source and sink for Apache Pulsar.
---

*Since*: Contrib Module

## Summary

Apache Pulsar connector enables Jet to ingest data from Pulsar topics
into Jet pipelines and publish messages to Pulsar topics.

It uses the Pulsar client library to read and publish messages from/to a
Pulsar topic. Pulsar client library provides two different ways of
reading messages from a topic. They are namely Consumer API and Reader
API that have major differences. Pulsar connectors have benefits of both
Consumer API and Reader API.

Besides, this Pulsar client library has an API called the Producer API.
The Pulsar connector enables users to use the Pulsar topic as a sink in
Jet pipelines.

## Implementation Details

In the implementation of the Apache Pulsar sources, both Consumer API
and Reader API are used. Each API is advantageous in different aspects
compared to one another.  

By using the Consumer API, the distributed source is created without
dealing with the partition mapping - it is handled below this
abstraction level. But, since this API does not return the cursor for
the last consumed message, it is incapable of assuring fault-tolerance.
This issue will be discussed in the upcoming section.

As opposed to Consumer API, the Reader API helps us to build a source
that consumes through a Pulsar topic with an **exactly-once guarantee**
if the remaining sections of the pipeline also assure this.

### The Source using Consumer API

Pulsar provides an abstraction called Consumer API that is based on a
topic subscription. An application that subscribes to the topic can
consume messages from the first unacknowledged message for this
subscription.

The Consumer API has the 4 main operations listed below:

1. Subscribe to a topic.
2. Start to consume from the first unacknowledged message.
3. Consume messages
4. Send an acknowledgment to the broker.

#### Subscription Mode

A subscription determines how messages are delivered to consumers, and
it is identified by its subscription-name. There are three available
subscription modes in Pulsar: exclusive, shared, and failover. In the
Pulsar source connector using Consumer API abstraction, the shared or
round-robin mode is used. The shared subscription allows multiple
consumers to attach to the same subscription. The messages belong to the
subscribed topic are delivered in a round-robin distribution across
consumers, and any given message is delivered to only one consumer. When
a consumer disconnects, all the messages that were sent to it and not
acknowledged will be rescheduled for sending to the remaining consumers.
This subscription mode allows consuming messages in a distributed
manner. However, this distributed manner prevents the order of messages
from being preserved. Please see Pulsar’s documentation for information
on other subscription options.

#### Message Receive Policy

Consumer API provides 4 different policies (2 for single or batch x 2
for sync or async) for receiving messages from the broker. These are

1. Blocking Single Receive
2. Async Single Receive
3. Blocking Batch Receive (Our choice)
4. Async Batch Receive

Since `SourceBuilder.SourceBuffer` and
`SourceBuilder.TimestampedSourceBuffer` are not thread-safe, it would
require extra care when async policies were chosen. Therefore, handling
the result of the async calls should not be performed by creating new
threads. This requirement conflicts with the usual practice of async
processing. That is why I did not prefer async policies.

We prefer to use batch receive assuming that it is better in
performance. The batch receiving of Pulsar consumer assures that “the
batch receive will be completed as long as anyone of the conditions(has
enough number of messages, has enough of the size of messages in terms
of bytes and number, wait for a timeout) is met”. As the default batch
receive policy, we set timeout to 1000 ms and max number of messages to
512 messages. Since the batch receive policy of the consumer is more
likely to change, a separate builder setter is created for it.

#### Consumer API’s acknowledgment mechanism

As a default, Pulsar Broker deletes a message from the topic provided
that all subscriptions for this topic acknowledge this message. By
changing the broker configurations, these fully-acknowledged messages
can continue to persist.

With a consumer, the application resume consuming from the first
unacknowledged message. It resembles the cursor mechanism but it hinders
some tricky conditions that can result in message loss. In order to
support fault-tolerance, the rollback mechanism should be able to
perform properly in case of failure. However, the acknowledgment
mechanism of Consumer API has no rollback mechanism.

#### Consumer API cannot support Fault-tolerance

At the first trial of mine, between two snapshotting times, I stored the
consumed messages ids in a list. Then, I tried to acknowledge the list
of messages at the snapshot time (by assuming the job does not fail at
snapshot time). But if the job fails during the snapshot, there is no
way to rollback these acknowledgments.  As a result, after the restart
of the job, the job loses these acknowledged messages. If the Consumer
API had a commit mechanism, it could guarantee exactly-once processing.

To avoid these complications, you could remove acknowledgment logic
entirely. But, this would introduce another short come: all of the
messages would be stored permanently unless any eviction mechanism
exists. For the continuous processes, this may cause excessive storage
usage.

Note that: The Pulsar consumer source does not fail fast in the scenario
that when the job reading Pulsar topic using the Pulsar consumer source
is running and Pulsar broker is closed, the job doesn't fail, it just
prints log at warn level. The reason for this is that the Pulsar Client
tries to reconnect to the broker with exponential backoff in this case,
and while doing this, we cannot notice the connection issue because it
only prints logs without throwing an exception.

#### The decision on Acknowledgement Design on Jet Connector

It should be determined when the messages are acknowledged or whether
should be acknowledged at all. The current implementation of the Pulsar
Consumer source acknowledges messages whenever it consumes them. It
should be noted that this implementation prevents at-least-once logic
let alone exactly-once.

This issue with existence and timing of acknowledgment should be
discussed further.

#### Summary of the Design Choices

The design choices regarding the usage of Consumer API are listed below:

1. Shared subscription mode is preferred.
2. Receive messages in a synchronous batch manner.
3. Immediately send acknowledgments to a Pulsar broker after consuming
   a batch of messages.

### The Source using Reader API

The Reader API is a lower-level API of the Pulsar. It does not have any
subscription mode or so. A user can simply read from a topic by telling
the Reader API from which message it should read onwards. The Reader API
provides us the `MessageId` of the earliest or latest message. You can use
one of these as the starting point for message reading. Or, the user can
declare the exact starting position by giving `MessageId` between these two
ends.  That being said, the current implementation of the source using
Reader API overwrites any user-specified preference on the starting
point and just reads from the earliest message onwards.

The implementation might require enhancements in order not to ignore the
user’s preference on the starting point of reading messages.

#### Fault-tolerance support

Since Reader API enables us to start reading from a specified message,
the source using Reader API can provide exactly-once processing. In
Pulsar source using Reader API, the `MessageId` of the latest read message
is stored in the snapshot. In case of failure, the job restarts from the
latest snapshot and reads from the stored `MessageId`.

For further information about Reader API visit its
[documentation](https://pulsar.apache.org/docs/en/concepts-clients/#reader-interface).

Note that: The Pulsar reader source does not fail fast in the scenario
that when the job reading Pulsar topic using the Pulsar reader source is
running and Pulsar broker is closed, the job doesn't fail, it just
prints logs at warn level. The reason for this is that the Pulsar Client
tries to reconnect to the broker with exponential backoff in this case,
and while doing this, we cannot notice the connection issue because it
only prints logs without throwing an exception.

## The Sink

Pulsar has Producer API to publish messages to the topics.

Producer API provides 4 different options for sending messages to
brokers:  

1. Sync Single Publish
2. Async Single Publish (Our choice)
3. Sync Batch Publish
4. Async Batch Publish

The builder pattern is used when constructing a Pulsar Sink. The
constructor of the builder, `PulsarSinkBuilder`, consists of the required
fields for Sink. The other optional fields can be added with setter
functions as well. At the sink, the messages are sent in an asynchronous
manner. This async sending has a retry mechanism, the max retry count is
set to 10.

### Fault-tolerance support on Sink

Pulsar does not provide a commit mechanism as a messaging system (they
want very low-latency). It has a deduplication mechanism to provide
exactly-once processing. After enabling the deduplication, the Pulsar
brokers remove the duplicate messages. It detects duplicates by looking
into the `SequenceId` field of messages which is added at publish-time.
Since the order of processing items cannot be guaranteed to be preserved
in Jet pipelines,  we cannot assign the same `SequenceId` to the same
message after a job restart.  The wrong assignments of `SequenceId` lead
to loss of messages, we cannot benefit from this deduplication
mechanism.

For more information about Pulsar producers
[look](https://pulsar.apache.org/docs/en/concepts-messaging/#producers).

## Common Properties of Sources and Sink

Both the source and sink creation APIs of the Pulsar connector implement
the builder pattern. We get the required parameters in the constructor
and other parameters in the setters. Configuration values are set to
default values and these default values can be changed using setters. We
have shown basic usage in all of the usage examples we provided, if you
want to change the values of optional parameters, you can use the setter
methods of the builder.
Both creating sources or sink of the Pulsar requires some kind of
projection functions from the user as a required parameter. These
functions are used to transform the data format with respect to the data
transfer direction. In the case of the source, we need to convert the
received Pulsar message to a Jet-compatible serializable data, that is,
emitting items. The Jet sources use `Event Time` of the Pulsar message
object as a timestamp, if it exists. Otherwise, its `Publish Time` is
used as a timestamp which always presents.

In turn, converting the processed items to the Pulsar message form is
required at the sink.

Additionally, Pulsar has a mechanism for type-safety called Schema.
Normally Pulsar allows messaging without schema, but we preferred to
impose schema usage while creating the sources and sink. It should be
noted that not using any schema is almost equivalent to using
Schema.BYTE in practice.

## Testing Details

The `PulsarTestSupport extends JetTestSupport` is formed to provide a
Pulsar container, Pulsar clients, ‘source and sink setup functions’ for
tests.

Various integration tests were to test the Pulsar connector. To test the
sources, firstly, a Pulsar container is initiated. Then, through some
client that is created in `PulsarTestSupport` for the purpose of testing
(external to Jet), some sequence of messages are published into a
pre-defined topic. After that, by using the Pulsar source,  we run a job
that reads from that topic. As the last step of the job,  we collect the
items and check whether all of the messages are read. To test the Pulsar
sink, we perform the steps above in reverse order.

To check fault tolerance support of PulsarReader source, distributed
node failure recovery is simulated. Two Jet instances are created and
the job is submitted to them. Once we make sure at least one snapshot is
created for the job, we enforce the job to restart by killing one of the
Jet instances and check if it manages not to lose or duplicate any
messages.

## Future Improvements

### Make the Source using Reader API Distributed

- The current version of the source using Reader API does not support
  distributed processing. It can be enabled by adding a custom partition
  mapping mechanism to it.

### Exactly-once at the sink

- If a mechanism for ordering processing items add to the Jet pipelines,
  then the exactly-once processing semantics can be achieved on the
  Pulsar sink

## Code Samples

### Pulsar Reader Source

Example code for Pulsar Reader Source can be found in this
[tutorial](../tutorials/pulsar.md).

### Pulsar Consumer Source

The program below creates a job that connects the local Pulsar cluster
located at `localhost:6650`(default address) and then consumes messages
from the pulsar topic, `hazelcast-demo-topic`,

```java
package com.hazelcast.jet.contrib.pulsar;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class PulsarConsumerDemo {

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        final StreamSource<Integer> pulsarConsumerSource = PulsarSources.pulsarConsumerBuilder(
                "hazelcast-demo-topic",
                () -> PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build(),
                () -> Schema.INT32,
                Message::getValue).build();
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(pulsarConsumerSource)
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        jet.newJob(pipeline).join();
    }
}
```

The created source above uses default client configurations, you can
change them by using builder methods.

### Pulsar Sink

The program below creates a job that connects the local Pulsar cluster
located at `localhost:6650`(default address) and then publishes messages
to the pulsar topic, `hazelcast-demo-topic`,

```java
package com.hazelcast.jet.contrib.pulsar;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;

import com.hazelcast.jet.pipeline.test.TestSources;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.HashMap;
import java.util.Map;


public class PulsarProducerDemo {
    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        Pipeline p = Pipeline.create();
        Sink<Integer> pulsarSink = PulsarSinks.builder(
                "hazelcast-demo-topic",
                () -> PulsarClient.builder()
                                  .serviceUrl("pulsar://localhost:6650")
                                  .build(),
                () -> Schema.INT32,
                FunctionEx.identity()).build();
        p.readFrom(TestSources.itemStream(15))
         .withoutTimestamps()
         .map(x -> (int) x.sequence())
         .writeTo(pulsarSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("hazelcast-pulsar-producer");
        jet.newJob(p, jobConfig).join();
    }
}
```
