---
title: 015 - Mqtt Connector
description: Mqtt Connector (source and sink)
---

*Since*: 4.4

## Background

MQTT is a machine-to-machine (M2M)/”Internet of Things” connectivity
protocol. It was designed as an extremely lightweight publish/subscribe
messaging transport.

## MQTT Protocol

### Publish

MQTT utilizes subject-based(topics) filtering of the messages on the
broker. Each message contains a topic name, payload (binary format),
QoS (Quality of Service Level), identifier and retain-flag.

Topic Name: a simple string that is hierarchically structured with
'/' as delimiters. For example, “myhome/livingroom/temperature”.

Payload: the actual content of the message in binary format.

QoS: a number indicates the delivery guarantee of the message.

- 0, fire and forget
- 1, at least once (using acknowledgement)
- 2, exactly once (using 2-phase acknowledgement)

Identifier: a short number that uniquely identifies the messages
between the client and broker. This is relevant only for QoS levels
greater than 0. The identifier is not unique between all clients.
Once the message flow is complete, the client can reuse the identifier.

Retain Flag: flag that defines whether the message is saved by the
broker as the last known good value for the specified topic. When a new
client subscribes to a topic, it receives the last message that is
retained on that topic.

Publishing means sending a message for a specific topic. The broker
reads, acknowledges (according to QoS) and processes the message.
Processing means determining the clients subscribed to the specified
topic and sending the message to them. The publishing client does not
know anything about the processing.

### Subscribe

To receive messages on topics of interest, the client sends a
`SUBSCRIBE` message to the broker. The message contains a list of
subscriptions which consists of a topic and a QoS level. The topic can
contain wildcards that make it possible to subscribe to a topic pattern
rather than a specific topic.

The broker sends back a return code indicating the QoS level granted
for each of the subscriptions. Messages published at a lower QoS will be
received at the published QoS. Messages published at a higher quality
of service will be received using the QoS specified on the subscription.
If the broker refuses a subscription, the return code indicates the
failure rather than QoS. For example, the client may have insufficient
permission to subscribe to the topic, or the topic may be malformed.

All messages sent with QoS 1 and 2 are queued for offline clients until
the client is available again. However, this queuing is only possible
if the client has a persistent session.

## MQTT Clients

We need to pick a java MQTT client for source and sink to connect to
the broker. There are several options out there:

### Paho Java Client

The Eclipse Paho project provides open-source client implementations of
MQTT protocols for various languages and [Paho Java Client](https://www.eclipse.org/paho/clients/java/)
is one of them. The client offers synchronous and asynchronous APIs.
The sync one is a wrapper to the asynchronous one. Paho Java Client
does not support MQTT 5.0 protocol yet. It is a work in progress.

If not configured explicitly, the client will try to connect to the
broker using MQTT 3.1.1 protocol. If it fails to connect, the client
falls back to MQTT 3.0 protocol.

The client supports:

- LWT: Last will and testament
- SSL/TLS: secure connection
- Message persistence: Client persists messages in case of an
  application crash
- Automatic reconnect: Automatically reconnects
- Offline buffering: Client buffers messages whilst offline to send
  when reconnects.
- WebSocket: Client can connect to brokers that support WebSockets
- High Availability: You can configure multiple brokers and in case of
  a failure, the client tries other brokers.

It is lightweight(240 KB) and a single jar without any dependencies.
We can say that it is one of the most popular clients.

### HiveMQ Client

HiveMQ Client uses `Netty` for handling networking and `RxJava` for
handling the asynchronous streaming of messages. The client provides
three distinct flavours of API: blocking, asynchronous and reactive.
HiveMQ Client supports MQTT 5.0 protocol as well as 3.1.1.

The client supports:

- LWT: Last will and testament
- SSL/TLS: secure connection
- Automatic reconnect: Automatically reconnects
- Offline buffering: Client buffers messages whilst offline to send
  when reconnects.
- WebSocket: Client can connect to brokers that support WebSockets
- Backpressure handling: ask the producers to throttle their output
  back (MQTT 5.0 feature, broker needs to have/enabled the feature)

HiveMQ Client is not lightweight compared to Paho Java client, 1.1 MB.
It has also `netty` and `rxjava` dependencies.

## MQTT Versions

- 1999 MQTT invention
- 2010 MQTT 3.1 Royalty-free release
- 2014 MQTT 3.1.1 OASIS standard
- 2016 MQTT 3.1.1 ISO standard
- 2018 MQTT 5 Initial release
- 2019 MQTT 5 OASIS standard

MQTT 5 brings new features like `Shared subscriptions` and `Time to
live` for messages and client sessions and many more. While these new
features look promising, I couldn't find any information regarding the
adoption rate of MQTT 5. The only java client supporting MQTT 5 is
`HiveMQ Client` and for other languages, I've found only a single
library or none at all.

## MQTT Connector

We chose `Paho Java Client` over `HiveMQ Client` since it is
lightweight and without dependencies. `Paho Java Client` does not
support MQTT 5, but the adoption rate of MQTT 5 is questionable.

### Source

We use our `SourceBuilder` to create a streaming source for MQTT
messages. The source is not distributed, it creates a client on one of
the members and subscribes to the topics.

The subscription mechanism is push-based. We set a callback to the
client, and it is called as the messages arrive. Since our
`SourceBuilder` is designed for pull-based systems, we buffer the
messages to a blocking queue and drain them in the `fillBufferFn`. We
apply the given mapping function to the binary message and keep the
mapped item in the queue.

#### API

Since there are several configuration options for the source, we
created a `MqttSourceBuilder` to configure and build the source. We've
also introduced a class named `Subscription` which consists of the
topic and quality of service for that topic.

Below is a usage example of the source builder with all the
configuration options:

```java
MqttSources.builder()
        .clientId("consumer")
        .broker("tcp://localhost:1883")
        .auth("username", "password".toCharArray())
        .topic("topic")
        .qualityOfService(QualityOfService.EXACTLY_ONCE)
        .autoReconnect()
        .keepSession()
        .mapToItemFn((topic, message) -> message.toString())
        .build();
```

You can also subscribe to multiple topics and provide a
`MqttConnectOptions` function instead of configuring the options one by
one:

```java
MqttSources.builder()
        .clientId("consumer")
        .broker("tcp://localhost:1883")
        .subscriptions(Subscription.of("topic1"), Subscription.of("topic2", QualityOfService.EXACTLY_ONCE))
        .connectOptionsFn(() -> {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setUserName("username");
            options.setPassword("password".toCharArray());
            options.setMaxInflight(100);
            return options;
        })
        .mapToItemFn((topic, message) -> message.toString())
        .build();
```

#### Fault Tolerance

To support fault tolerance for a source, Jet needs the remote system to
support one of these:

- provide an offset. After a failure, we'll use that offset and replay
the messages

- support manual acknowledgement. Jet will ack the messages in the
second phase of the snapshot

MQTT provides acknowledgements, however, the Paho client we use
acknowledges the messages automatically. There are other clients that
support manual acknowledgements, however the whole MQTT protocol is
designed for IoT and small resource usage. Many brokers (for example
Mosquitto) limit the number of non-acknowledged messages to a low value
(100 in case of Mosquitto). Therefore we can't really rely on this
feature.

We've also considered an option where Jet itself would save the messages
received since the last snapshot. We cannot save it to the snapshot
because the snapshot stores state created _before_ the snapshot is
taken, but we need to save messages received _after_ the snapshot. We
could save them to an IMap and maybe we could provide a persistence
implementation for Paho that we'll be able to "roll back". However, we
didn't implement a PoC for this option. We decided to not support any
fault tolerance for the MQTT source. It can be implemented in the future
if there's demand. Currently, if a fault-tolerant job fails, the source
will simply start where it left off and the messages acked since the
last snapshot will be lost.

If a client subscribes to a topic with quality of service `AT_LEAST_ONCE`
or `EXACTLY_ONCE` and connects to the broker with `cleanSession=false`,
then the broker keeps the messages in case of a disconnection. The broker
serves these buffered messages once the client is re-connected. You
need to use a unique identifier for the client.

Paho client has an `autoReconnect` option, in case of a disconnect, the
client tries to reconnect to the broker. After the reconnection, source
re-subscribes to the topics.

### Sink

We use `SinkBuilder` to create a sink for MQTT messages. The sink
creates a client for each processor and publishes messages to the
specified topic. We append the global processor index to the specified
clientId for uniqueness, e.g `producer-1`, `producer-2`...

#### API

Since there are several configuration options for the sink, we created
a `MqttSinkBuilder` to configure and build the sink.

Below is a usage example of the sink builder with all the configuration
options:

```java
MqttSinks.builder()
        .clientId("producer")
        .broker("tcp://localhost:1883")
        .auth("username", "password".toCharArray())
        .topic("topic")
        .autoReconnect()
        .keepSession()
        .retryStrategy(RetryStrategies.indefinitely(1000))
        .messageFn(item -> {
            MqttMessage message = new MqttMessage(item.getBytes());
            message.setQos(2);
            return message;
        })
        .build();
```

You can provide a `MqttConnectOptions` function instead of configuring
the options one by one.

```java
MqttSinks.builder()
        .clientId("producer")
        .broker("tcp://localhost:1883")
        .connectOptionsFn(() -> {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setUserName("username");
            options.setPassword("password".toCharArray());
            options.setMaxInflight(100);
            return options;
        })
        .retryStrategy(RetryStrategies.indefinitely(1000))
        .topic("topic")
        .messageFn(item -> {
            MqttMessage message = new MqttMessage(item.getBytes());
            message.setQos(2);
            return message;
        })
        .build();
```

#### Fault Tolerance

The MQTT sink provides at-least-once guarantee - it sends the messages
synchronously. If a job fails and restarts, the same messages will be
sent again.

#### Error handling

The sink uses sync client to publish the messages. Any error/exception
encountered while publishing the messages will fail the job. User can
configure the retrying of the messages by providing a retry-strategy.
