/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.pulsar;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.TestedVersions;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ParameterizedClass
@MethodSource("pulsarVersions")
public abstract class PulsarTestSupport extends JetTestSupport {
    public static PulsarContainer pulsarContainer;
    protected static final String BROKER_HOST = "pulsar-broker";
    protected static Network network = Network.newNetwork();

    private static final Map<String, Producer<byte[]>> producerMap = new HashMap<>();
    private static final Map<String, Consumer<Double>> consumerMap = new HashMap<>();
    private static final int QUEUE_CAPACITY = 1000;
    private static PulsarClient client;

    @Parameter(0)
    static DockerImageName imageName;

    @BeforeParameterizedClassInvocation
    static void startContainer(DockerImageName dockerImageName) {
        pulsarContainer = new PulsarContainer(dockerImageName)
                              .withNetwork(network)
                              .withNetworkAliases(BROKER_HOST);
        pulsarContainer.start();
    }

    @AfterParameterizedClassInvocation
    static void shutdown() {
        producerMap.forEach((s, producer) -> closeResource(producer));
        consumerMap.forEach((s, consumer) -> closeResource(consumer));

        closeResource(client);
        client = null;
        pulsarContainer.close();
    }

    protected static String getServiceUrl() {
        return pulsarContainer.getPulsarBrokerUrl();
    }

    protected static PulsarClient getClient() throws PulsarClientException {
        if (client == null) {
            client = PulsarClient.builder()
                                 .serviceUrl(getServiceUrl())
                                 .build();
        }
        return client;
    }

    private static Producer<byte[]> getProducer(String topicName) throws PulsarClientException {
        // If there exists a producer with same name returns it.
        if (!producerMap.containsKey(topicName)) {
            Producer<byte[]> newProducer = getClient()
                    .newProducer()
                    .topic(topicName)
                    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                    .sendTimeout(10, TimeUnit.SECONDS)
                    .blockIfQueueFull(true)
                    .create();
            producerMap.put(topicName, newProducer);
            return newProducer;
        } else {
            return producerMap.get(topicName);
        }
    }

    protected static void produceMessages(String message, String topicName, int count) {
        for (int i = 0; i < count; i++) {
            try {
                produceMessage(message + "-" + i, topicName);
            } catch (PulsarClientException e) {
                sneakyThrow(e);
            }
        }
    }

    protected static void produceMessage(String message, String topicName)
            throws PulsarClientException {
        getProducer(topicName).send(message.getBytes(StandardCharsets.UTF_8));
    }


    protected static List<Double> consumeMessages(String topicName, int count)
            throws PulsarClientException {
        List<Double> list = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            Message<Double> message = consumeMessage(topicName);
            if (message != null) {
                list.add(message.getValue());
            }
        }
        return list;
    }

    protected static Message<Double> consumeMessage(String topicName) throws PulsarClientException {
        return getConsumer(topicName).receive(1, TimeUnit.SECONDS);
    }

    protected static Consumer<Double> getConsumer(String topicName) throws PulsarClientException {
        if (!consumerMap.containsKey(topicName)) {
            Consumer<Double> newConsumer = getClient()
                    .newConsumer(Schema.DOUBLE)
                    .topic(topicName)
                    .consumerName("hazelcast-jet-consumer-" + topicName)
                    .subscriptionName("hazelcast-jet-subscription")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .receiverQueueSize(QUEUE_CAPACITY)
                    .subscribe();
            consumerMap.put(topicName, newConsumer);
            return newConsumer;
        } else {
            return consumerMap.get(topicName);
        }
    }

    protected static StreamSource<String> setupConsumerSource(String topicName,
                                                              FunctionEx<Message<byte[]>, String> projectionFn) {
        return PulsarSources.pulsarConsumer(
                topicName,
                () -> PulsarClient.builder().serviceUrl(getServiceUrl()).build(),
                () -> Schema.BYTES,
                projectionFn);
    }

    protected static StreamSource<String> setupReaderSource(String topicName,
                                                            FunctionEx<Message<byte[]>, String> projectionFn) {
        return PulsarSources.pulsarReader(
                topicName,
                () -> PulsarClient.builder().serviceUrl(getServiceUrl()).build(),
                () -> Schema.BYTES,
                projectionFn);
    }

    protected static Sink<Integer> setupSink(String topicName) {
        return PulsarSinks.pulsarSink(topicName,
                                      () -> PulsarClient.builder().serviceUrl(getServiceUrl()).build(),
                                      () -> Schema.DOUBLE, Integer::doubleValue);
    }

    public static Stream<Arguments> pulsarVersions() {
        return Stream.of(
            arguments(TestedVersions.PULSAR_4_IMAGE),
            arguments(TestedVersions.PULSAR_5_IMAGE)
        );
    }
}
