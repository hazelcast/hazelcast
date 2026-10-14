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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.TestedVersions;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.ILoggerLogConsumer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
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

import javax.annotation.Nonnull;
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
    private static final ILogger LOGGER = Logger.getLogger(PulsarTestSupport.class.getName());
    public static PulsarContainer pulsarContainer;
    static final int NUM_PARTITIONS = 12;
    protected static final String BROKER_HOST = "pulsar-broker";
    protected static final Network NETWORK = Network.newNetwork();

    private static final Map<String, Producer<byte[]>> producerMap = new HashMap<>();
    private static final Map<String, Consumer<Double>> consumerMap = new HashMap<>();
    private static final int QUEUE_CAPACITY = 1000;
    private static PulsarClient client;

    @Parameter(0)
    static DockerImageName imageName;

    @BeforeParameterizedClassInvocation
    static void startContainer(DockerImageName dockerImageName) throws PulsarClientException, PulsarAdminException {
        pulsarContainer = new PulsarContainer(dockerImageName)
                              .withNetwork(NETWORK)
                              .withTransactions()
                              .withNetworkAliases(BROKER_HOST)
                              .withEnv("transactionBufferClientOperationTimeoutInMills", "9")
                              .withEnv("PULSAR_LOG_LEVEL", "ERROR")
                              .withLogConsumer(new ILoggerLogConsumer(LOGGER));
        pulsarContainer.start();
        try (var admin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getHttpServiceUrl()).build()) {
            admin.namespaces().setDeduplicationStatus("public/default", true);
        }
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

    protected static @Nonnull SupplierEx<PulsarClient> getConnectionSupplier() {
        return () -> PulsarClient.builder().serviceUrl(getServiceUrl()).enableTransaction(true).build();
    }

    protected static Producer<byte[]> getProducer(String topicName) {
        // If there exists a producer with same name returns it.
        try {
            if (!producerMap.containsKey(topicName)) {
                Producer<byte[]> newProducer = getClient()
                                                   .newProducer()
                                                   .topic(topicName)
                                                   .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                                                   .sendTimeout(10, TimeUnit.SECONDS)
                                                   .blockIfQueueFull(true)
                                                   .autoUpdatePartitions(true)
                                                   .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                                                   .create();
                producerMap.put(topicName, newProducer);
                return newProducer;
            } else {
                return producerMap.get(topicName);
            }
        } catch (PulsarClientException e) {
            throw sneakyThrow(e);
        }
    }

    protected static void produceMessages(String message, String topicName, int count) {
        produceMessages(message, topicName, 0, count);
    }

    protected static void produceMessages(String message, String topicName, int start, int end) {
        for (int i = start; i < end; i++) {
            produceMessage(message + "-" + i, topicName);
        }
    }

    protected static void produceMessage(String message, String topicName) {
        try {
            getProducer(topicName).send(message.getBytes(StandardCharsets.UTF_8));
        } catch (PulsarClientException e) {
            sneakyThrow(e);
        }
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

    public static Stream<Arguments> pulsarVersions() {
        String property = System.getProperty("testWithAllPulsarVersions", "false");
        if (property.equalsIgnoreCase("true")) {
            return Stream.of(
                arguments(TestedVersions.PULSAR_4_IMAGE),
                arguments(TestedVersions.PULSAR_5_IMAGE)
            );
        }
        return Stream.of(
            arguments(TestedVersions.PULSAR_4_IMAGE)
        );
    }

    protected SupplierEx<PulsarClient> notSerializableConnectionSupplier() {
        return new SupplierEx<>() {
            // used to make class non-serializable
            @SuppressWarnings("unused")
            private final Object object = new Object();

            @Override
            public PulsarClient getEx() {
                return null;
            }
        };
    }

    static void createTopic(String topicName, boolean partitioned) {
        createTopic(topicName, partitioned ? NUM_PARTITIONS : null);
    }

    static void createTopic(String topicName, Integer partitionCount) {
        boolean partitioned = partitionCount != null;
        try (var admin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getHttpServiceUrl()).build()) {
            if (partitioned) {
                admin.topics().createPartitionedTopic(topicName, partitionCount);
            } else {
                admin.topics().createNonPartitionedTopic(topicName);
            }
        } catch (PulsarClientException | PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }
}
