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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;


/**
 * See {@link PulsarSources#pulsarConsumerBuilder(List, SupplierEx, SupplierEx, FunctionEx)}}
 *
 * @param <M> the type of the value of message read by {@code PulsarConsumer}
 * @param <T> the type of the emitted item after projection.
 */
public final class PulsarConsumerBuilder<M, T> implements Serializable {
    private final List<String> topics;
    private final SupplierEx<PulsarClient> connectionSupplier;
    private final SupplierEx<Schema<M>> schemaSupplier;
    private final FunctionEx<Message<M>, T> projectionFn;

    private Map<String, Object> consumerConfig;
    private SupplierEx<BatchReceivePolicy> batchReceivePolicySupplier;

    /**
     * Required fields of Pulsar consumer.
     *
     * @param topics                     the topics to consume, at least one is required
     * @param connectionSupplier         Pulsar client supplier
     * @param schemaSupplier             supplies the schema for consuming messages
     * @param projectionFn               converts a Pulsar message to an emitted item.
     */
    public PulsarConsumerBuilder(@Nonnull List<String> topics,
                                 @Nonnull SupplierEx<PulsarClient> connectionSupplier,
                                 @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                                 @Nonnull FunctionEx<Message<M>, T> projectionFn) {
        checkSerializable(topics, "topics");
        checkSerializable(connectionSupplier, "connectionSupplier");
        checkSerializable(schemaSupplier, "schemaSupplier");
        checkSerializable(projectionFn, "projectionFn");

        this.topics = topics;
        this.connectionSupplier = connectionSupplier;
        this.schemaSupplier = schemaSupplier;
        this.projectionFn = projectionFn;
        this.consumerConfig = getDefaultConsumerConfig();
        this.batchReceivePolicySupplier = getDefaultBatchReceivePolicySupplier();
    }

    private static Map<String, Object> getDefaultConsumerConfig() {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("consumerName", "hazelcast-jet-consumer");
        consumerConfig.put("subscriptionName", "hazelcast-jet-subscription");
        return consumerConfig;
    }

    private static SupplierEx<BatchReceivePolicy> getDefaultBatchReceivePolicySupplier() {
        final int maxNumMessages = 512;
        final int timeoutInMs = 1000;
        return () -> BatchReceivePolicy.builder()
                                       .maxNumMessages(maxNumMessages)
                                       .timeout(timeoutInMs, TimeUnit.MILLISECONDS)
                                       .build();
    }

    /**
     * @param consumerConfig Pulsar consumer configurations that must
     *                       contain consumer name, and subscription name.
     */
    @Nonnull
    public PulsarConsumerBuilder<M, T> consumerConfig(
            @Nonnull Map<String, Object> consumerConfig
    ) {
        checkSerializable(consumerConfig, "consumerConfig");
        this.consumerConfig = consumerConfig;
        return this;
    }

    /**
     * @param batchReceivePolicySupplier supplies the batch receive policy for the consumer
     */
    @Nonnull
    public PulsarConsumerBuilder<M, T> batchReceivePolicySupplier(
            @Nonnull SupplierEx<BatchReceivePolicy> batchReceivePolicySupplier
    ) {
        checkSerializable(batchReceivePolicySupplier, "batchReceivePolicySupplier");
        this.batchReceivePolicySupplier = batchReceivePolicySupplier;
        return this;
    }


    /**
     * Creates and returns the Pulsar Consumer {@link StreamSource} with using builder configurations set before.
     */
    @Nonnull
    public StreamSource<T> build() {
        return SourceBuilder.timestampedStream("pulsar-consumer-source",
                ctx -> new PulsarConsumerBuilder.ConsumerContext<>(
                        ctx.logger(), connectionSupplier.get(), topics, consumerConfig,
                        schemaSupplier, batchReceivePolicySupplier, projectionFn))
                .<T>fillBufferFn(PulsarConsumerBuilder.ConsumerContext::fillBuffer)
                .destroyFn(PulsarConsumerBuilder.ConsumerContext::destroy)
                .distributed(2)
                .build();
    }

    /**
     * A context object for the consumer source of Apache Pulsar
     *
     * @param <M> the type of the value of message read by {@code PulsarConsumer}
     * @param <T> the type of the emitted item after projection.
     */
    private static final class ConsumerContext<M, T> {
        private final ILogger logger;
        private final PulsarClient client;
        private final Consumer<M> consumer;
        private final FunctionEx<Message<M>, T> projectionFn;

        private ConsumerContext(
                @Nonnull ILogger logger,
                @Nonnull PulsarClient client,
                @Nonnull List<String> topics,
                @Nonnull Map<String, Object> consumerConfig,
                @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                @Nonnull SupplierEx<BatchReceivePolicy> batchReceivePolicySupplier,
                @Nonnull FunctionEx<Message<M>, T> projectionFn
        ) throws PulsarClientException {

            this.logger = logger;
            this.projectionFn = projectionFn;
            this.client = client;
            this.consumer = client.newConsumer(schemaSupplier.get())
                                  .topics(topics)
                                  .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                  .loadConf(consumerConfig)
                                  .batchReceivePolicy(batchReceivePolicySupplier.get())
                                  .subscriptionType(SubscriptionType.Shared)
                                  .subscribe();
        }


        /**
         * Receive the messages as a batch. The {@link BatchReceivePolicy} is
         * configured while creating the Pulsar {@link Consumer}.
         * In this method, emitted items are created by applying the projection function
         * to the messages received from Pulsar client. If there is an event time
         * associated with the message, it sets the event time as the timestamp of the
         * emitted item. Otherwise, it sets the publish time(which always exists)
         * of the message as the timestamp.
         */
        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) throws PulsarClientException {
            Messages<M> messages = consumer.batchReceive();
            for (Message<M> message : messages) {
                if (message.getEventTime() != 0) {
                    sourceBuffer.add(projectionFn.apply(message), message.getEventTime());
                } else {
                    sourceBuffer.add(projectionFn.apply(message), message.getPublishTime());
                }
            }
            consumer.acknowledgeAsync(messages)
                    .exceptionally(t -> {
                        logger.warning(buildLogMessage(messages));
                        return null;
                    });
        }

        private String buildLogMessage(Messages<M> messages) {
            StringBuilder builder = new StringBuilder();
            builder.append("Received batch with message ids: ");
            String prefix = "";
            for (Message<M> message : messages) {
                builder.append(prefix);
                prefix = ", ";
                builder.append(message.getMessageId());
            }
            builder.append(" cannot be acknowledged.");
            return builder.toString();
        }

        private void destroy() {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                logger.warning("Error while closing the 'PulsarConsumer'.", e);
            }
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                logger.warning("Error while shutting down the 'PulsarClient'.", e);
            }
        }
    }

}
