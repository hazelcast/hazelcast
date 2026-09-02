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
import com.hazelcast.function.ThrowingSupplier;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.DataConnectionRef;
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
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Validation.validate;
import static com.hazelcast.jet.pulsar.Utils.exactlyOnlyOneIsNotNull;
import static java.util.Objects.requireNonNull;

/**
 * See {@link PulsarSources#pulsarConsumerBuilder(List, SupplierEx, SupplierEx, FunctionEx)}}
 *
 * @param <M> the type of the value of message read by {@code PulsarConsumer}
 * @param <T> the type of the emitted item after projection.
 */
public final class PulsarConsumerBuilder<M, T> implements Serializable {
    private List<String> topics;
    private SupplierEx<PulsarClient> connectionSupplier;
    private DataConnectionRef dataConnectionRef;
    private final SupplierEx<Schema<M>> schemaSupplier;
    private FunctionEx<Message<M>, T> projectionFn;

    private Map<String, Object> consumerConfig;
    private SupplierEx<BatchReceivePolicy> batchReceivePolicySupplier;

    public PulsarConsumerBuilder(@Nonnull SupplierEx<Schema<M>> schemaSupplier, @Nonnull FunctionEx<Message<M>, T> projectionFn) {
        checkNonNullAndSerializable(schemaSupplier, "schemaSupplier");
        this.schemaSupplier = schemaSupplier;
        this.consumerConfig = getDefaultConsumerConfig();
        this.batchReceivePolicySupplier = getDefaultBatchReceivePolicySupplier();
        this.projectionFn = checkNonNullAndSerializable(projectionFn, "projectionFn");
    }

    /**
     * All-required fields constructor of Pulsar consumer, kept for compatibility with older code
     * from hazeclast-jet-contrib repository.
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

    @Nonnull
    public PulsarConsumerBuilder<M, T> topic(@Nonnull String... topics) {
        this.topics = List.of(topics);
        return this;
    }

    @Nonnull
    public PulsarConsumerBuilder<M, T> connectionSupplier(SupplierEx<PulsarClient> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
        return this;
    }

    @Nonnull
    public PulsarConsumerBuilder<M, T> dataConnectionRef(@Nonnull DataConnectionRef dataConnectionRef) {
        requireNonNull(dataConnectionRef, "dataConnectionRef");
        this.dataConnectionRef = dataConnectionRef;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <NEW_T> PulsarConsumerBuilder<M, NEW_T> projectionFn(FunctionEx<Message<M>, NEW_T> projectionFn) {
        this.projectionFn = (FunctionEx<Message<M>, T>) projectionFn;
        return (PulsarConsumerBuilder<M, NEW_T>) this;
    }

    /**
     * @param consumerConfig Pulsar consumer configurations that must
     *                       contain consumer name, and subscription name.
     */
    @Nonnull
    public PulsarConsumerBuilder<M, T> consumerConfig(@Nonnull Map<String, Object> consumerConfig) {
        checkSerializable(consumerConfig, "consumerConfig");
        this.consumerConfig = consumerConfig;
        return this;
    }

    private static Map<String, Object> getDefaultConsumerConfig() {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("consumerName", "hazelcast-jet-consumer");
        consumerConfig.put("subscriptionName", "hazelcast-jet-subscription");
        return consumerConfig;
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

    private static SupplierEx<BatchReceivePolicy> getDefaultBatchReceivePolicySupplier() {
        final int maxNumMessages = 512;
        final int timeoutInMs = 1000;
        return () -> BatchReceivePolicy.builder()
                                       .maxNumMessages(maxNumMessages)
                                       .timeout(timeoutInMs, TimeUnit.MILLISECONDS)
                                       .build();
    }


    /**
     * Creates and returns the Pulsar Consumer {@link StreamSource} with using builder configurations set before.
     */
    @Nonnull
    public StreamSource<T> build() {
        validate()
            .checkNotNull(topics, "topics")
            .checkNotNull(consumerConfig, "consumerConfig")
            .checkNotNullAndSerializable(schemaSupplier, "schemaSupplier")
            .checkNotNullAndSerializable(batchReceivePolicySupplier, "batchReceivePolicySupplier")
            .checkNotNullAndSerializable(projectionFn, "projectionFn")
            .checkSerializableIfNotNull(connectionSupplier, "connectionSupplier")
            .check(exactlyOnlyOneIsNotNull(connectionSupplier, dataConnectionRef),
                   "Either connectionSupplier or dataConnectionRef must be provided")
            .throwIfErrors();
        return SourceBuilder.timestampedStream("pulsar-consumer-source",
                                               ctx -> new PulsarConsumerBuilder.ConsumerContext<>(
                                                   ctx, connectionSupplier, dataConnectionRef, topics, consumerConfig,
                                                   schemaSupplier, batchReceivePolicySupplier, projectionFn))
                            .<T>fillBufferFn(PulsarConsumerBuilder.ConsumerContext::fillBuffer)
                            .initFn(ctx -> requireNonNull(ctx).init())
                            .destroyFn(ctx -> requireNonNull(ctx).destroy())
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
        private PulsarClient client;
        private Consumer<M> consumer;
        private final FunctionEx<Message<M>, T> projectionFn;

        private ThrowingSupplier<PulsarClient> clientSupplier;
        private ThrowingSupplier<Consumer<M>> consumerSupplier;

        private ConsumerContext(
                @Nonnull Processor.Context ctx,
                @Nullable SupplierEx<PulsarClient> clientSupplier,
                @Nullable DataConnectionRef dataConnectionRef,
                @Nonnull List<String> topics,
                @Nonnull Map<String, Object> consumerConfig,
                @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                @Nonnull SupplierEx<BatchReceivePolicy> batchReceivePolicySupplier,
                @Nonnull FunctionEx<Message<M>, T> projectionFn
        ) {
            this.logger = ctx.logger();
            // we use lambdas to avoid flooding the class with a lot of one-use fields.
            this.clientSupplier = () -> Utils.getClient(ctx, clientSupplier, dataConnectionRef);
            this.consumerSupplier = () -> client.newConsumer(schemaSupplier.get()).topics(topics)
                                                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                                .loadConf(consumerConfig)
                                                .batchReceivePolicy(batchReceivePolicySupplier.get())
                                                .subscriptionType(SubscriptionType.Shared)
                                                .subscribe();
            this.projectionFn = projectionFn;
        }

        public void init() {
            try {
                this.client = clientSupplier.get();
                this.consumer = consumerSupplier.get();

                clientSupplier = null;
                consumerSupplier = null;
            } catch (Exception e) {
                throw new JetException("Unable to initialize context for Pulsar Consumer", e);
            }
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
