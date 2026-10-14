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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ThrowingSupplier;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pulsar.impl.PulsarSourceConfig;
import com.hazelcast.logging.ILogger;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.jspecify.annotations.NonNull;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotEmpty;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Objects.requireNonNull;

/**
 * See {@link PulsarSources#pulsarConsumerBuilder(List, SupplierEx, SupplierEx, FunctionEx)}}
 *
 * @param <M> the type of the value of message read by {@code PulsarConsumer}
 * @param <T> the type of the emitted item after projection.
 *
 * @since 6.0
 */
public final class PulsarConsumerBuilder<M, T> {
    private static final int PREFERRED_LOCAL_PARALLELISM = 4;
    private final PulsarSourceConfig<M, T> conf = new PulsarSourceConfig<>();
    private FunctionEx<Message<M>, T> projectionFn;

    public PulsarConsumerBuilder(@Nonnull SupplierEx<Schema<M>> schemaSupplier, @Nonnull FunctionEx<Message<M>, T> projectionFn) {
        checkNonNullAndSerializable(schemaSupplier, "schemaSupplier");
        checkNonNullAndSerializable(projectionFn, "projectionFn");
        conf.setLocalParallelism(PREFERRED_LOCAL_PARALLELISM);
        conf.setSchemaSupplier(schemaSupplier);
        conf.setOptions(getDefaultConsumerConfig());
        conf.setBatchReceivePolicy(getDefaultBatchReceivePolicySupplier().get());
        this.projectionFn = projectionFn;
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
        this(schemaSupplier, projectionFn);
        checkSerializable(topics, "topics");
        checkSerializable(connectionSupplier, "connectionSupplier");

        conf.setTopics(topics);
        conf.setConnectionSupplier(connectionSupplier);
    }

    @Nonnull
    public PulsarConsumerBuilder<M, T> topic(@Nonnull String... topics) {
        checkNotEmpty(topics, "topics");
        conf.setTopics(List.of(topics));
        return this;
    }

    @Nonnull
    public PulsarConsumerBuilder<M, T> subscriberName(@Nonnull String subscriberName) {
        checkNotNull(subscriberName, "subscriberName");
        conf.setSubscriberName(subscriberName);
        return this;
    }

    @Nonnull
    public PulsarConsumerBuilder<M, T> connectionSupplier(@Nonnull SupplierEx<PulsarClient> connectionSupplier) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        conf.setConnectionSupplier(connectionSupplier);
        return this;
    }

    @Nonnull
    public PulsarConsumerBuilder<M, T> dataConnectionRef(@Nonnull DataConnectionRef dataConnectionRef) {
        requireNonNull(dataConnectionRef, "dataConnectionRef");
        conf.setDataConnectionRef(dataConnectionRef);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <NEW_T> PulsarConsumerBuilder<M, NEW_T> projectionFn(@Nonnull FunctionEx<@NonNull Message<M>, NEW_T> projectionFn) {
        this.projectionFn = (FunctionEx<Message<M>, T>) projectionFn;
        return (PulsarConsumerBuilder<M, NEW_T>) this;
    }

    /**
     * Allows additional customization and invocation of other {@link ConsumerBuilder} method,
     * that doesn't have a representative here.
     */
    @Nonnull
    public PulsarConsumerBuilder<M, T> consumerCustomizer(@Nonnull ConsumerEx<@NonNull ConsumerBuilder<M>> consumerCustomizer) {
        checkNonNullAndSerializable(consumerCustomizer, "consumerCustomizer");
        conf.setConsumerCustomizer(consumerCustomizer);
        return this;
    }

    /**
     * @param consumerConfig Pulsar consumer settings, subject to the same
     *                       restrictions as {@link #consumerCustomizer(ConsumerEx)}
     */
    @Nonnull
    public PulsarConsumerBuilder<M, T> consumerConfig(@Nonnull Map<String, Object> consumerConfig) {
        checkSerializable(consumerConfig, "consumerConfig");
        conf.setOptions(consumerConfig);
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
        conf.setBatchReceivePolicy(batchReceivePolicySupplier.get());
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
     * Preferred local parallelism for every processor created by this builder.
     */
    public PulsarConsumerBuilder<M, T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.conf.setLocalParallelism(Vertex.checkLocalParallelism(preferredLocalParallelism));
        return this;
    }

    /**
     * Creates a Pulsar consumer source with the configured projection.
     *
     * <p>Each physical topic is assigned to one processor using an exclusive,
     * durable subscription. Use a subscription name dedicated to this source.
     * Partition changes are discovered when the job restarts.
     *
     * <p>With {@code AT_LEAST_ONCE} or {@code EXACTLY_ONCE}, the source acknowledges
     * messages only after a successful snapshot and filters replayed messages
     * using snapshotted positions. With {@code NONE}, messages are acknowledged
     * after emission. The broker must retain unacknowledged messages: message TTL,
     * backlog eviction and subscription expiration must not discard them.
     * Non-persistent topics cannot be used with snapshotting.
     *
     * <p>Recovery uses the latest committed snapshot and the existing subscription.
     * Restoring an older exported snapshot after newer acknowledgements is not
     * supported. Snapshots written by the earlier consumer implementation are
     * incompatible. End-to-end exactly-once also requires suitable downstream
     * processors and sinks.
     */
    @Nonnull
    public StreamSource<T> build() {
        conf.setProjectionFn(projectionFn);
        conf.validate();
        var localConf = new PulsarSourceConfig<>(conf);
        return SourceBuilder.timestampedStream("pulsar-consumer-source",
                                               ctx -> new PulsarConsumerBuilder.ConsumerContext<>(ctx, localConf))
                            .<T>fillBufferFn(PulsarConsumerBuilder.ConsumerContext::fillBuffer)
                            .initFn(ctx -> requireNonNull(ctx).init())
                            .destroyFn(ctx -> requireNonNull(ctx).destroy())
                            .distributed(conf.getLocalParallelism())
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

        private SupplierEx<PulsarClient> clientSupplier;
        private ThrowingSupplier<Consumer<M>> consumerSupplier;

        private ConsumerContext(
                @Nonnull Processor.Context ctx, @Nonnull PulsarSourceConfig<M, T> conf
        ) {
            this.logger = ctx.logger();

            // we use lambdas to avoid flooding the class with a lot of one-use fields.
            this.clientSupplier = conf.getTargetConnectionSupplier(ctx);
            this.consumerSupplier = () -> {
                ConsumerBuilder<M> builder = client.newConsumer(conf.getSchemaSupplier().get());
                conf.getConsumerCustomizer().accept(builder);
                return builder
                      .subscriptionName(conf.getSubscriberName())
                      .topics(conf.getTopics())
                      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                      .loadConf(conf.getOptions())
                      .batchReceivePolicy(conf.getBatchReceivePolicy())
                      .subscriptionType(SubscriptionType.Shared)
                      .subscribe();
            };
            this.projectionFn = conf.getProjectionFn();
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
         * emitted item. Otherwise, it sets the publishing time(which always exists)
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
