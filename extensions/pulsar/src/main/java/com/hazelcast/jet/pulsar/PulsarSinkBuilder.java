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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.logging.ILogger;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * See {@link PulsarSinks#builder(String, SupplierEx, SupplierEx, FunctionEx)}
 *
 * @param <E> the type of stream item
 * @param <M> the type of the message published by {@code PulsarProducer}
 */
public final class PulsarSinkBuilder<E, M> implements Serializable {
    private final SupplierEx<PulsarClient> connectionSupplier;
    private final String topic;
    private final SupplierEx<Schema<M>> schemaSupplier;

    private Map<String, Object> producerConfig;
    private FunctionEx<? super E, M> extractValueFn;
    private FunctionEx<? super E, String> extractKeyFn;
    private FunctionEx<? super E, Map<String, String>> extractPropertiesFn;
    private FunctionEx<? super E, Long> extractTimestampFn;
    private int preferredLocalParallelism = 2;


    /**
     * Required fields of Pulsar sink
     *
     * @param topic              Pulsar topic name to publish to
     * @param connectionSupplier Pulsar client supplier
     * @param extractValueFn     extracts the message value from the emitted items.
     * @param schemaSupplier     Pulsar messaging schema supplier.
     */
    public PulsarSinkBuilder(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<? super E, M> extractValueFn
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        checkSerializable(schemaSupplier, "schemaSupplier");
        checkSerializable(extractValueFn, "extractValueFn");

        this.topic = topic;
        this.producerConfig = getDefaultProducerConfig();
        this.connectionSupplier = connectionSupplier;
        this.schemaSupplier = schemaSupplier;
        this.extractValueFn = extractValueFn;
    }

    private static Map<String, Object> getDefaultProducerConfig() {
        return new HashMap<>();
    }

    /**
     * @param producerConfig The configurations for {@code PulsarProducer}
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> producerConfig(
            Map<String, Object> producerConfig
    ) {
        checkSerializable(producerConfig, "producerConfig");
        this.producerConfig = producerConfig;
        return this;
    }

    /**
     * @param extractKeyFn extracts the message key from the emitted items.
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> extractKeyFn(
            FunctionEx<? super E, String> extractKeyFn
    ) {
        checkSerializable(extractKeyFn, "extractKeyFn");
        this.extractKeyFn = extractKeyFn;
        return this;
    }

    /**
     * @param extractPropertiesFn extracts the message properties from the emitted items.
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> extractPropertiesFn(
            FunctionEx<? super E, Map<String, String>> extractPropertiesFn
    ) {
        checkSerializable(extractPropertiesFn, "extractPropertiesFn");
        this.extractPropertiesFn = extractPropertiesFn;
        return this;
    }

    /**
     * @param extractTimestampFn the function that extracts the timestamp from the emitted item.
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> extractTimestampFn(
            FunctionEx<? super E, Long> extractTimestampFn
    ) {
        checkSerializable(extractTimestampFn, "extractTimestampFn");
        this.extractTimestampFn = extractTimestampFn;
        return this;
    }

    /**
     * See {@link SinkBuilder#preferredLocalParallelism(int)}.
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = Vertex.checkLocalParallelism(preferredLocalParallelism);
        return this;
    }

    /**
     * Creates and returns the Pulsar {@link Sink} with using builder configurations set before.
     */
    @Nonnull
    public Sink<E> build() {
        return SinkBuilder.sinkBuilder("pulsar-sink", ctx -> new PulsarSinkContext<>(ctx.logger(), topic,
                connectionSupplier.get(), producerConfig, schemaSupplier, extractValueFn,
                extractKeyFn, extractPropertiesFn, extractTimestampFn))
                .<E>receiveFn(PulsarSinkContext::add)
                .flushFn(PulsarSinkContext::flush)
                .destroyFn(PulsarSinkContext::destroy)
                .preferredLocalParallelism(preferredLocalParallelism)
                .build();
    }


    private static final class PulsarSinkContext<E, M> {
        private final ILogger logger;
        private final PulsarClient client;
        private final Producer<M> producer;

        private final FunctionEx<? super E, M> extractValueFn;
        private final FunctionEx<? super E, String> extractKeyFn;
        private final FunctionEx<? super E, Map<String, String>> extractPropertiesFn;
        private final FunctionEx<? super E, Long> extractTimestampFn;

        private final AtomicReference<Throwable> error = new AtomicReference<>();

        private PulsarSinkContext(
                @Nonnull ILogger logger,
                @Nonnull String topic,
                @Nonnull PulsarClient client,
                @Nonnull Map<String, Object> producerConfig,
                @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                @Nonnull FunctionEx<? super E, M> extractValueFn,
                @Nullable FunctionEx<? super E, String> extractKeyFn,
                @Nullable FunctionEx<? super E, Map<String, String>> extractPropertiesFn,
                @Nullable FunctionEx<? super E, Long> extractTimestampFn
        ) throws PulsarClientException {
            this.logger = logger;
            this.client = client;
            this.producer = client.newProducer(schemaSupplier.get())
                                  .topic(topic)
                                  .loadConf(producerConfig)
                                  .create();
            this.extractKeyFn = extractKeyFn;
            this.extractValueFn = extractValueFn;
            this.extractPropertiesFn = extractPropertiesFn;
            this.extractTimestampFn = extractTimestampFn;
        }

        private void flush() throws PulsarClientException {
            producer.flush();
            rethrowError();
        }

        private void add(E item) {
            rethrowError();
            TypedMessageBuilder<M> messageBuilder = producer.newMessage()
                                                            .value(extractValueFn.apply(item));
            if (extractKeyFn != null) {
                messageBuilder = messageBuilder.key(extractKeyFn.apply(item));
            }
            if (extractPropertiesFn != null) {
                messageBuilder = messageBuilder.properties(extractPropertiesFn.apply(item));
            }
            if (extractTimestampFn != null) {
                messageBuilder.eventTime(extractTimestampFn.apply(item));
            }
            messageBuilder.sendAsync()
                          .exceptionally(t -> {
                              error.set(t);
                              return null;
                          });
        }

        private void rethrowError() {
            Throwable throwable = error.get();
            if (throwable != null) {
                error.set(null);
                throw new JetException("Error during message send", throwable);
            }
        }

        private void destroy() {
            try {
                producer.close();
            } catch (PulsarClientException e) {
                logger.warning("Error while closing the 'PulsarProducer'.", e);
            }
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                logger.warning("Error while shutting down the 'PulsarClient'.", e);
            }
            rethrowError();
        }
    }

}
