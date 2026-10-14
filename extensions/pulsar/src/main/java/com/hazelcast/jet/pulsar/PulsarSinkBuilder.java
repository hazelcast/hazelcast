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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pulsar.impl.PulsarSinkConfig;
import com.hazelcast.jet.pulsar.impl.Utils;
import com.hazelcast.jet.pulsar.impl.WritePulsarP;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.jspecify.annotations.NonNull;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.shorten;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * See {@link PulsarSinks#builder(String, SupplierEx, SupplierEx, FunctionEx)}
 *
 * @param <E> the type of stream item
 * @param <M> the type of the message published by {@code PulsarProducer}
 * @since 6.0
 */
public final class PulsarSinkBuilder<E, M> {
    private static final int PREFERRED_LOCAL_PARALLELISM = 4;
    private final PulsarSinkConfig<E, M> config = new PulsarSinkConfig<>();

    public PulsarSinkBuilder(@Nonnull SupplierEx<Schema<M>> schemaSupplier, @Nonnull FunctionEx<? super E, M> extractValueFn) {
        checkNonNullAndSerializable(schemaSupplier, "schemaSupplier");
        checkNonNullAndSerializable(extractValueFn, "extractValueFn");
        config.setLocalParallelism(PREFERRED_LOCAL_PARALLELISM);
        config.setSchemaSupplier(schemaSupplier);
        config.setExtractValueFn(extractValueFn);
        config.setOptions(getDefaultProducerConfig());
    }

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
        this(schemaSupplier, extractValueFn);
        checkSerializable(connectionSupplier, "connectionSupplier");

        config.setTopic(topic);
        config.setConnectionSupplier(connectionSupplier);
        config.setOptions(getDefaultProducerConfig());
    }

    /**
     * Helper function to force type interference. {@link NEW_E} should be equal to {@link E}.
     */
    @Nonnull
    // parameter is used only to force new type
    @SuppressWarnings("unused")
    public <NEW_E> PulsarSinkBuilder<NEW_E, M> inputType(@Nonnull Class<NEW_E> type) {
        //noinspection unchecked
        return (PulsarSinkBuilder<NEW_E, M>) this;
    }

    @Nonnull
    public PulsarSinkBuilder<E, M> topic(@Nonnull String topic) {
        checkNotNull(topic, "topic");
        config.setTopic(topic);
        return this;
    }

    @Nonnull
    public PulsarSinkBuilder<E, M> connectionSupplier(@Nonnull SupplierEx<PulsarClient> connectionSupplier) {
        checkNonNullAndSerializable(connectionSupplier, "connectionSupplier");
        config.setConnectionSupplier(connectionSupplier);
        return this;
    }

    @Nonnull
    public PulsarSinkBuilder<E, M> httpServiceUrl(@Nonnull String httpServiceUrl) {
        checkNotNull(httpServiceUrl, "httpServiceUrl");
        config.setHttpServiceUrl(httpServiceUrl);
        return this;
    }

    @Nonnull
    public PulsarSinkBuilder<E, M> dataConnectionRef(@Nonnull DataConnectionRef dataConnectionRef) {
        checkNotNull(dataConnectionRef, "dataConnectionRef");
        config.setDataConnectionRef(dataConnectionRef);
        return this;
    }

    private static Map<String, Object> getDefaultProducerConfig() {
        return new HashMap<>();
    }

    /**
     * @param producerConfig The configurations for {@code PulsarProducer}
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> producerConfig(Map<String, Object> producerConfig) {
        checkSerializable(producerConfig, "producerConfig");
        config.setOptions(producerConfig);
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
        config.setExtractKeyFn(extractKeyFn);
        return this;
    }

    /**
     * @param extractValueFn extracts the message value from the emitted items.
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> extractValueFn(
            FunctionEx<? super E, M> extractValueFn
    ) {
        checkSerializable(extractValueFn, "extractValueFn");
        config.setExtractValueFn(extractValueFn);
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
        config.setExtractPropertiesFn(extractPropertiesFn);
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
        config.setExtractTimestampFn(extractTimestampFn);
        return this;
    }

    /**
     * @param transactionTimeout timeout for transactions, that will be used if there is processing guarantee
     *                           specified for the job
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> transactionTimeout(@Nonnull Duration transactionTimeout) {
        checkNotNull(transactionTimeout, "transactionTimeout");
        config.setTransactionTimeout(transactionTimeout);
        return this;
    }
    /**
     *
     * Enables or disables the exactly-once behavior of the sink using
     * two-phase commit of state snapshots. If enabled, the {@linkplain
     * JobConfig#setProcessingGuarantee(ProcessingGuarantee) processing
     * guarantee} of the job must be set to {@linkplain
     * ProcessingGuarantee#EXACTLY_ONCE exactly-once}, otherwise the sink
     * guarantee will match that of the job. In other words, sink
     * guarantee cannot be higher than job's, but can be lowered to avoid the
     * additional overhead.
     * <p>
     * The default value is true.
     * @param enable If true, sink guarantee will match the job
     *      guarantee. If false, sink guarantee will be at-least-once
     *      even if job's is exactly-once
     * @return this instance for fluent API
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> useTransactions(boolean enable) {
        config.setTransactional(enable);
        return this;
    }

    /**
     * Preferred local parallelism for every processor created by this builder.
     */
    public PulsarSinkBuilder<E, M> preferredLocalParallelism(int preferredLocalParallelism) {
        this.config.setLocalParallelism(Vertex.checkLocalParallelism(preferredLocalParallelism));
        return this;
    }

    /**
     * Customizer of the {@link ProducerBuilder} created by this sink.
     */
    @Nonnull
    public PulsarSinkBuilder<E, M> producerCustomizer(@Nonnull ConsumerEx<@NonNull ProducerBuilder<M>> producerCustomizer) {
        checkNonNullAndSerializable(producerCustomizer, "producerCustomizer");
        config.setProducerCustomizer(producerCustomizer);
        return this;
    }

    /**
     * Creates and returns the Pulsar {@link Sink} with using builder configurations set before.
     */
    @Nonnull
    public Sink<E> build() {
        config.validate();

        String sinkName = "pulsarSink(" + shorten(config.getTopic(), Utils.MAX_TOPICS_LENGTH) + ")";
        final PulsarSinkConfig<E, M> localConfig = new PulsarSinkConfig<>(config);
        return localConfig.hasKeyFn()
            ? Sinks.fromProcessor(sinkName, WritePulsarP.processorMetaSupplier(localConfig), localConfig.getExtractKeyFn())
            : Sinks.fromProcessor(sinkName, WritePulsarP.processorMetaSupplier(localConfig));
    }

}
