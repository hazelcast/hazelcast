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
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pulsar.impl.PulsarSourceConfig;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.jspecify.annotations.NonNull;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.shorten;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pulsar.impl.StreamPulsarViaReaderP.processorMetaSupplier;
import static com.hazelcast.jet.pulsar.impl.Utils.MAX_TOPICS_LENGTH;
import static java.util.Objects.requireNonNull;

/**
 * See {@link PulsarSources#pulsarReaderBuilder(String, SupplierEx, SupplierEx, FunctionEx)}
 *
 * @param <M> the type of the message read by {@code pulsarReader}
 * @param <T> the type of data emitted from {@code StreamSource}
 * @since 6.0
 */
public final class PulsarReaderBuilder<M, T> {

    private FunctionEx<Message<M>, T> projectionFn;
    private final PulsarSourceConfig<M, T> conf = new PulsarSourceConfig<>();

    public PulsarReaderBuilder(SupplierEx<Schema<M>> schemaSupplier) {
        checkNonNullAndSerializable(schemaSupplier, "schemaSupplier");
        conf.setSchemaSupplier(schemaSupplier);
        conf.setOptions(Map.of());
    }

    /**
     * Required fields of Pulsar reader
     *
     * @param topic              Pulsar topic name to consume from
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     Pulsar messaging schema supplier.
     * @param projectionFn       converts a Pulsar message to an emitted item.
     */
    public PulsarReaderBuilder(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn) {
        this(schemaSupplier);
        checkSerializable(connectionSupplier, "connectionSupplier");
        checkSerializable(projectionFn, "projectionFn");

        conf.setTopics(List.of(topic));
        conf.setConnectionSupplier(connectionSupplier);
        conf.setSchemaSupplier(schemaSupplier);
        this.projectionFn = projectionFn;
    }

    @Nonnull
    public PulsarReaderBuilder<M, T> topic(@Nonnull String... topics) {
        checkNotNull(topics, "topics");
        conf.setTopics(List.of(topics));
        return this;
    }

    @Nonnull
    public PulsarReaderBuilder<M, T> connectionSupplier(@Nonnull SupplierEx<PulsarClient> connectionSupplier) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        conf.setConnectionSupplier(connectionSupplier);
        return this;
    }

    @Nonnull
    public PulsarReaderBuilder<M, T> dataConnectionRef(@Nonnull DataConnectionRef dataConnectionRef) {
        requireNonNull(dataConnectionRef, "dataConnectionRef");
        conf.setDataConnectionRef(dataConnectionRef);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <NEW_T> PulsarReaderBuilder<M, NEW_T> projectionFn(@Nonnull FunctionEx<Message<M>, NEW_T> projectionFn) {
        checkNonNullAndSerializable(projectionFn, "projectionFn");
        this.projectionFn = (FunctionEx<Message<M>, T>) projectionFn;
        return (PulsarReaderBuilder<M, NEW_T>) this;
    }

    /**
     * Allows additional customization and invocation of other {@link ReaderBuilder} method,
     * that doesn't have a representative here.
     */
    @Nonnull
    public PulsarReaderBuilder<M, T> readerCustomizer(@Nonnull ConsumerEx<@NonNull ReaderBuilder<M>> readerCustomizer) {
        checkNonNullAndSerializable(readerCustomizer, "readerCustomizer");
        conf.setReaderCustomizer(readerCustomizer);
        return this;
    }

    /**
     * @param readerConfig Pulsar reader configurations that must contain reader name
     */
    @Nonnull
    public PulsarReaderBuilder<M, T> readerConfig(@Nonnull Map<String, Object> readerConfig) {
        checkSerializable(readerConfig, "readerConfig");
        conf.setOptions(readerConfig);
        return this;
    }

    /**
     * Preferred local parallelism for every processor created by this builder.
     */
    public PulsarReaderBuilder<M, T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.conf.setLocalParallelism(Vertex.checkLocalParallelism(preferredLocalParallelism));
        return this;
    }
    /**
     * @param batchReceivePolicy batch receive policy for the Reader
     */
    @Nonnull
    public PulsarReaderBuilder<M, T> batchReceivePolicy(
            @Nonnull BatchReceivePolicy batchReceivePolicy
    ) {
        checkSerializable(batchReceivePolicy, "batchReceivePolicy");
        conf.setBatchReceivePolicy(batchReceivePolicy);
        return this;
    }

    /**
     * Creates and returns the Pulsar Reader {@link StreamSource} with using builder configurations set before.
     */
    @Nonnull
    public StreamSource<T> build() {
        conf.setProjectionFn(projectionFn);
        conf.validate();
        final PulsarSourceConfig<M, T> localSourceConf = new PulsarSourceConfig<>(conf);
        String sourceName = "pulsarReader(" + shorten(String.join(",", localSourceConf.getTopics()), MAX_TOPICS_LENGTH) + ")";
        return Sources.streamFromProcessorWithWatermarks(sourceName, true,
                                                         eventTimePolicy ->
                                                             processorMetaSupplier(localSourceConf, eventTimePolicy));
    }
}
