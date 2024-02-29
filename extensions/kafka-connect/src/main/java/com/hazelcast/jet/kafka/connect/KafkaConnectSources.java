/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.kafka.connect.impl.SourceConnectorWrapper;
import com.hazelcast.jet.kafka.connect.impl.processorsupplier.TaskMaxProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.retry.RetryStrategy;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;
import static com.hazelcast.jet.kafka.connect.impl.ReadKafkaConnectP.processorSupplier;
import static com.hazelcast.jet.kafka.connect.impl.SourceConnectorWrapper.DEFAULT_RECONNECT_BEHAVIOR;
import static java.util.Objects.requireNonNull;

/**
 * Contains factory methods to create a Kafka Connect source.
 */
public final class KafkaConnectSources {

    private KafkaConnectSources() {
    }

    /**
     * A generic Kafka Connect source provides ability to plug any Kafka
     * Connect source for data ingestion to Jet pipelines.
     * <p>
     * You need to add the Kafka Connect connector JARs or a ZIP file
     * contains the JARs as a job resource via {@link com.hazelcast.jet.config.JobConfig#addJar(URL)}
     * or {@link com.hazelcast.jet.config.JobConfig#addJarsInZip(URL)}
     * respectively.
     * <p>
     * After that you can use the Kafka Connect connector with the
     * configuration parameters as you'd use it with Kafka. Hazelcast
     * Jet will drive the Kafka Connect connector from the pipeline and
     * the records will be available to your pipeline as a stream of
     * the custom type objects created by projectionFn.
     * <p>
     * In case of a failure; this source keeps track of the source
     * partition offsets, it will restore the partition offsets and
     * resume the consumption from where it left off.
     * <p>
     * Hazelcast Jet will instantiate tasks on a random cluster member and use local parallelism for scaling.
     * Property <code>tasks.max</code> is not allowed. Use {@link StreamStage#setLocalParallelism(int)} in the pipeline
     * instead. This limitation can be changed in the future.
     *
     * @param properties   Kafka connect properties
     * @param projectionFn function to create output objects from the Kafka {@link SourceRecord}s.
     *                     If the projection returns a {@code null} for an item,
     *                     that item will be filtered out.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     * @since 5.3
     */
    @Nonnull
    public static <T> StreamSource<T> connect(@Nonnull Properties properties,
                                              @Nonnull FunctionEx<SourceRecord, T> projectionFn) {
        return connect(properties, projectionFn, DEFAULT_RECONNECT_BEHAVIOR);
    }

    /**
     * A generic Kafka Connect source provides ability to plug any Kafka
     * Connect source for data ingestion to Jet pipelines.
     * <p>
     * You need to add the Kafka Connect connector JARs or a ZIP file
     * contains the JARs as a job resource via {@link com.hazelcast.jet.config.JobConfig#addJar(URL)}
     * or {@link com.hazelcast.jet.config.JobConfig#addJarsInZip(URL)}
     * respectively.
     * <p>
     * After that you can use the Kafka Connect connector with the
     * configuration parameters as you'd use it with Kafka. Hazelcast
     * Jet will drive the Kafka Connect connector from the pipeline and
     * the records will be available to your pipeline as a stream of
     * the custom type objects created by projectionFn.
     * <p>
     * In case of a failure; this source keeps track of the source
     * partition offsets, it will restore the partition offsets and
     * resume the consumption from where it left off.
     * <p>
     * Hazelcast Jet will instantiate tasks on a random cluster member and use local parallelism for scaling.
     * Property <code>tasks.max</code> is not allowed. Use {@link StreamStage#setLocalParallelism(int)} in the pipeline
     * instead. This limitation can be changed in the future.
     *
     * @param properties   Kafka connect properties
     * @param projectionFn function to create output objects from the Kafka {@link SourceRecord}s.
     *                     If the projection returns a {@code null} for an item,
     *                     that item will be filtered out.
     * @param retryStrategy Strategy that will be used to perform reconnection retries after the connection is lost.
     *                      You may want to use {@link com.hazelcast.jet.retry.RetryStrategies} to provide custom strategy.
     *                      By default, it's {@link SourceConnectorWrapper#DEFAULT_RECONNECT_BEHAVIOR}.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     * @since 5.4
     */
    @Nonnull
    public static <T> StreamSource<T> connect(@Nonnull Properties properties,
                                              @Nonnull FunctionEx<SourceRecord, T> projectionFn,
                                              @Nullable RetryStrategy retryStrategy) {
        requireNonNull(properties, "properties is required");
        requireNonNull(projectionFn, "projectionFn is required");
        if (retryStrategy != null) {
            checkSerializable(retryStrategy, "retryStrategy");
        }

        //fail fast, required by lazy-initialized KafkaConnectSource
        checkRequiredProperty(properties, "name");
        checkRequiredProperty(properties, "connector.class");

        // Populate by default values
        Properties defaultProperties = getDefaultProperties(properties);

        // Check tasks.max is positive
        String strTasksMax = defaultProperties.getProperty("tasks.max");
        int tasksMax = Integer.parseInt(strTasksMax);
        checkPositive(tasksMax, "tasks.max must be positive");

        final var metaSupplier = new TaskMaxProcessorMetaSupplier();
        metaSupplier.setTasksMax(tasksMax);

        // Create source name
        String name = "kafkaConnectSource(" + defaultProperties.getProperty("name") + ")";

        return Sources.streamFromProcessorWithWatermarks(name, true,
                eventTimePolicy -> {
                    var sup = processorSupplier(defaultProperties, eventTimePolicy, projectionFn, retryStrategy);
                    metaSupplier.setSupplier(sup);
                    return metaSupplier;
                });
    }

    /**
     * A generic Kafka Connect source provides ability to plug any Kafka
     * Connect source for data ingestion to Jet pipelines.
     * <p>
     * You need to add the Kafka Connect connector JARs or a ZIP file
     * contains the JARs as a job resource via {@link com.hazelcast.jet.config.JobConfig#addJar(URL)}
     * or {@link com.hazelcast.jet.config.JobConfig#addJarsInZip(URL)}
     * respectively.
     * <p>
     * After that you can use the Kafka Connect connector with the
     * configuration parameters as you'd use it with Kafka. Hazelcast
     * Jet will drive the Kafka Connect connector from the pipeline and
     * the records will be available to your pipeline as {@link SourceRecord}s.
     * <p>
     * In case of a failure; this source keeps track of the source
     * partition offsets, it will restore the partition offsets and
     * resume the consumption from where it left off.
     * <p>
     * Hazelcast Jet will instantiate tasks on a random cluster member and use local parallelism for scaling.
     * Property <code>tasks.max</code> is not allowed. Use {@link StreamStage#setLocalParallelism(int)} in the pipeline
     * instead.
     *
     * @param properties Kafka connect properties
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     * @since 5.3
     */
    @Nonnull
    public static StreamSource<SourceRecord> connect(@Nonnull Properties properties) {
        return connect(properties, FunctionEx.identity());
    }

    private static Properties getDefaultProperties(Properties properties) {
        // Make new copy
        Properties defaultProperties = new Properties();
        defaultProperties.putAll(properties);

        // Populate tasks.max property if necessary
        defaultProperties.putIfAbsent("tasks.max", "1");

        return defaultProperties;
    }
}
