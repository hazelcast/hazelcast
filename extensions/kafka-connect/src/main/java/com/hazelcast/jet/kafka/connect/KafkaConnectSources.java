/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.kafka.connect.impl.ConnectorWrapper;
import com.hazelcast.jet.kafka.connect.impl.ReadKafkaConnectP;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.spi.annotation.Beta;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URL;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;

/**
 * Contains factory methods to create a Kafka Connect source.
 */
@Beta
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
     * configuration parameters as you'd using it with Kafka. Hazelcast
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
     */
    @Nonnull
    @Beta
    public static StreamSource<SourceRecord> connect(@Nonnull Properties properties) {
        Preconditions.checkRequiredProperty(properties, "name");
        String name = properties.getProperty("name");

        //fail fast, required by lazy-initialized KafkaConnectSource
        checkRequiredProperty(properties, "connector.class");

        if (properties.containsKey("tasks.max")) {
            throw new IllegalArgumentException("Property 'tasks.max' not allowed. Use setLocalParallelism("
                    + properties.getProperty("tasks.max") + ") in the pipeline instead");
        }

        return Sources.streamFromProcessorWithWatermarks(name, true,
                eventTimePolicy -> ProcessorMetaSupplier.randomMember(
                        new ProcessorSupplier() {
                            private transient ConnectorWrapper connectorWrapper;

                            @Override
                            public void init(@Nonnull Context context) {
                                properties.put("tasks.max", Integer.toString(context.localParallelism()));
                                this.connectorWrapper = new ConnectorWrapper(properties);
                            }

                            @Override
                            public void close(@Nullable Throwable error) {
                                if (connectorWrapper != null) {
                                    connectorWrapper.stop();
                                }
                            }

                            @Nonnull
                            @Override
                            public Collection<? extends Processor> get(int count) {
                                return IntStream.range(0, count)
                                        .mapToObj(i -> new ReadKafkaConnectP(connectorWrapper, eventTimePolicy))
                                        .collect(Collectors.toList());
                            }

                        })
        );
    }

}
