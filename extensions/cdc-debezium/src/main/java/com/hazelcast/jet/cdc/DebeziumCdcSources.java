/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.impl.CdcSourceP;
import com.hazelcast.jet.cdc.impl.ChangeRecordCdcSourceP;
import com.hazelcast.jet.cdc.impl.ConstantSequenceExtractor;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.JsonCdcSourceP;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Contains factory methods for creating Change Data Capture (CDC) sources.
 * <p>
 * Note: It is better to use first-class CDC sources than this generic one
 * because it has less functionality. For example, these sources lack
 * sequence numbers, so functionality based on them (like reordering
 * protection in {@link CdcSinks}) is disabled.
 *
 * @since Jet 4.2
 */
@EvolvingApi
public final class DebeziumCdcSources {

    private DebeziumCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a
     * Debezium-supported database to a Hazelcast Jet pipeline.
     *
     * @param name the name of this source, must unique, will be passed
     *             to the underlying Kafka Connect source
     * @return a builder you can use to set the source's properties and
     * then construct it
     */
    @Nonnull
    public static Builder<ChangeRecord> debezium(@Nonnull String name, @Nonnull String connectorClass) {
        return new Builder<>(name, connectorClass,
                (properties, eventTimePolicy) -> new ChangeRecordCdcSourceP(properties, eventTimePolicy));
    }

    /**
     * Creates a CDC source that streams change data from a
     * Debezium-supported database to a Hazelcast Jet pipeline.
     * <p>
     * Differs from the {@link #debezium(String, String) regular source}
     * in that it does the least possible amount of processing of the
     * raw Debezium data. Just returns a pair of JSON strings, one is
     * the key of the Debezium CDC event, the other the value.
     *
     * @return a builder you can use to set the source's properties and then construct it
     */
    @Nonnull
    public static Builder<Entry<String, String>> debeziumJson(@Nonnull String name, @Nonnull String connectorClass) {
        return new Builder<>(name, connectorClass,
                (properties, eventTimePolicy) -> new JsonCdcSourceP(properties, eventTimePolicy));
    }

    /**
     * A builder to configure a CDC source that streams the change data from
     * a Debezium-supported database to Hazelcast Jet.
     *
     * @param <T> type of items handled by the source
     */
    public static final class Builder<T> {

        private final BiFunctionEx<Properties, EventTimePolicy<? super T>, CdcSourceP<T>> processorFn;
        private final DebeziumConfig config;

        private Builder(
                @Nonnull String name,
                @Nonnull String connectorClass,
                @Nonnull BiFunctionEx<Properties, EventTimePolicy<? super T>, CdcSourceP<T>> processorFn
        ) {
            config = new DebeziumConfig(name, connectorClass);
            config.setProperty(CdcSourceP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, ConstantSequenceExtractor.class.getName());

            this.processorFn = processorFn;
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         */
        @Nonnull
        public Builder<T> setProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Returns the CDC source based on the properties set.
         */
        @Nonnull
        public StreamSource<T> build() {
            Properties properties = config.toProperties();
            BiFunctionEx<Properties, EventTimePolicy<? super T>, CdcSourceP<T>> processorFn =
                    this.processorFn;

            return Sources.streamFromProcessorWithWatermarks(
                    properties.getProperty(CdcSourceP.NAME_PROPERTY),
                    true,
                    eventTimePolicy -> {
                        ProcessorSupplier supplier = ProcessorSupplier.of(
                                () -> processorFn.apply(properties, eventTimePolicy));
                        return ProcessorMetaSupplier.forceTotalParallelismOne(supplier);
                    });
        }
    }

}
