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
package com.hazelcast.jet.cdc;

import com.hazelcast.jet.cdc.impl.ReadCdcP;
import com.hazelcast.jet.cdc.impl.ChangeRecordMappingFn;
import com.hazelcast.jet.cdc.impl.ConstantSequenceExtractor;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.JsonRecordMappingFn;
import com.hazelcast.jet.cdc.impl.WriteCdcP;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkState;

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
@SuppressWarnings("unchecked")
public final class DebeziumCdcSources {

    private DebeziumCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a
     * Debezium-supported database to a Hazelcast Jet pipeline.
     *
     * @param name the name of this source, must be unique.
     * @param connectorClass name of the Debezium connector class
     * @return a builder you can use to set the source's properties and
     * then construct it
     */
    @Nonnull
    public static Builder<ChangeRecord> debezium(@Nonnull String name, @Nonnull String connectorClass) {
        return new Builder<>(name, connectorClass, new ChangeRecordMappingFn());
    }

    /**
     * Creates a CDC source that streams change data from a
     * Debezium-supported database to a Hazelcast Jet pipeline.
     *
     * @param name the name of this source, must unique, will be passed
     *             to the underlying Kafka Connect source
     * @param connectorClass class of the Debezium Connector which will be used for the CDC
     * @return a builder you can use to set the source's properties and
     * then construct it
     */
    @Nonnull
    public static Builder<ChangeRecord> debezium(
            @Nonnull String name,
            @Nonnull Class<?> connectorClass) {
        checkState(SourceConnector.class.isAssignableFrom(connectorClass), "connector class must be a subclass"
                + " of SourceConnector");
        return new Builder<>(name, connectorClass.getName(), new ChangeRecordMappingFn());
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
     * @param name the name of this source, must unique, will be passed
     *             to the underlying Kafka Connect source
     * @param connectorClass name of the Debezium connector class
     * @return a builder you can use to set the source's properties and then construct it
     */
    @Nonnull
    public static Builder<Entry<String, String>> debeziumJson(@Nonnull String name, @Nonnull String connectorClass) {
        return new Builder<>(name, connectorClass, new JsonRecordMappingFn());
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
     * @param name the name of this source, must unique, will be passed
     *             to the underlying Kafka Connect source
     * @param connectorClass class of the Debezium Connector which will be used for the CDC
     * @return a builder you can use to set the source's properties and then construct it
     */
    @Nonnull
    public static Builder<Entry<String, String>> debeziumJson(
            @Nonnull String name,
            @Nonnull Class<?> connectorClass) {
        checkState(SourceConnector.class.isAssignableFrom(connectorClass), "connector class must be a subclass"
                + " of SourceConnector");
        return new Builder<>(name, connectorClass.getName(), new JsonRecordMappingFn());
    }

    /**
     * A builder to configure a CDC source that streams the change data from
     * a Debezium-supported database to Hazelcast Jet.
     *
     * @param <T> type of items handled by the source
     */
    public static class Builder<T> {

        protected final DebeziumConfig config;
        protected RecordMappingFunction<T> recordMappingFunction;

        protected Builder(
                @Nonnull String name,
                @Nonnull String connectorClass,
                @Nonnull RecordMappingFunction<T> recordMappingFunction
        ) {
            config = new DebeziumConfig(name, connectorClass);
            config.setProperty(ReadCdcP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, ConstantSequenceExtractor.class.getName());
            // Kafka now validates if this property is set; it is ignored later, but must be set nevertheless.
            config.setProperty("bootstrap.servers", "ignored");

            this.recordMappingFunction = recordMappingFunction;
        }

        /**
         * Sets the return type of the source to {@link ChangeRecord}.
         */
        @Nonnull
        public Builder<ChangeRecord> changeRecord() {
            this.recordMappingFunction = (RecordMappingFunction<T>) new ChangeRecordMappingFn();
            return (Builder<ChangeRecord>) this;
        }

        /**
         * Sets the return type of the source to {@link Map.Entry} with key and value being {@link SourceRecord}'s
         * key and value, parsed to json string.
         */
        @Nonnull
        public Builder<Map.Entry<String, String>> json() {
            this.recordMappingFunction = (RecordMappingFunction<T>) new JsonRecordMappingFn();
            return (Builder<Map.Entry<String, String>>) this;
        }

        /**
         * Sets the return type of the source to user defined {@code #T_NEW} type. Mapping will be performed
         * by user-defined mapping function.
         */
        @Nonnull
        public <T_NEW> Builder<T_NEW> customMapping(@Nonnull RecordMappingFunction<T_NEW> recordMappingFunction) {
            Util.checkNonNullAndSerializable(recordMappingFunction, "recordMappingFunction");
            this.recordMappingFunction = (RecordMappingFunction<T>) recordMappingFunction;
            return (Builder<T_NEW>) this;
        }

        /**
         * Sets the {@link SequenceExtractor} class property.
         * <p>
         * Sequence extractor is used to determine monotonically increasing order of CDC order, that
         * can be used later in {@link WriteCdcP}.
         * @param sequenceExtractorClass Class of the SequenceExtractor implementation. Must be on the
         *                               classpath during execution.
         */
        @Nonnull
        public Builder<T> withSequenceExtractor(Class<? extends SequenceExtractor> sequenceExtractorClass) {
            config.setProperty(ReadCdcP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, sequenceExtractorClass.getName());
            return this;
        }

        /**
         * Sets the maximum retry count in case of errors.
         * <p>Default value is -1, which means infinite retries.
         */
        @Nonnull
        public Builder<T> withErrorMaxRetries(int errorRetryCount) {
            config.setProperty("errors.max.retries", String.valueOf(errorRetryCount));
            return this;
        }

        //region Includes/Excludes
        /**
         * Optional regular expressions that match databases to be monitored; any database not included in the
         * include list will be excluded from monitoring. By default, the connector will
         * monitor all databases. May not be
         * used with {@link #setDatabaseExcludeList(String...) database exclude list}.
         */
        @Nonnull
        public Builder<T> setDatabaseIncludeList(@Nonnull String... databaseNameRegExps) {
            config.setProperty("database.include.list", String.join(",", databaseNameRegExps));
            return this;
        }

        /**
         * Optional regular expressions that match databases to be excluded from monitoring; any table not
         * included in the exclude list will be monitored. May not be used with
         * {@link #setDatabaseIncludeList(String...) database include list}.
         */
        @Nonnull
        public Builder<T> setDatabaseExcludeList(@Nonnull String... databaseNameRegExps) {
            config.setProperty("database.exclude.list", databaseNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not included in the
         * whitelist will be excluded from monitoring. Each identifier is of the
         * form <em>schemaName.tableName</em>. By default, the connector will
         * monitor every non-system table in each monitored database. May not be
         * used with {@link #setTableExcludeList(String...) table exclude list}.
         */
        @Nonnull
        public Builder<T> setTableIncludeList(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.include.list", String.join(",", tableNameRegExps));
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any table not
         * included in the exclude list will be monitored. Each identifier is of
         * the form <em>schemaName.tableName</em>. May not be used with
         * {@link #setTableIncludeList(String...) table include list}.
         */
        @Nonnull
        public Builder<T> setTableExcludeList(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.exclude.list", tableNameRegExps);
            return this;
        }
        //endregion

        /**
         * Sets a source property. These properties are passed to Debezium.
         */
        @Nonnull
        public Builder<T> setProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         * @since 6.0
         */
        @Nonnull
        public Builder<T>  setProperty(@Nonnull String key, int value) {
            setProperty(key, Integer.toString(value));
            return this;
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         * @since 6.0
         */
        @Nonnull
        public Builder<T>  setProperty(@Nonnull String key, long value) {
            setProperty(key, Long.toString(value));
            return this;
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         *
         * @since 6.0
         */
        @Nonnull
        public Builder<T>  setProperty(@Nonnull String key, boolean value) {
            setProperty(key, Boolean.toString(value));
            return this;
        }

        /**
         * Returns the CDC source based on the properties set.
         */
        @Nonnull
        public StreamSource<T> build() {
            final Properties properties = config.toProperties();
            final RecordMappingFunction<T> recordMappingFunction = this.recordMappingFunction;

            final String name = properties.getProperty(ReadCdcP.NAME_PROPERTY);
            return Sources.streamFromProcessorWithWatermarks(
                    name,
                    true,
                    eventTimePolicy -> {
                        ProcessorSupplier supplier = ProcessorSupplier.of(
                                () -> new ReadCdcP<>(properties, eventTimePolicy, recordMappingFunction));
                        return ProcessorMetaSupplier.forceTotalParallelismOne(supplier);
                    });
        }
    }

}
