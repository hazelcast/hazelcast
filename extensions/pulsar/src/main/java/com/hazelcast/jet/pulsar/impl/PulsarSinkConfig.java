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
package com.hazelcast.jet.pulsar.impl;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Object encapsulating common Pulsar connector properties.
 * @param <E> the type of object produced by the connector ({@code M} before applying projectionFn).
 * @param <M> the type of messages send to Pulsar.
 */
public class PulsarSinkConfig<E, M> extends PulsarConfig<M> implements DataSerializable, Serializable {
    @SuppressWarnings("rawtypes")
    private static final FunctionEx RETURN_NULL = anything -> null;
    private static final Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.ofMinutes(1);

    @Serial
    private static final long serialVersionUID = 1L;
    private String topic;
    private FunctionEx<? super E, M> extractValueFn;
    private FunctionEx<? super E, String> extractKeyFn;
    private FunctionEx<? super E, Map<String, String>> extractPropertiesFn;
    private FunctionEx<? super E, Long> extractTimestampFn;
    private Duration transactionTimeout;
    private ConsumerEx<ProducerBuilder<M>> producerCustomizer = ConsumerEx.noop();
    private boolean transactional = true;

    // needed for serialization
    @SuppressWarnings("unused")
    public PulsarSinkConfig() {
        super();
    }

    public PulsarSinkConfig(PulsarSinkConfig<E, M> other) {
        extractKeyFn = other.extractKeyFn;
        extractPropertiesFn = other.extractPropertiesFn;
        extractTimestampFn = other.extractTimestampFn;
        extractValueFn = other.extractValueFn;
        transactionTimeout = other.transactionTimeout;
        producerCustomizer = other.producerCustomizer;
        transactional = other.transactional;
        setConnectionSupplier(other.getConnectionSupplier());
        setDataConnectionRef(other.getDataConnectionRef());
        setOptions(other.getOptions());
        setSchemaSupplier(other.getSchemaSupplier());
        setLocalParallelism(other.getLocalParallelism());
        topic = other.topic;
        setHttpServiceUrl(other.getHttpServiceUrl());
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public FunctionEx<? super E, M> getExtractValueFn() {
        return extractValueFn;
    }

    public void setExtractValueFn(FunctionEx<? super E, M> extractValueFn) {
        this.extractValueFn = extractValueFn;
    }

    public FunctionEx<? super E, String> getExtractKeyFn() {
        return extractKeyFn == null ? returnNullFn() : extractKeyFn;
    }

    public void setExtractKeyFn(FunctionEx<? super E, String> extractKeyFn) {
        this.extractKeyFn = extractKeyFn;
    }

    public FunctionEx<? super E, Map<String, String>> getExtractPropertiesFn() {
        return extractPropertiesFn == null ? anything -> emptyMap() : extractPropertiesFn;
    }

    public boolean hasKeyFn() {
        return extractKeyFn != null;
    }

    public void setExtractPropertiesFn(FunctionEx<? super E, Map<String, String>> extractPropertiesFn) {
        this.extractPropertiesFn = extractPropertiesFn;
    }

    public FunctionEx<? super E, Long> getExtractTimestampFn() {
        return extractTimestampFn == null ? returnNullFn() : extractTimestampFn;
    }

    public void setExtractTimestampFn(FunctionEx<? super E, Long> extractTimestampFn) {
        this.extractTimestampFn = extractTimestampFn;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    private <I, O> FunctionEx<I, O> returnNullFn() {
        //noinspection unchecked
        return RETURN_NULL;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeString(topic);
        out.writeObject(extractValueFn);
        out.writeObject(extractKeyFn);
        out.writeObject(extractPropertiesFn);
        out.writeObject(extractTimestampFn);
        out.writeBoolean(transactionTimeout != null);
        if (transactionTimeout != null) {
            out.writeLong(transactionTimeout.getSeconds());
            out.writeInt(transactionTimeout.getNano());
        }
        out.writeObject(producerCustomizer);
        out.writeBoolean(transactional);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        topic = in.readString();
        extractValueFn = in.readObject();
        extractKeyFn = in.readObject();
        extractPropertiesFn = in.readObject();
        extractTimestampFn = in.readObject();
        if (in.readBoolean()) {
            long sec = in.readLong();
            int nano = in.readInt();
            transactionTimeout = Duration.ofSeconds(sec, nano);
        }
        producerCustomizer = in.readObject();
        transactional = in.readBoolean();
    }

    public Duration getTransactionTimeout() {
        return transactionTimeout == null ? DEFAULT_TRANSACTION_TIMEOUT : transactionTimeout;
    }

    public void setTransactionTimeout(Duration transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }

    public void validate() {
        var validation = commonValidation();

        validation
            .checkNotNull(topic, "topic")
            .checkSerializableIfNotNull(extractKeyFn, "extractKeyFn")
            .checkSerializableIfNotNull(extractPropertiesFn, "extractPropertiesFn")
            .checkSerializableIfNotNull(extractTimestampFn, "extractTimestampFn")
            .checkNotNullAndSerializable(extractValueFn, "extractValueFn")
            .throwIfErrors();
    }

    public void setProducerCustomizer(ConsumerEx<@NonNull ProducerBuilder<M>> producerCustomizer) {
        this.producerCustomizer = producerCustomizer;
    }

    @NonNull
    public ConsumerEx<ProducerBuilder<M>> getProducerCustomizer() {
        return producerCustomizer == null ? ConsumerEx.noop() : producerCustomizer;
    }
}
