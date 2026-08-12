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

import com.hazelcast.jet.pipeline.Sink;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for Pulsar sinks.
 *
 * @since 6.0
 */
public final class PulsarSinks {

    private PulsarSinks() {
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom Pulsar {@link Sink} for the Pipeline API.
     *
     * @param topic              Pulsar topic name to publish to
     * @param connectionSupplier Pulsar client supplier
     * @param extractValueFn     extracts the message value from the emitted items.
     * @param schemaSupplier     Pulsar messaging schema supplier.
     * @param <E>                the type of stream items that sink accepts
     * @param <M>                the type of the message published by {@code PulsarProducer}
     *
     * @since 6.0
     */
    @Nonnull
    public static <E, M> PulsarSinkBuilder<E, M> builder(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<? super E, M> extractValueFn

    ) {
        return new PulsarSinkBuilder<>(topic, connectionSupplier, schemaSupplier, extractValueFn);
    }

    /**
     * Convenience for {@link #builder(String, SupplierEx, SupplierEx, FunctionEx)}.
     * It creates a basic Pulsar sink that connect the topic.
     *
     * @param topic              Pulsar topic name to publish to
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     extracts the message value from the emitted items.
     * @param extractValueFn     Pulsar messaging schema supplier.
     * @param <E>                the type of stream items that sink accepts
     * @param <M>                the type of the message published by {@code PulsarProducer}
     *
     * @since 6.0
     */
    @Nonnull
    public static <E, M> Sink<E> pulsarSink(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<? super E, M> extractValueFn
    ) {
        return PulsarSinks.<E, M>builder(topic, connectionSupplier, schemaSupplier, extractValueFn).build();
    }

}
