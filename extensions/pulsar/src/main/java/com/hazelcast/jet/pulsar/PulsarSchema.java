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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.impl.util.Util;
import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Helper class that holds context info about Pulsar's Schema, without using Pulsar API directly.
 *
 * @since 6.0
 */
public class PulsarSchema<E> {

    private final SupplierEx<Schema<E>> schemaV4Supplier;

    private PulsarSchema(SupplierEx<Schema<E>> schemaV4Supplier) {
        Util.checkNonNullAndSerializable(schemaV4Supplier, "schemaV4Supplier");
        this.schemaV4Supplier = schemaV4Supplier;
    }

    // intentionally raw type to avoid casting problems in builders
    @SuppressWarnings("rawtypes")
    @Nonnull
    SupplierEx supplierV4() {
        return schemaV4Supplier;
    }

    /**
     * Returns the equivalent of {@link Schema#BYTES}.
     */
    @Nonnull
    public static PulsarSchema<byte[]> bytes() {
        return new PulsarSchema<>(() -> Schema.BYTES);
    }

    /**
     * Returns the equivalent of {@link Schema#STRING}.
     */
    @Nonnull
    public static PulsarSchema<String> string() {
        return new PulsarSchema<>(() -> Schema.STRING);
    }

    /**
     * Returns the equivalent of {@link Schema#DOUBLE}.
     */
    @Nonnull
    public static PulsarSchema<Double> doubles() {
        return new PulsarSchema<>(() -> Schema.DOUBLE);
    }

    /**
     * Returns the equivalent of {@link Schema#FLOAT}.
     */
    @Nonnull
    public static PulsarSchema<Float> floats() {
        return new PulsarSchema<>(() -> Schema.FLOAT);
    }

    /**
     * Returns the equivalent of {@link Schema#INT8}.
     */
    @Nonnull
    public static PulsarSchema<Byte> int8() {
        return new PulsarSchema<>(() -> Schema.INT8);
    }

    /**
     * Returns the equivalent of {@link Schema#INT16}.
     */
    @Nonnull
    public static PulsarSchema<Short> int16() {
        return new PulsarSchema<>(() -> Schema.INT16);
    }

    /**
     * Returns the equivalent of {@link Schema#INT32}.
     */
    @Nonnull
    public static PulsarSchema<Integer> int32() {
        return new PulsarSchema<>(() -> Schema.INT32);
    }

    /**
     * Returns the equivalent of {@link Schema#INT64}.
     */
    @Nonnull
    public static PulsarSchema<Long> int64() {
        return new PulsarSchema<>(() -> Schema.INT64);
    }

    /**
     * Returns the equivalent of {@link Schema#BYTEBUFFER}.
     */
    @Nonnull
    public static PulsarSchema<ByteBuffer> byteBuffer() {
        return new PulsarSchema<>(() -> Schema.BYTEBUFFER);
    }

    /**
     * Returns the equivalent of {@link Schema#JSON(Class)}.
     */
    @Nonnull
    public static <T> PulsarSchema<T> json(@Nonnull Class<T> clazz) {
        checkNotNull(clazz, "class");
        return new PulsarSchema<>(() -> Schema.JSON(clazz));
    }
}
