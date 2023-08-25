/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.serialization.compact.CompactReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Adapter to make CompactInternalGenericRecord provide `CompactReader` API.
 * <p>
 * See the javadoc of {@link InternalGenericRecord} for GenericRecord class hierarchy.
 */
public class DefaultCompactReader extends CompactInternalGenericRecord implements CompactReader {

    public DefaultCompactReader(CompactStreamSerializer serializer, BufferObjectDataInput in, Schema schema,
                                @Nullable Class associatedClass, boolean schemaIncludedInBinary) {
        super(serializer, in, schema, associatedClass, schemaIncludedInBinary);
    }

    @Nonnull
    protected String getMethodPrefixForErrorMessages() {
        return "read";
    }

    @Override
    public boolean readBoolean(@Nonnull String fieldName) {
        return getBoolean(fieldName);
    }


    @Override
    public byte readInt8(@Nonnull String fieldName) {
        return getInt8(fieldName);
    }

    @Override
    public short readInt16(@Nonnull String fieldName) {
        return getInt16(fieldName);
    }

    @Override
    public int readInt32(@Nonnull String fieldName) {
        return getInt32(fieldName);
    }

    @Override
    public long readInt64(@Nonnull String fieldName) {
        return getInt64(fieldName);
    }

    @Override
    public float readFloat32(@Nonnull String fieldName) {
        return getFloat32(fieldName);
    }

    @Override
    public double readFloat64(@Nonnull String fieldName) {
        return getFloat64(fieldName);
    }

    @Override
    public String readString(@Nonnull String fieldName) {
        return getString(fieldName);
    }

    @Override
    public BigDecimal readDecimal(@Nonnull String fieldName) {
        return getDecimal(fieldName);
    }

    @Override
    @Nullable
    public LocalTime readTime(@Nonnull String fieldName) {
        return getTime(fieldName);
    }

    @Override
    @Nullable
    public LocalDate readDate(@Nonnull String fieldName) {
        return getDate(fieldName);
    }

    @Override
    @Nullable
    public LocalDateTime readTimestamp(@Nonnull String fieldName) {
        return getTimestamp(fieldName);
    }

    @Override
    @Nullable
    public OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName) {
        return getTimestampWithTimezone(fieldName);
    }

    @Override
    public <T> T readCompact(@Nonnull String fieldName) {
        return getObject(fieldName);
    }

    @Override
    public byte[] readArrayOfInt8(@Nonnull String fieldName) {
        return getArrayOfInt8(fieldName);
    }

    @Override
    public boolean[] readArrayOfBoolean(@Nonnull String fieldName) {
        return getArrayOfBoolean(fieldName);
    }

    @Override
    public int[] readArrayOfInt32(@Nonnull String fieldName) {
        return getArrayOfInt32(fieldName);
    }

    @Override
    public long[] readArrayOfInt64(@Nonnull String fieldName) {
        return getArrayOfInt64(fieldName);
    }

    @Override
    public double[] readArrayOfFloat64(@Nonnull String fieldName) {
        return getArrayOfFloat64(fieldName);
    }

    @Override
    public float[] readArrayOfFloat32(@Nonnull String fieldName) {
        return getArrayOfFloat32(fieldName);
    }

    @Override
    public short[] readArrayOfInt16(@Nonnull String fieldName) {
        return getArrayOfInt16(fieldName);
    }

    @Override
    public String[] readArrayOfString(@Nonnull String fieldName) {
        return getArrayOfString(fieldName);
    }

    @Override
    public BigDecimal[] readArrayOfDecimal(@Nonnull String fieldName) {
        return getArrayOfDecimal(fieldName);
    }

    @Override
    public LocalTime[] readArrayOfTime(@Nonnull String fieldName) {
        return getArrayOfTime(fieldName);
    }

    @Override
    public LocalDate[] readArrayOfDate(@Nonnull String fieldName) {
        return getArrayOfDate(fieldName);
    }

    @Override
    public LocalDateTime[] readArrayOfTimestamp(@Nonnull String fieldName) {
        return getArrayOfTimestamp(fieldName);
    }

    @Override
    public OffsetDateTime[] readArrayOfTimestampWithTimezone(@Nonnull String fieldName) {
        return getArrayOfTimestampWithTimezone(fieldName);
    }

    @Override
    public <T> T[] readArrayOfCompact(@Nonnull String fieldName, Class<T> componentType) {
        return getArrayOfObject(fieldName, componentType);
    }

    @Nullable
    @Override
    public Boolean readNullableBoolean(@Nonnull String fieldName) {
        return getNullableBoolean(fieldName);
    }

    @Nullable
    @Override
    public Byte readNullableInt8(@Nonnull String fieldName) {
        return getNullableInt8(fieldName);
    }

    @Override
    public Short readNullableInt16(@Nonnull String fieldName) {
        return getNullableInt16(fieldName);
    }

    @Nullable
    @Override
    public Integer readNullableInt32(@Nonnull String fieldName) {
        return getNullableInt32(fieldName);
    }

    @Nullable
    @Override
    public Long readNullableInt64(@Nonnull String fieldName) {
        return getNullableInt64(fieldName);
    }

    @Nullable
    @Override
    public Float readNullableFloat32(@Nonnull String fieldName) {
        return getNullableFloat32(fieldName);
    }

    @Nullable
    @Override
    public Double readNullableFloat64(@Nonnull String fieldName) {
        return getNullableFloat64(fieldName);
    }

    @Nullable
    @Override
    public Boolean[] readArrayOfNullableBoolean(@Nonnull String fieldName) {
        return getArrayOfNullableBoolean(fieldName);
    }

    @Nullable
    @Override
    public Byte[] readArrayOfNullableInt8(@Nonnull String fieldName) {
        return getArrayOfNullableInt8(fieldName);
    }

    @Nullable
    @Override
    public Short[] readArrayOfNullableInt16(@Nonnull String fieldName) {
        return getArrayOfNullableInt16(fieldName);
    }

    @Nullable
    @Override
    public Integer[] readArrayOfNullableInt32(@Nonnull String fieldName) {
        return getArrayOfNullableInt32(fieldName);
    }

    @Nullable
    @Override
    public Long[] readArrayOfNullableInt64(@Nonnull String fieldName) {
        return getArrayOfNullableInt64(fieldName);
    }

    @Nullable
    @Override
    public Float[] readArrayOfNullableFloat32(@Nonnull String fieldName) {
        return getArrayOfNullableFloat32(fieldName);
    }

    @Nullable
    @Override
    public Double[] readArrayOfNullableFloat64(@Nonnull String fieldName) {
        return getArrayOfNullableFloat64(fieldName);
    }

}
