/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.compact.CompactReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.BYTE;
import static com.hazelcast.nio.serialization.FieldKind.CHAR;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.INT;
import static com.hazelcast.nio.serialization.FieldKind.LONG;
import static com.hazelcast.nio.serialization.FieldKind.SHORT;

/**
 * Adapter to make CompactInternalGenericRecord provide `CompactReader` API.
 */
public class DefaultCompactReader extends CompactInternalGenericRecord implements CompactReader {

    public DefaultCompactReader(CompactStreamSerializer serializer, BufferObjectDataInput in, Schema schema,
                                @Nullable Class associatedClass, boolean schemaIncludedInBinary) {
        super(serializer, in, schema, associatedClass, schemaIncludedInBinary);
    }


    @Override
    public boolean readBoolean(@Nonnull String fieldName) {
        return getBoolean(fieldName);
    }

    @Override
    public boolean readBoolean(@Nonnull String fieldName, boolean defaultValue) {
        return isFieldExists(fieldName, BOOLEAN) ? getBoolean(fieldName) : defaultValue;
    }

    @Override
    public byte readByte(@Nonnull String fieldName) {
        return getByte(fieldName);
    }

    @Override
    public byte readByte(@Nonnull String fieldName, byte defaultValue) {
        return isFieldExists(fieldName, BYTE) ? getByte(fieldName) : defaultValue;
    }

    @Override
    public short readShort(@Nonnull String fieldName) {
        return getShort(fieldName);
    }

    @Override
    public short readShort(@Nonnull String fieldName, short defaultValue) {
        return isFieldExists(fieldName, SHORT) ? readShort(fieldName) : defaultValue;
    }

    @Override
    public int readInt(@Nonnull String fieldName) {
        return getInt(fieldName);
    }

    @Override
    public int readInt(@Nonnull String fieldName, int defaultValue) {
        return isFieldExists(fieldName, INT) ? getInt(fieldName) : defaultValue;
    }

    @Override
    public long readLong(@Nonnull String fieldName) {
        return getLong(fieldName);
    }

    @Override
    public long readLong(@Nonnull String fieldName, long defaultValue) {
        return isFieldExists(fieldName, LONG) ? getLong(fieldName) : defaultValue;
    }

    @Override
    public float readFloat(@Nonnull String fieldName) {
        return getFloat(fieldName);
    }

    @Override
    public float readFloat(@Nonnull String fieldName, float defaultValue) {
        return isFieldExists(fieldName, FLOAT) ? getFloat(fieldName) : defaultValue;
    }

    @Override
    public double readDouble(@Nonnull String fieldName) {
        return getDouble(fieldName);
    }

    @Override
    public double readDouble(@Nonnull String fieldName, double defaultValue) {
        return isFieldExists(fieldName, DOUBLE) ? getDouble(fieldName) : defaultValue;
    }

    @Override
    public char readChar(@Nonnull String fieldName) {
        return getChar(fieldName);
    }

    @Override
    public char readChar(@Nonnull String fieldName, char defaultValue) {
        return isFieldExists(fieldName, CHAR) ? getChar(fieldName) : defaultValue;
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
    public byte[] readByteArray(@Nonnull String fieldName) {
        return getByteArray(fieldName);
    }

    @Override
    public boolean[] readBooleanArray(@Nonnull String fieldName) {
        return getBooleanArray(fieldName);
    }

    @Override
    public char[] readCharArray(@Nonnull String fieldName) {
        return getCharArray(fieldName);
    }

    @Override
    public int[] readIntArray(@Nonnull String fieldName) {
        return getIntArray(fieldName);
    }

    @Override
    public long[] readLongArray(@Nonnull String fieldName) {
        return getLongArray(fieldName);
    }

    @Override
    public double[] readDoubleArray(@Nonnull String fieldName) {
        return getDoubleArray(fieldName);
    }

    @Override
    public float[] readFloatArray(@Nonnull String fieldName) {
        return getFloatArray(fieldName);
    }

    @Override
    public short[] readShortArray(@Nonnull String fieldName) {
        return getShortArray(fieldName);
    }

    @Override
    public String[] readStringArray(@Nonnull String fieldName) {
        return getStringArray(fieldName);
    }

    @Override
    public BigDecimal[] readDecimalArray(@Nonnull String fieldName) {
        return getDecimalArray(fieldName);
    }

    @Override
    public LocalTime[] readTimeArray(@Nonnull String fieldName) {
        return getTimeArray(fieldName);
    }

    @Override
    public LocalDate[] readDateArray(@Nonnull String fieldName) {
        return getDateArray(fieldName);
    }

    @Override
    public LocalDateTime[] readTimestampArray(@Nonnull String fieldName) {
        return getTimestampArray(fieldName);
    }

    @Override
    public OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return getTimestampWithTimezoneArray(fieldName);
    }

    @Override
    public <T> T[] readCompactArray(@Nonnull String fieldName, Class<T> componentType) {
        return getObjectArray(fieldName, componentType);
    }

    @Nullable
    @Override
    public Boolean readNullableBoolean(@Nonnull String fieldName) {
        return getNullableBoolean(fieldName);
    }

    @Nullable
    @Override
    public Byte readNullableByte(@Nonnull String fieldName) {
        return getNullableByte(fieldName);
    }

    @Override
    public Short readNullableShort(@Nonnull String fieldName) {
        return getNullableShort(fieldName);
    }

    @Nullable
    @Override
    public Integer readNullableInt(@Nonnull String fieldName) {
        return getNullableInt(fieldName);
    }

    @Nullable
    @Override
    public Long readNullableLong(@Nonnull String fieldName) {
        return getNullableLong(fieldName);
    }

    @Nullable
    @Override
    public Float readNullableFloat(@Nonnull String fieldName) {
        return getNullableFloat(fieldName);
    }

    @Nullable
    @Override
    public Double readNullableDouble(@Nonnull String fieldName) {
        return getNullableDouble(fieldName);
    }

    @Nullable
    @Override
    public Boolean[] readNullableBooleanArray(@Nonnull String fieldName) {
        return getNullableBooleanArray(fieldName);
    }

    @Nullable
    @Override
    public Byte[] readNullableByteArray(@Nonnull String fieldName) {
        return getNullableByteArray(fieldName);
    }

    @Nullable
    @Override
    public Short[] readNullableShortArray(@Nonnull String fieldName) {
        return getNullableShortArray(fieldName);
    }

    @Nullable
    @Override
    public Integer[] readNullableIntArray(@Nonnull String fieldName) {
        return getNullableIntArray(fieldName);
    }

    @Nullable
    @Override
    public Long[] readNullableLongArray(@Nonnull String fieldName) {
        return getNullableLongArray(fieldName);
    }

    @Nullable
    @Override
    public Float[] readNullableFloatArray(@Nonnull String fieldName) {
        return getNullableFloatArray(fieldName);
    }

    @Nullable
    @Override
    public Double[] readNullableDoubleArray(@Nonnull String fieldName) {
        return getNullableDoubleArray(fieldName);
    }
}
