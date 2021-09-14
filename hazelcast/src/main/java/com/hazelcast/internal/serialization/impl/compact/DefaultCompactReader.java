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
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.nio.serialization.FieldType.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldType.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.BYTE;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_BYTE;
import static com.hazelcast.nio.serialization.FieldType.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.CHAR;
import static com.hazelcast.nio.serialization.FieldType.CHAR_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.COMPOSED;
import static com.hazelcast.nio.serialization.FieldType.COMPOSED_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DATE;
import static com.hazelcast.nio.serialization.FieldType.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.FLOAT;
import static com.hazelcast.nio.serialization.FieldType.FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.INT;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_INT;
import static com.hazelcast.nio.serialization.FieldType.INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.LONG;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_LONG;
import static com.hazelcast.nio.serialization.FieldType.LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.SHORT;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_SHORT;
import static com.hazelcast.nio.serialization.FieldType.SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UNSIGNED_SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UTF;
import static com.hazelcast.nio.serialization.FieldType.UTF_ARRAY;

/**
 * Adapter to make CompactInternalGenericRecord provide `CompactReader` API.
 */
public class DefaultCompactReader extends CompactInternalGenericRecord implements CompactReader {

    public DefaultCompactReader(CompactStreamSerializer serializer, BufferObjectDataInput in, Schema schema,
                                @Nullable Class associatedClass, boolean schemaIncludedInBinary) {
        super(serializer, in, schema, associatedClass, schemaIncludedInBinary);
    }

    @Override
    public byte readByte(@Nonnull String fieldName) {
        return getByte(fieldName);
    }

    @Override
    public int readUnsignedByte(@Nonnull String fieldName) {
        return getUnsignedByte(fieldName);
    }

    @Override
    public byte readByte(@Nonnull String fieldName, byte defaultValue) {
        return isFieldExists(fieldName, BYTE) ? getByte(fieldName) : defaultValue;
    }

    @Override
    public int readUnsignedByte(@Nonnull String fieldName, int defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_BYTE) ? getUnsignedByte(fieldName) : defaultValue;
    }

    @Override
    public short readShort(@Nonnull String fieldName) {
        return getShort(fieldName);
    }

    @Override
    public int readUnsignedShort(@Nonnull String fieldName) {
        return getUnsignedShort(fieldName);
    }

    @Override
    public short readShort(@Nonnull String fieldName, short defaultValue) {
        return isFieldExists(fieldName, SHORT) ? readShort(fieldName) : defaultValue;
    }

    @Override
    public int readUnsignedShort(@Nonnull String fieldName, int defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_SHORT) ? readUnsignedShort(fieldName) : defaultValue;
    }

    @Override
    public int readInt(@Nonnull String fieldName) {
        return getInt(fieldName);
    }

    @Override
    public long readUnsignedInt(@Nonnull String fieldName) {
        return getUnsignedInt(fieldName);
    }

    @Override
    public int readInt(@Nonnull String fieldName, int defaultValue) {
        return isFieldExists(fieldName, INT) ? getInt(fieldName) : defaultValue;
    }

    @Override
    public long readUnsignedInt(@Nonnull String fieldName, long defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_INT) ? getUnsignedInt(fieldName) : defaultValue;
    }

    @Override
    public long readLong(@Nonnull String fieldName) {
        return getLong(fieldName);
    }

    @Override
    public BigInteger readUnsignedLong(@Nonnull String fieldName) {
        return getUnsignedLong(fieldName);
    }

    @Override
    public long readLong(@Nonnull String fieldName, long defaultValue) {
        return isFieldExists(fieldName, LONG) ? getLong(fieldName) : defaultValue;
    }

    @Override
    public BigInteger readUnsignedLong(@Nonnull String fieldName, BigInteger defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_LONG) ? getUnsignedLong(fieldName) : defaultValue;
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
    public boolean readBoolean(@Nonnull String fieldName) {
        return getBoolean(fieldName);
    }

    @Override
    public boolean readBoolean(@Nonnull String fieldName, boolean defaultValue) {
        return isFieldExists(fieldName, BOOLEAN) ? getBoolean(fieldName) : defaultValue;
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
    public String readString(@Nonnull String fieldName, String defaultValue) {
        return isFieldExists(fieldName, UTF) ? getString(fieldName) : defaultValue;
    }

    @Override
    public BigDecimal readDecimal(@Nonnull String fieldName) {
        return getDecimal(fieldName);
    }

    @Override
    public BigDecimal readDecimal(@Nonnull String fieldName, BigDecimal defaultValue) {
        return isFieldExists(fieldName, DECIMAL) ? this.getDecimal(fieldName) : defaultValue;
    }

    @Override
    @Nonnull
    public LocalTime readTime(@Nonnull String fieldName) {
        return getTime(fieldName);
    }

    @Override
    @Nonnull
    public LocalTime readTime(@Nonnull String fieldName, @Nonnull LocalTime defaultValue) {
        return isFieldExists(fieldName, TIME) ? getTime(fieldName) : defaultValue;
    }

    @Override
    @Nonnull
    public LocalDate readDate(@Nonnull String fieldName) {
        return getDate(fieldName);
    }

    @Override
    @Nonnull
    public LocalDate readDate(@Nonnull String fieldName, @Nonnull LocalDate defaultValue) {
        return isFieldExists(fieldName, DATE) ? getDate(fieldName) : defaultValue;
    }

    @Override
    @Nonnull
    public LocalDateTime readTimestamp(@Nonnull String fieldName) {
        return getTimestamp(fieldName);
    }

    @Override
    @Nonnull
    public LocalDateTime readTimestamp(@Nonnull String fieldName, @Nonnull LocalDateTime defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP) ? getTimestamp(fieldName) : defaultValue;
    }

    @Override
    @Nonnull
    public OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName) {
        return getTimestampWithTimezone(fieldName);
    }

    @Override
    @Nonnull
    public OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName, @Nonnull OffsetDateTime defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_WITH_TIMEZONE) ? getTimestampWithTimezone(fieldName) : defaultValue;
    }

    @Override
    public <T> T readObject(@Nonnull String fieldName) {
        return getObject(fieldName);
    }

    @Override
    public <T> T readObject(@Nonnull String fieldName, T defaultValue) {
        return isFieldExists(fieldName, COMPOSED) ? getObject(fieldName) : defaultValue;
    }

    @Override
    public byte[] readByteArray(@Nonnull String fieldName) {
        return getByteArray(fieldName);
    }

    @Nullable
    @Override
    public int[] readUnsignedByteArray(@Nonnull String fieldName) {
        return getUnsignedByteArray(fieldName);
    }

    @Override
    public byte[] readByteArray(@Nonnull String fieldName, byte[] defaultValue) {
        return isFieldExists(fieldName, BYTE_ARRAY) ? getByteArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public int[] readUnsignedByteArray(@Nonnull String fieldName, @Nullable int[] defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_BYTE_ARRAY) ? getUnsignedByteArray(fieldName) : defaultValue;
    }

    @Override
    public boolean[] readBooleanArray(@Nonnull String fieldName) {
        return getBooleanArray(fieldName);
    }

    @Override
    public boolean[] readBooleanArray(@Nonnull String fieldName, boolean[] defaultValue) {
        return isFieldExists(fieldName, BOOLEAN_ARRAY) ? getBooleanArray(fieldName) : defaultValue;
    }

    @Override
    public char[] readCharArray(@Nonnull String fieldName) {
        return getCharArray(fieldName);
    }

    @Override
    public char[] readCharArray(@Nonnull String fieldName, char[] defaultValue) {
        return isFieldExists(fieldName, CHAR_ARRAY) ? getCharArray(fieldName) : defaultValue;
    }

    @Override
    public int[] readIntArray(@Nonnull String fieldName) {
        return getIntArray(fieldName);
    }

    @Nullable
    @Override
    public long[] readUnsignedIntArray(@Nonnull String fieldName) {
        return getUnsignedIntArray(fieldName);
    }

    @Override
    public int[] readIntArray(@Nonnull String fieldName, int[] defaultValue) {
        return isFieldExists(fieldName, INT_ARRAY) ? getIntArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public long[] readUnsignedIntArray(@Nonnull String fieldName, @Nullable long[] defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_INT_ARRAY) ? getUnsignedIntArray(fieldName) : defaultValue;
    }

    @Override
    public long[] readLongArray(@Nonnull String fieldName) {
        return getLongArray(fieldName);
    }

    @Nullable
    @Override
    public BigInteger[] readUnsignedLongArray(@Nonnull String fieldName) {
        return getUnsignedLongArray(fieldName);
    }

    @Override
    public long[] readLongArray(@Nonnull String fieldName, long[] defaultValue) {
        return isFieldExists(fieldName, LONG_ARRAY) ? getLongArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public BigInteger[] readLongArray(@Nonnull String fieldName, @Nullable BigInteger[] defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_LONG_ARRAY) ? getUnsignedLongArray(fieldName) : defaultValue;
    }

    @Override
    public double[] readDoubleArray(@Nonnull String fieldName) {
        return getDoubleArray(fieldName);
    }

    @Override
    public double[] readDoubleArray(@Nonnull String fieldName, double[] defaultValue) {
        return isFieldExists(fieldName, DOUBLE_ARRAY) ? getDoubleArray(fieldName) : defaultValue;
    }

    @Override
    public float[] readFloatArray(@Nonnull String fieldName) {
        return getFloatArray(fieldName);
    }

    @Override
    public float[] readFloatArray(@Nonnull String fieldName, float[] defaultValue) {
        return isFieldExists(fieldName, FLOAT_ARRAY) ? getFloatArray(fieldName) : defaultValue;
    }

    @Override
    public short[] readShortArray(@Nonnull String fieldName) {
        return getShortArray(fieldName);
    }

    @Nullable
    @Override
    public int[] readUnsignedShortArray(@Nonnull String fieldName) {
        return getUnsignedShortArray(fieldName);
    }

    @Override
    public short[] readShortArray(@Nonnull String fieldName, short[] defaultValue) {
        return isFieldExists(fieldName, SHORT_ARRAY) ? getShortArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public int[] readUnsignedShortArray(@Nonnull String fieldName, @Nullable int[] defaultValue) {
        return isFieldExists(fieldName, UNSIGNED_SHORT_ARRAY) ? getUnsignedShortArray(fieldName) : defaultValue;
    }

    @Override
    public String[] readStringArray(@Nonnull String fieldName) {
        return getStringArray(fieldName);
    }

    @Override
    public String[] readStringArray(@Nonnull String fieldName, String[] defaultValue) {
        return isFieldExists(fieldName, UTF_ARRAY) ? this.getStringArray(fieldName) : defaultValue;
    }

    @Override
    public BigDecimal[] readDecimalArray(@Nonnull String fieldName) {
        return getDecimalArray(fieldName);
    }

    @Override
    public BigDecimal[] readDecimalArray(@Nonnull String fieldName, BigDecimal[] defaultValue) {
        return isFieldExists(fieldName, DECIMAL_ARRAY) ? this.getDecimalArray(fieldName) : defaultValue;
    }

    @Override
    public LocalTime[] readTimeArray(@Nonnull String fieldName) {
        return getTimeArray(fieldName);
    }

    @Override
    public LocalTime[] readTimeArray(@Nonnull String fieldName, LocalTime[] defaultValue) {
        return isFieldExists(fieldName, TIME_ARRAY) ? this.getTimeArray(fieldName) : defaultValue;
    }

    @Override
    public LocalDate[] readDateArray(@Nonnull String fieldName) {
        return getDateArray(fieldName);
    }

    @Override
    public LocalDate[] readDateArray(@Nonnull String fieldName, LocalDate[] defaultValue) {
        return isFieldExists(fieldName, DATE_ARRAY) ? this.getDateArray(fieldName) : defaultValue;
    }

    @Override
    public LocalDateTime[] readTimestampArray(@Nonnull String fieldName) {
        return getTimestampArray(fieldName);
    }

    @Override
    public LocalDateTime[] readTimestampArray(@Nonnull String fieldName, LocalDateTime[] defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_ARRAY) ? this.getTimestampArray(fieldName) : defaultValue;
    }

    @Override
    public OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return getTimestampWithTimezoneArray(fieldName);
    }

    @Override
    public OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY)
                ? this.getTimestampWithTimezoneArray(fieldName) : defaultValue;
    }

    @Override
    public <T> T[] readObjectArray(@Nonnull String fieldName, Class<T> componentType) {
        return getObjectArray(fieldName, componentType);
    }

    @Override
    public <T> T[] readObjectArray(@Nonnull String fieldName, Class<T> componentType, T[] defaultValue) {
        return isFieldExists(fieldName, COMPOSED_ARRAY) ? getObjectArray(fieldName, componentType) : defaultValue;
    }
}
