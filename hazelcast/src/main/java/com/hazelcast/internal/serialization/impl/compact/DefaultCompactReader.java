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
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.BYTE;
import static com.hazelcast.nio.serialization.FieldKind.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.CHAR;
import static com.hazelcast.nio.serialization.FieldKind.CHAR_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.INT;
import static com.hazelcast.nio.serialization.FieldKind.INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.LONG;
import static com.hazelcast.nio.serialization.FieldKind.LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_LONG;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.SHORT;
import static com.hazelcast.nio.serialization.FieldKind.SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.STRING_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.TIME_ARRAY;

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

    @Nullable
    @Override
    public String readString(@Nonnull String fieldName, @Nullable String defaultValue) {
        return isFieldExists(fieldName, STRING) ? getString(fieldName) : defaultValue;
    }

    @Override
    public BigDecimal readDecimal(@Nonnull String fieldName) {
        return getDecimal(fieldName);
    }

    @Nullable
    @Override
    public BigDecimal readDecimal(@Nonnull String fieldName, @Nullable BigDecimal defaultValue) {
        return isFieldExists(fieldName, DECIMAL) ? getDecimal(fieldName) : defaultValue;
    }

    @Override
    @Nullable
    public LocalTime readTime(@Nonnull String fieldName) {
        return getTime(fieldName);
    }

    @Nullable
    @Override
    public LocalTime readTime(@Nonnull String fieldName, @Nullable LocalTime defaultValue) {
        return isFieldExists(fieldName, TIME) ? getTime(fieldName) : defaultValue;
    }

    @Override
    @Nullable
    public LocalDate readDate(@Nonnull String fieldName) {
        return getDate(fieldName);
    }

    @Nullable
    @Override
    public LocalDate readDate(@Nonnull String fieldName, @Nullable LocalDate defaultValue) {
        return isFieldExists(fieldName, DATE) ? getDate(fieldName) : defaultValue;
    }

    @Override
    @Nullable
    public LocalDateTime readTimestamp(@Nonnull String fieldName) {
        return getTimestamp(fieldName);
    }

    @Nullable
    @Override
    public LocalDateTime readTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP) ? getTimestamp(fieldName) : defaultValue;
    }

    @Override
    @Nullable
    public OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName) {
        return getTimestampWithTimezone(fieldName);
    }

    @Nullable
    @Override
    public OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_WITH_TIMEZONE) ? getTimestampWithTimezone(fieldName) : defaultValue;
    }

    @Override
    public <T> T readCompact(@Nonnull String fieldName) {
        return getObject(fieldName);
    }

    @Nullable
    @Override
    public <T> T readCompact(@Nonnull String fieldName, @Nullable T defaultValue) {
        return isFieldExists(fieldName, COMPACT) ? getObject(fieldName) : defaultValue;
    }

    @Override
    public byte[] readByteArray(@Nonnull String fieldName) {
        return getByteArray(fieldName);
    }

    @Nullable
    @Override
    public byte[] readByteArray(@Nonnull String fieldName, @Nullable byte[] defaultValue) {
        return isFieldExists(fieldName, BYTE_ARRAY) ? getByteArray(fieldName) : defaultValue;
    }

    @Override
    public boolean[] readBooleanArray(@Nonnull String fieldName) {
        return getBooleanArray(fieldName);
    }

    @Nullable
    @Override
    public boolean[] readBooleanArray(@Nonnull String fieldName, @Nullable boolean[] defaultValue) {
        return isFieldExists(fieldName, BOOLEAN_ARRAY) ? getBooleanArray(fieldName) : defaultValue;
    }

    @Override
    public char[] readCharArray(@Nonnull String fieldName) {
        return getCharArray(fieldName);
    }

    @Nullable
    @Override
    public char[] readCharArray(@Nonnull String fieldName, @Nullable char[] defaultValue) {
        return isFieldExists(fieldName, CHAR_ARRAY) ? getCharArray(fieldName) : defaultValue;
    }

    @Override
    public int[] readIntArray(@Nonnull String fieldName) {
        return getIntArray(fieldName);
    }

    @Nullable
    @Override
    public int[] readIntArray(@Nonnull String fieldName, @Nullable int[] defaultValue) {
        return isFieldExists(fieldName, INT_ARRAY) ? getIntArray(fieldName) : defaultValue;
    }

    @Override
    public long[] readLongArray(@Nonnull String fieldName) {
        return getLongArray(fieldName);
    }

    @Nullable
    @Override
    public long[] readLongArray(@Nonnull String fieldName, @Nullable long[] defaultValue) {
        return isFieldExists(fieldName, LONG_ARRAY) ? getLongArray(fieldName) : defaultValue;
    }

    @Override
    public double[] readDoubleArray(@Nonnull String fieldName) {
        return getDoubleArray(fieldName);
    }

    @Nullable
    @Override
    public double[] readDoubleArray(@Nonnull String fieldName, @Nullable double[] defaultValue) {
        return isFieldExists(fieldName, DOUBLE_ARRAY) ? getDoubleArray(fieldName) : defaultValue;
    }

    @Override
    public float[] readFloatArray(@Nonnull String fieldName) {
        return getFloatArray(fieldName);
    }

    @Nullable
    @Override
    public float[] readFloatArray(@Nonnull String fieldName, @Nullable float[] defaultValue) {
        return isFieldExists(fieldName, FLOAT_ARRAY) ? getFloatArray(fieldName) : defaultValue;
    }

    @Override
    public short[] readShortArray(@Nonnull String fieldName) {
        return getShortArray(fieldName);
    }

    @Nullable
    @Override
    public short[] readShortArray(@Nonnull String fieldName, @Nullable short[] defaultValue) {
        return isFieldExists(fieldName, SHORT_ARRAY) ? getShortArray(fieldName) : defaultValue;
    }

    @Override
    public String[] readStringArray(@Nonnull String fieldName) {
        return getStringArray(fieldName);
    }

    @Nullable
    @Override
    public String[] readStringArray(@Nonnull String fieldName, @Nullable String[] defaultValue) {
        return isFieldExists(fieldName, STRING_ARRAY) ? getStringArray(fieldName) : defaultValue;
    }

    @Override
    public BigDecimal[] readDecimalArray(@Nonnull String fieldName) {
        return getDecimalArray(fieldName);
    }

    @Nullable
    @Override
    public BigDecimal[] readDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] defaultValue) {
        return isFieldExists(fieldName, DECIMAL_ARRAY) ? getDecimalArray(fieldName) : defaultValue;
    }

    @Override
    public LocalTime[] readTimeArray(@Nonnull String fieldName) {
        return getTimeArray(fieldName);
    }

    @Nullable
    @Override
    public LocalTime[] readTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] defaultValue) {
        return isFieldExists(fieldName, TIME_ARRAY) ? getTimeArray(fieldName) : defaultValue;
    }

    @Override
    public LocalDate[] readDateArray(@Nonnull String fieldName) {
        return getDateArray(fieldName);
    }

    @Nullable
    @Override
    public LocalDate[] readDateArray(@Nonnull String fieldName, @Nullable LocalDate[] defaultValue) {
        return isFieldExists(fieldName, DATE_ARRAY) ? getDateArray(fieldName) : defaultValue;
    }

    @Override
    public LocalDateTime[] readTimestampArray(@Nonnull String fieldName) {
        return getTimestampArray(fieldName);
    }

    @Nullable
    @Override
    public LocalDateTime[] readTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_ARRAY) ? getTimestampArray(fieldName) : defaultValue;
    }

    @Override
    public OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return getTimestampWithTimezoneArray(fieldName);
    }

    @Nullable
    @Override
    public OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY) ? getTimestampWithTimezoneArray(fieldName) : defaultValue;
    }

    @Override
    public <T> T[] readCompactArray(@Nonnull String fieldName, Class<T> componentType) {
        return getObjectArray(fieldName, componentType);
    }

    @Nullable
    @Override
    public <T> T[] readCompactArray(@Nonnull String fieldName, @Nullable Class<T> componentType, @Nullable T[] defaultValue) {
        return isFieldExists(fieldName, COMPACT_ARRAY) ? getObjectArray(fieldName, componentType) : defaultValue;
    }

    @Nullable
    @Override
    public Boolean readNullableBoolean(@Nonnull String fieldName) {
        return getNullableBoolean(fieldName);
    }

    @Nullable
    @Override
    public Boolean readNullableBoolean(@Nonnull String fieldName, @Nullable Boolean defaultValue) {
        return isFieldExists(fieldName, BOOLEAN) ? getNullableBoolean(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Byte readNullableByte(@Nonnull String fieldName) {
        return getNullableByte(fieldName);
    }

    @Nullable
    @Override
    public Byte readNullableByte(@Nonnull String fieldName, @Nullable Byte defaultValue) {
        return isFieldExists(fieldName, BYTE) ? getNullableByte(fieldName) : defaultValue;
    }

    @Override
    public Short readNullableShort(@Nonnull String fieldName) {
        return getNullableShort(fieldName);
    }

    @Override
    public Short readNullableShort(@Nonnull String fieldName, @Nullable Short defaultValue) {
        return isFieldExists(fieldName, SHORT) ? getNullableShort(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Integer readNullableInt(@Nonnull String fieldName) {
        return getNullableInt(fieldName);
    }

    @Nullable
    @Override
    public Integer readNullableInt(@Nonnull String fieldName, @Nullable Integer defaultValue) {
        return isFieldExists(fieldName, NULLABLE_INT) ? getNullableInt(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Long readNullableLong(@Nonnull String fieldName) {
        return getNullableLong(fieldName);
    }

    @Nullable
    @Override
    public Long readNullableLong(@Nonnull String fieldName, @Nullable Long defaultValue) {
        return isFieldExists(fieldName, NULLABLE_LONG) ? getNullableLong(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Float readNullableFloat(@Nonnull String fieldName) {
        return getNullableFloat(fieldName);
    }

    @Nullable
    @Override
    public Float readNullableFloat(@Nonnull String fieldName, @Nullable Float defaultValue) {
        return isFieldExists(fieldName, NULLABLE_FLOAT) ? getNullableFloat(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Double readNullableDouble(@Nonnull String fieldName) {
        return getNullableDouble(fieldName);
    }

    @Nullable
    @Override
    public Double readNullableDouble(@Nonnull String fieldName, @Nullable Double defaultValue) {
        return isFieldExists(fieldName, NULLABLE_DOUBLE) ? getNullableDouble(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Boolean[] readNullableBooleanArray(@Nonnull String fieldName) {
        return getNullableBooleanArray(fieldName);
    }

    @Nullable
    @Override
    public Boolean[] readNullableBooleanArray(@Nonnull String fieldName, @Nullable Boolean[] defaultValue) {
        return isFieldExists(fieldName, NULLABLE_BOOLEAN_ARRAY) ? getNullableBooleanArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Byte[] readNullableByteArray(@Nonnull String fieldName) {
        return getNullableByteArray(fieldName);
    }

    @Nullable
    @Override
    public Byte[] readNullableByteArray(@Nonnull String fieldName, @Nullable Byte[] defaultValue) {
        return isFieldExists(fieldName, NULLABLE_BYTE_ARRAY) ? getNullableByteArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Short[] readNullableShortArray(@Nonnull String fieldName) {
        return getNullableShortArray(fieldName);
    }

    @Nullable
    @Override
    public Short[] readNullableShortArray(@Nonnull String fieldName, @Nullable Short[] defaultValue) {
        return isFieldExists(fieldName, NULLABLE_SHORT_ARRAY) ? getNullableShortArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Integer[] readNullableIntArray(@Nonnull String fieldName) {
        return getNullableIntArray(fieldName);
    }

    @Nullable
    @Override
    public Integer[] readNullableIntArray(@Nonnull String fieldName, @Nullable Integer[] defaultValue) {
        return isFieldExists(fieldName, NULLABLE_INT_ARRAY) ? getNullableIntArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Long[] readNullableLongArray(@Nonnull String fieldName) {
        return getNullableLongArray(fieldName);
    }

    @Nullable
    @Override
    public Long[] readNullableLongArray(@Nonnull String fieldName, @Nullable Long[] defaultValue) {
        return isFieldExists(fieldName, NULLABLE_LONG_ARRAY) ? getNullableLongArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Float[] readNullableFloatArray(@Nonnull String fieldName) {
        return getNullableFloatArray(fieldName);
    }

    @Nullable
    @Override
    public Float[] readNullableFloatArray(@Nonnull String fieldName, @Nullable Float[] defaultValue) {
        return isFieldExists(fieldName, NULLABLE_FLOAT_ARRAY) ? getNullableFloatArray(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Double[] readNullableDoubleArray(@Nonnull String fieldName) {
        return getNullableDoubleArray(fieldName);
    }

    @Nullable
    @Override
    public Double[] readNullableDoubleArray(@Nonnull String fieldName, @Nullable Double[] defaultValue) {
        return isFieldExists(fieldName, NULLABLE_DOUBLE_ARRAY) ? getNullableDoubleArray(fieldName) : defaultValue;
    }
}
