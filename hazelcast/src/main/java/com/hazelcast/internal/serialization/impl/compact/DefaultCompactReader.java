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

import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMALS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRINGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMPS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.BYTE;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.INT;
import static com.hazelcast.nio.serialization.FieldKind.LONG;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_LONG;
import static com.hazelcast.nio.serialization.FieldKind.SHORT;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;

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
    public byte[] readArrayOfBytes(@Nonnull String fieldName) {
        return getArrayOfBytes(fieldName);
    }

    @Nullable
    @Override
    public byte[] readArrayOfBytes(@Nonnull String fieldName, @Nullable byte[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_BYTES) ? getArrayOfBytes(fieldName) : defaultValue;
    }

    @Override
    public boolean[] readArrayOfBooleans(@Nonnull String fieldName) {
        return getArrayOfBooleans(fieldName);
    }

    @Nullable
    @Override
    public boolean[] readArrayOfBooleans(@Nonnull String fieldName, @Nullable boolean[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_BOOLEANS) ? getArrayOfBooleans(fieldName) : defaultValue;
    }

    @Override
    public int[] readArrayOfInts(@Nonnull String fieldName) {
        return getArrayOfInts(fieldName);
    }

    @Nullable
    @Override
    public int[] readArrayOfInts(@Nonnull String fieldName, @Nullable int[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INTS) ? getArrayOfInts(fieldName) : defaultValue;
    }

    @Override
    public long[] readArrayOfLongs(@Nonnull String fieldName) {
        return getArrayOfLongs(fieldName);
    }

    @Nullable
    @Override
    public long[] readArrayOfLongs(@Nonnull String fieldName, @Nullable long[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_LONGS) ? getArrayOfLongs(fieldName) : defaultValue;
    }

    @Override
    public double[] readArrayOfDoubles(@Nonnull String fieldName) {
        return getArrayOfDoubles(fieldName);
    }

    @Nullable
    @Override
    public double[] readArrayOfDoubles(@Nonnull String fieldName, @Nullable double[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_DOUBLES) ? getArrayOfDoubles(fieldName) : defaultValue;
    }

    @Override
    public float[] readArrayOfFloats(@Nonnull String fieldName) {
        return getArrayOfFloats(fieldName);
    }

    @Nullable
    @Override
    public float[] readArrayOfFloats(@Nonnull String fieldName, @Nullable float[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_FLOATS) ? getArrayOfFloats(fieldName) : defaultValue;
    }

    @Override
    public short[] readArrayOfShorts(@Nonnull String fieldName) {
        return getArrayOfShorts(fieldName);
    }

    @Nullable
    @Override
    public short[] readArrayOfShorts(@Nonnull String fieldName, @Nullable short[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_SHORTS) ? getArrayOfShorts(fieldName) : defaultValue;
    }

    @Override
    public String[] readArrayOfStrings(@Nonnull String fieldName) {
        return getArrayOfStrings(fieldName);
    }

    @Nullable
    @Override
    public String[] readArrayOfStrings(@Nonnull String fieldName, @Nullable String[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_STRINGS) ? getArrayOfStrings(fieldName) : defaultValue;
    }

    @Override
    public BigDecimal[] readArrayOfDecimals(@Nonnull String fieldName) {
        return getArrayOfDecimals(fieldName);
    }

    @Nullable
    @Override
    public BigDecimal[] readArrayOfDecimals(@Nonnull String fieldName, @Nullable BigDecimal[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_DECIMALS) ? getArrayOfDecimals(fieldName) : defaultValue;
    }

    @Override
    public LocalTime[] readArrayOfTimes(@Nonnull String fieldName) {
        return getArrayOfTimes(fieldName);
    }

    @Nullable
    @Override
    public LocalTime[] readArrayOfTimes(@Nonnull String fieldName, @Nullable LocalTime[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_TIMES) ? getArrayOfTimes(fieldName) : defaultValue;
    }

    @Override
    public LocalDate[] readArrayOfDates(@Nonnull String fieldName) {
        return getArrayOfDates(fieldName);
    }

    @Nullable
    @Override
    public LocalDate[] readArrayOfDates(@Nonnull String fieldName, @Nullable LocalDate[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_DATES) ? getArrayOfDates(fieldName) : defaultValue;
    }

    @Override
    public LocalDateTime[] readArrayOfTimestamps(@Nonnull String fieldName) {
        return getArrayOfTimestamps(fieldName);
    }

    @Nullable
    @Override
    public LocalDateTime[] readArrayOfTimestamps(@Nonnull String fieldName, @Nullable LocalDateTime[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_TIMESTAMPS) ? getArrayOfTimestamps(fieldName) : defaultValue;
    }

    @Override
    public OffsetDateTime[] readArrayOfTimestampWithTimezones(@Nonnull String fieldName) {
        return getArrayOfTimestampWithTimezones(fieldName);
    }

    @Nullable
    @Override
    public OffsetDateTime[] readArrayOfTimestampWithTimezones(@Nonnull String fieldName,
                                                              @Nullable OffsetDateTime[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONES)
                ? getArrayOfTimestampWithTimezones(fieldName) : defaultValue;
    }

    @Override
    public <T> T[] readArrayOfCompacts(@Nonnull String fieldName, Class<T> componentType) {
        return getArrayOfObjects(fieldName, componentType);
    }

    @Nullable
    @Override
    public <T> T[] readArrayOfCompacts(@Nonnull String fieldName, @Nullable Class<T> componentType, @Nullable T[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_COMPACTS) ? getArrayOfObjects(fieldName, componentType) : defaultValue;
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
    public Boolean[] readArrayOfNullableBooleans(@Nonnull String fieldName) {
        return getArrayOfNullableBooleans(fieldName);
    }

    @Nullable
    @Override
    public Boolean[] readArrayOfNullableBooleans(@Nonnull String fieldName, @Nullable Boolean[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_BOOLEANS) ? getArrayOfNullableBooleans(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Byte[] readArrayOfNullableBytes(@Nonnull String fieldName) {
        return getArrayOfNullableBytes(fieldName);
    }

    @Nullable
    @Override
    public Byte[] readArrayOfNullableBytes(@Nonnull String fieldName, @Nullable Byte[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_BYTES) ? getArrayOfNullableBytes(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Short[] readArrayOfNullableShorts(@Nonnull String fieldName) {
        return getArrayOfNullableShorts(fieldName);
    }

    @Nullable
    @Override
    public Short[] readArrayOfNullableShorts(@Nonnull String fieldName, @Nullable Short[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_SHORTS) ? getArrayOfNullableShorts(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Integer[] readArrayOfNullableInts(@Nonnull String fieldName) {
        return getArrayOfNullableInts(fieldName);
    }

    @Nullable
    @Override
    public Integer[] readArrayOfNullableInts(@Nonnull String fieldName, @Nullable Integer[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INTS) ? getArrayOfNullableInts(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Long[] readArrayOfNullableLongs(@Nonnull String fieldName) {
        return getArrayOfNullableLongs(fieldName);
    }

    @Nullable
    @Override
    public Long[] readArrayOfNullableLongs(@Nonnull String fieldName, @Nullable Long[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_LONGS) ? getArrayOfNullableLongs(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Float[] readArrayOfNullableFloats(@Nonnull String fieldName) {
        return getArrayOfNullableFloats(fieldName);
    }

    @Nullable
    @Override
    public Float[] readArrayOfNullableFloats(@Nonnull String fieldName, @Nullable Float[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_FLOATS) ? getArrayOfNullableFloats(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Double[] readArrayOfNullableDoubles(@Nonnull String fieldName) {
        return getArrayOfNullableDoubles(fieldName);
    }

    @Nullable
    @Override
    public Double[] readArrayOfNullableDoubles(@Nonnull String fieldName, @Nullable Double[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_DOUBLES) ? getArrayOfNullableDoubles(fieldName) : defaultValue;
    }
}
