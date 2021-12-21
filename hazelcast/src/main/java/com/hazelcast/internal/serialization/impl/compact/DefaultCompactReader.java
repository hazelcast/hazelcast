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
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT8S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMALS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT64S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT32S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT32S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT64S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT8S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT64S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT32S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT32S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT64S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT16S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT16S;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRINGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMPS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.INT8;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.INT32;
import static com.hazelcast.nio.serialization.FieldKind.INT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.INT16;
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
    public byte readInt8(@Nonnull String fieldName) {
        return getInt8(fieldName);
    }

    @Override
    public byte readInt8(@Nonnull String fieldName, byte defaultValue) {
        return isFieldExists(fieldName, INT8) ? getInt8(fieldName) : defaultValue;
    }

    @Override
    public short readInt16(@Nonnull String fieldName) {
        return getInt16(fieldName);
    }

    @Override
    public short readInt16(@Nonnull String fieldName, short defaultValue) {
        return isFieldExists(fieldName, INT16) ? readInt16(fieldName) : defaultValue;
    }

    @Override
    public int readInt32(@Nonnull String fieldName) {
        return getInt32(fieldName);
    }

    @Override
    public int readInt32(@Nonnull String fieldName, int defaultValue) {
        return isFieldExists(fieldName, INT32) ? getInt32(fieldName) : defaultValue;
    }

    @Override
    public long readInt64(@Nonnull String fieldName) {
        return getInt64(fieldName);
    }

    @Override
    public long readInt64(@Nonnull String fieldName, long defaultValue) {
        return isFieldExists(fieldName, INT64) ? getInt64(fieldName) : defaultValue;
    }

    @Override
    public float readFloat32(@Nonnull String fieldName) {
        return getFloat32(fieldName);
    }

    @Override
    public float readFloat32(@Nonnull String fieldName, float defaultValue) {
        return isFieldExists(fieldName, FLOAT32) ? getFloat32(fieldName) : defaultValue;
    }

    @Override
    public double readFloat64(@Nonnull String fieldName) {
        return getFloat64(fieldName);
    }

    @Override
    public double readFloat64(@Nonnull String fieldName, double defaultValue) {
        return isFieldExists(fieldName, FLOAT64) ? getFloat64(fieldName) : defaultValue;
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
    public byte[] readArrayOInt8s(@Nonnull String fieldName) {
        return getArrayOfInt8s(fieldName);
    }

    @Nullable
    @Override
    public byte[] readArrayOInt8s(@Nonnull String fieldName, @Nullable byte[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT8S) ? getArrayOfInt8s(fieldName) : defaultValue;
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
    public int[] readArrayOfInt32s(@Nonnull String fieldName) {
        return getArrayOfInt32s(fieldName);
    }

    @Nullable
    @Override
    public int[] readArrayOfInt32s(@Nonnull String fieldName, @Nullable int[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT32S) ? getArrayOfInt32s(fieldName) : defaultValue;
    }

    @Override
    public long[] readArrayOfInt64s(@Nonnull String fieldName) {
        return getArrayOfInt64s(fieldName);
    }

    @Nullable
    @Override
    public long[] readArrayOfInt64s(@Nonnull String fieldName, @Nullable long[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT64S) ? getArrayOfInt64s(fieldName) : defaultValue;
    }

    @Override
    public double[] readArrayOfFloat64s(@Nonnull String fieldName) {
        return getArrayOfFloat64s(fieldName);
    }

    @Nullable
    @Override
    public double[] readArrayOfFloat64s(@Nonnull String fieldName, @Nullable double[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_FLOAT64S) ? getArrayOfFloat64s(fieldName) : defaultValue;
    }

    @Override
    public float[] readArrayOfFloat32s(@Nonnull String fieldName) {
        return getArrayOfFloat32s(fieldName);
    }

    @Nullable
    @Override
    public float[] readArrayOfFloat32s(@Nonnull String fieldName, @Nullable float[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_FLOAT32S) ? getArrayOfFloat32s(fieldName) : defaultValue;
    }

    @Override
    public short[] readArrayOfInt16s(@Nonnull String fieldName) {
        return getArrayOfInt16s(fieldName);
    }

    @Nullable
    @Override
    public short[] readArrayOfInt16s(@Nonnull String fieldName, @Nullable short[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT16S) ? getArrayOfInt16s(fieldName) : defaultValue;
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
    public Byte readNullableInt8(@Nonnull String fieldName) {
        return getNullableInt8(fieldName);
    }

    @Nullable
    @Override
    public Byte readNullableInt8(@Nonnull String fieldName, @Nullable Byte defaultValue) {
        return isFieldExists(fieldName, INT8) ? getNullableInt8(fieldName) : defaultValue;
    }

    @Override
    public Short readNullableInt16(@Nonnull String fieldName) {
        return getNullableInt16(fieldName);
    }

    @Override
    public Short readNullableInt16(@Nonnull String fieldName, @Nullable Short defaultValue) {
        return isFieldExists(fieldName, INT16) ? getNullableInt16(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Integer readNullableInt32(@Nonnull String fieldName) {
        return getNullableInt32(fieldName);
    }

    @Nullable
    @Override
    public Integer readNullableInt32(@Nonnull String fieldName, @Nullable Integer defaultValue) {
        return isFieldExists(fieldName, NULLABLE_INT32) ? getNullableInt32(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Long readNullableInt64(@Nonnull String fieldName) {
        return getNullableInt64(fieldName);
    }

    @Nullable
    @Override
    public Long readNullableInt64(@Nonnull String fieldName, @Nullable Long defaultValue) {
        return isFieldExists(fieldName, NULLABLE_INT64) ? getNullableInt64(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Float readNullableFloat32(@Nonnull String fieldName) {
        return getNullableFloat32(fieldName);
    }

    @Nullable
    @Override
    public Float readNullableFloat32(@Nonnull String fieldName, @Nullable Float defaultValue) {
        return isFieldExists(fieldName, NULLABLE_FLOAT32) ? getNullableFloat32(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Double readNullableFloat64(@Nonnull String fieldName) {
        return getNullableFloat64(fieldName);
    }

    @Nullable
    @Override
    public Double readNullableFloat64(@Nonnull String fieldName, @Nullable Double defaultValue) {
        return isFieldExists(fieldName, NULLABLE_FLOAT64) ? getNullableFloat64(fieldName) : defaultValue;
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
    public Byte[] readArrayOfNullableInt8s(@Nonnull String fieldName) {
        return getArrayOfNullableInt8s(fieldName);
    }

    @Nullable
    @Override
    public Byte[] readArrayOfNullableInt8s(@Nonnull String fieldName, @Nullable Byte[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT8S) ? getArrayOfNullableInt8s(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Short[] readArrayOfNullableInt16s(@Nonnull String fieldName) {
        return getArrayOfNullableInt16s(fieldName);
    }

    @Nullable
    @Override
    public Short[] readArrayOfNullableInt16s(@Nonnull String fieldName, @Nullable Short[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT16S) ? getArrayOfNullableInt16s(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Integer[] readArrayOfNullableInt32s(@Nonnull String fieldName) {
        return getArrayOfNullableInt32s(fieldName);
    }

    @Nullable
    @Override
    public Integer[] readArrayOfNullableInt32s(@Nonnull String fieldName, @Nullable Integer[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT32S) ? getArrayOfNullableInt32s(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Long[] readArrayOfNullableInt64s(@Nonnull String fieldName) {
        return getArrayOfNullableInt64s(fieldName);
    }

    @Nullable
    @Override
    public Long[] readArrayOfNullableInt64s(@Nonnull String fieldName, @Nullable Long[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT64S) ? getArrayOfNullableInt64s(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Float[] readArrayOfNullableFloat32s(@Nonnull String fieldName) {
        return getArrayOfNullableFloat32s(fieldName);
    }

    @Nullable
    @Override
    public Float[] readArrayOfNullableFloat32s(@Nonnull String fieldName, @Nullable Float[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_FLOAT32S) ? getArrayOfNullableFloat32s(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Double[] readArrayOfNullableFloat64s(@Nonnull String fieldName) {
        return getArrayOfNullableFloat64s(fieldName);
    }

    @Nullable
    @Override
    public Double[] readArrayOfNullableFloat64s(@Nonnull String fieldName, @Nullable Double[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_FLOAT64S) ? getArrayOfNullableFloat64s(fieldName) : defaultValue;
    }
}
