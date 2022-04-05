/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRING;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIME;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.INT16;
import static com.hazelcast.nio.serialization.FieldKind.INT32;
import static com.hazelcast.nio.serialization.FieldKind.INT64;
import static com.hazelcast.nio.serialization.FieldKind.INT8;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;

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
    public byte[] readArrayOfInt8(@Nonnull String fieldName) {
        return getArrayOfInt8(fieldName);
    }

    @Nullable
    @Override
    public byte[] readArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT8) ? getArrayOfInt8(fieldName) : defaultValue;
    }

    @Override
    public boolean[] readArrayOfBoolean(@Nonnull String fieldName) {
        return getArrayOfBoolean(fieldName);
    }

    @Nullable
    @Override
    public boolean[] readArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_BOOLEAN) ? getArrayOfBoolean(fieldName) : defaultValue;
    }

    @Override
    public int[] readArrayOfInt32(@Nonnull String fieldName) {
        return getArrayOfInt32(fieldName);
    }

    @Nullable
    @Override
    public int[] readArrayOfInt32(@Nonnull String fieldName, @Nullable int[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT32) ? getArrayOfInt32(fieldName) : defaultValue;
    }

    @Override
    public long[] readArrayOfInt64(@Nonnull String fieldName) {
        return getArrayOfInt64(fieldName);
    }

    @Nullable
    @Override
    public long[] readArrayOfInt64(@Nonnull String fieldName, @Nullable long[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT64) ? getArrayOfInt64(fieldName) : defaultValue;
    }

    @Override
    public double[] readArrayOfFloat64(@Nonnull String fieldName) {
        return getArrayOfFloat64(fieldName);
    }

    @Nullable
    @Override
    public double[] readArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_FLOAT64) ? getArrayOfFloat64(fieldName) : defaultValue;
    }

    @Override
    public float[] readArrayOfFloat32(@Nonnull String fieldName) {
        return getArrayOfFloat32(fieldName);
    }

    @Nullable
    @Override
    public float[] readArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_FLOAT32) ? getArrayOfFloat32(fieldName) : defaultValue;
    }

    @Override
    public short[] readArrayOfInt16(@Nonnull String fieldName) {
        return getArrayOfInt16(fieldName);
    }

    @Nullable
    @Override
    public short[] readArrayOfInt16(@Nonnull String fieldName, @Nullable short[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_INT16) ? getArrayOfInt16(fieldName) : defaultValue;
    }

    @Override
    public String[] readArrayOfString(@Nonnull String fieldName) {
        return getArrayOfString(fieldName);
    }

    @Nullable
    @Override
    public String[] readArrayOfString(@Nonnull String fieldName, @Nullable String[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_STRING) ? getArrayOfString(fieldName) : defaultValue;
    }

    @Override
    public BigDecimal[] readArrayOfDecimal(@Nonnull String fieldName) {
        return getArrayOfDecimal(fieldName);
    }

    @Nullable
    @Override
    public BigDecimal[] readArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_DECIMAL) ? getArrayOfDecimal(fieldName) : defaultValue;
    }

    @Override
    public LocalTime[] readArrayOfTime(@Nonnull String fieldName) {
        return getArrayOfTime(fieldName);
    }

    @Nullable
    @Override
    public LocalTime[] readArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_TIME) ? getArrayOfTime(fieldName) : defaultValue;
    }

    @Override
    public LocalDate[] readArrayOfDate(@Nonnull String fieldName) {
        return getArrayOfDate(fieldName);
    }

    @Nullable
    @Override
    public LocalDate[] readArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_DATE) ? getArrayOfDate(fieldName) : defaultValue;
    }

    @Override
    public LocalDateTime[] readArrayOfTimestamp(@Nonnull String fieldName) {
        return getArrayOfTimestamp(fieldName);
    }

    @Nullable
    @Override
    public LocalDateTime[] readArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_TIMESTAMP) ? getArrayOfTimestamp(fieldName) : defaultValue;
    }

    @Override
    public OffsetDateTime[] readArrayOfTimestampWithTimezone(@Nonnull String fieldName) {
        return getArrayOfTimestampWithTimezone(fieldName);
    }

    @Nullable
    @Override
    public OffsetDateTime[] readArrayOfTimestampWithTimezone(@Nonnull String fieldName,
                                                             @Nullable OffsetDateTime[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONE)
                ? getArrayOfTimestampWithTimezone(fieldName) : defaultValue;
    }

    @Override
    public <T> T[] readArrayOfCompact(@Nonnull String fieldName, Class<T> componentType) {
        return getArrayOfObject(fieldName, componentType);
    }

    @Nullable
    @Override
    public <T> T[] readArrayOfCompact(@Nonnull String fieldName, @Nullable Class<T> componentType, @Nullable T[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_COMPACT) ? getArrayOfObject(fieldName, componentType) : defaultValue;
    }

    @Nullable
    @Override
    public Boolean readNullableBoolean(@Nonnull String fieldName) {
        return getNullableBoolean(fieldName);
    }

    @Nullable
    @Override
    public Boolean readNullableBoolean(@Nonnull String fieldName, @Nullable Boolean defaultValue) {
        return isFieldExists(fieldName, NULLABLE_BOOLEAN) ? getNullableBoolean(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Byte readNullableInt8(@Nonnull String fieldName) {
        return getNullableInt8(fieldName);
    }

    @Nullable
    @Override
    public Byte readNullableInt8(@Nonnull String fieldName, @Nullable Byte defaultValue) {
        return isFieldExists(fieldName, NULLABLE_INT8) ? getNullableInt8(fieldName) : defaultValue;
    }

    @Override
    public Short readNullableInt16(@Nonnull String fieldName) {
        return getNullableInt16(fieldName);
    }

    @Override
    public Short readNullableInt16(@Nonnull String fieldName, @Nullable Short defaultValue) {
        return isFieldExists(fieldName, NULLABLE_INT16) ? getNullableInt16(fieldName) : defaultValue;
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
    public Boolean[] readArrayOfNullableBoolean(@Nonnull String fieldName) {
        return getArrayOfNullableBoolean(fieldName);
    }

    @Nullable
    @Override
    public Boolean[] readArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_BOOLEAN) ? getArrayOfNullableBoolean(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Byte[] readArrayOfNullableInt8(@Nonnull String fieldName) {
        return getArrayOfNullableInt8(fieldName);
    }

    @Nullable
    @Override
    public Byte[] readArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT8) ? getArrayOfNullableInt8(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Short[] readArrayOfNullableInt16(@Nonnull String fieldName) {
        return getArrayOfNullableInt16(fieldName);
    }

    @Nullable
    @Override
    public Short[] readArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT16) ? getArrayOfNullableInt16(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Integer[] readArrayOfNullableInt32(@Nonnull String fieldName) {
        return getArrayOfNullableInt32(fieldName);
    }

    @Nullable
    @Override
    public Integer[] readArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT32) ? getArrayOfNullableInt32(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Long[] readArrayOfNullableInt64(@Nonnull String fieldName) {
        return getArrayOfNullableInt64(fieldName);
    }

    @Nullable
    @Override
    public Long[] readArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_INT64) ? getArrayOfNullableInt64(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Float[] readArrayOfNullableFloat32(@Nonnull String fieldName) {
        return getArrayOfNullableFloat32(fieldName);
    }

    @Nullable
    @Override
    public Float[] readArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_FLOAT32) ? getArrayOfNullableFloat32(fieldName) : defaultValue;
    }

    @Nullable
    @Override
    public Double[] readArrayOfNullableFloat64(@Nonnull String fieldName) {
        return getArrayOfNullableFloat64(fieldName);
    }

    @Nullable
    @Override
    public Double[] readArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] defaultValue) {
        return isFieldExists(fieldName, ARRAY_OF_NULLABLE_FLOAT64) ? getArrayOfNullableFloat64(fieldName) : defaultValue;
    }
}
