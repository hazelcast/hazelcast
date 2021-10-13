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

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Abstract class that implements most of the methods of the {@link GenericRecordBuilder}
 * interface. It leaves the responsibility of building the actual {@link GenericRecord}
 * to the child classes.
 */
abstract class AbstractGenericRecordBuilder implements GenericRecordBuilder {

    @Nonnull
    @Override
    public GenericRecordBuilder setInt(@Nonnull String fieldName, int value) {
        return write(fieldName, value, FieldKind.INT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        return write(fieldName, value, FieldKind.LONG);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setString(@Nonnull String fieldName, @Nullable String value) {
        return write(fieldName, value, FieldKind.STRING);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        return write(fieldName, value, FieldKind.BOOLEAN);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        return write(fieldName, value, FieldKind.BYTE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        return write(fieldName, value, FieldKind.CHAR);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        return write(fieldName, value, FieldKind.DOUBLE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        return write(fieldName, value, FieldKind.FLOAT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        return write(fieldName, value, FieldKind.SHORT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value) {
        return write(fieldName, value, FieldKind.COMPACT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        return write(fieldName, value, FieldKind.DECIMAL);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        return write(fieldName, value, FieldKind.TIME);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        return write(fieldName, value, FieldKind.DATE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        return write(fieldName, value, FieldKind.TIMESTAMP);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        return write(fieldName, value, FieldKind.TIMESTAMP_WITH_TIMEZONE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setGenericRecordArray(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        return write(fieldName, value, FieldKind.COMPACT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setByteArray(@Nonnull String fieldName, @Nullable byte[] value) {
        return write(fieldName, value, FieldKind.BYTE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setBooleanArray(@Nonnull String fieldName, @Nullable boolean[] value) {
        return write(fieldName, value, FieldKind.BOOLEAN_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setCharArray(@Nonnull String fieldName, @Nullable char[] value) {
        return write(fieldName, value, FieldKind.CHAR_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setIntArray(@Nonnull String fieldName, @Nullable int[] value) {
        return write(fieldName, value, FieldKind.INT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setLongArray(@Nonnull String fieldName, @Nullable long[] value) {
        return write(fieldName, value, FieldKind.LONG_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDoubleArray(@Nonnull String fieldName, @Nullable double[] value) {
        return write(fieldName, value, FieldKind.DOUBLE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloatArray(@Nonnull String fieldName, @Nullable float[] value) {
        return write(fieldName, value, FieldKind.FLOAT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setShortArray(@Nonnull String fieldName, @Nullable short[] value) {
        return write(fieldName, value, FieldKind.SHORT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setStringArray(@Nonnull String fieldName, @Nullable String[] value) {
        return write(fieldName, value, FieldKind.STRING_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        return write(fieldName, value, FieldKind.DECIMAL_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        return write(fieldName, value, FieldKind.TIME_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDateArray(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        return write(fieldName, value, FieldKind.DATE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        return write(fieldName, value, FieldKind.TIMESTAMP_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        return write(fieldName, value, FieldKind.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        return write(fieldName, value, FieldKind.NULLABLE_BOOLEAN);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableByte(@Nonnull String fieldName, @Nullable Byte value) {
        return write(fieldName, value, FieldKind.NULLABLE_BYTE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableDouble(@Nonnull String fieldName, @Nullable Double value) {
        return write(fieldName, value, FieldKind.NULLABLE_DOUBLE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat(@Nonnull String fieldName, @Nullable Float value) {
        return write(fieldName, value, FieldKind.NULLABLE_FLOAT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt(@Nonnull String fieldName, @Nullable Integer value) {
        return write(fieldName, value, FieldKind.NULLABLE_INT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableLong(@Nonnull String fieldName, @Nullable Long value) {
        return write(fieldName, value, FieldKind.NULLABLE_LONG);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableShort(@Nonnull String fieldName, @Nullable Short value) {
        return write(fieldName, value, FieldKind.NULLABLE_SHORT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableBooleanArray(@Nonnull String fieldName, @Nullable Boolean[] value) {
        return write(fieldName, value, FieldKind.NULLABLE_BOOLEAN_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableByteArray(@Nonnull String fieldName, @Nullable Byte[] value) {
        return write(fieldName, value, FieldKind.NULLABLE_BYTE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloatArray(@Nonnull String fieldName, @Nullable Float[] value) {
        return write(fieldName, value, FieldKind.NULLABLE_FLOAT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableIntArray(@Nonnull String fieldName, @Nullable Integer[] value) {
        return write(fieldName, value, FieldKind.NULLABLE_INT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableDoubleArray(@Nonnull String fieldName, @Nullable Double[] value) {
        return write(fieldName, value, FieldKind.NULLABLE_DOUBLE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableLongArray(@Nonnull String fieldName, @Nullable Long[] value) {
        return write(fieldName, value, FieldKind.NULLABLE_LONG_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableShortArray(@Nonnull String fieldName, @Nullable Short[] value) {
        return write(fieldName, value, FieldKind.NULLABLE_SHORT_ARRAY);
    }

    protected abstract GenericRecordBuilder write(@Nonnull String fieldName, Object value, FieldKind fieldType);

    public static void checkTypeWithSchema(Schema schema, @Nonnull String fieldName, FieldKind fieldKind) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + "' for " + schema);
        }
        if (fd.getKind() != fieldKind) {
            throw new HazelcastSerializationException("Invalid field kind: '" + fieldName
                    + "' for " + schema + ", expected : " + fd.getKind() + ", given : " + fieldKind);
        }
    }
}
