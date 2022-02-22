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
    public GenericRecordBuilder setInt32(@Nonnull String fieldName, int value) {
        return write(fieldName, value, FieldKind.INT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setInt64(@Nonnull String fieldName, long value) {
        return write(fieldName, value, FieldKind.INT64);
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
    public GenericRecordBuilder setInt8(@Nonnull String fieldName, byte value) {
        return write(fieldName, value, FieldKind.INT8);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        throw new UnsupportedOperationException("Compact format does not support writing a char field");
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloat64(@Nonnull String fieldName, double value) {
        return write(fieldName, value, FieldKind.FLOAT64);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloat32(@Nonnull String fieldName, float value) {
        return write(fieldName, value, FieldKind.FLOAT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setInt16(@Nonnull String fieldName, short value) {
        return write(fieldName, value, FieldKind.INT16);
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
    public GenericRecordBuilder setArrayOfGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_COMPACT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_INT8);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_BOOLEAN);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfChar(@Nonnull String fieldName, @Nullable char[] value) {
        throw new UnsupportedOperationException("Compact format does not support writing an array of chars field");
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfInt32(@Nonnull String fieldName, @Nullable int[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_INT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfInt64(@Nonnull String fieldName, @Nullable long[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_INT64);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_FLOAT64);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_FLOAT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfInt16(@Nonnull String fieldName, @Nullable short[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_INT16);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfString(@Nonnull String fieldName, @Nullable String[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_STRING);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_DECIMAL);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_TIME);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_DATE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_TIMESTAMP);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        return write(fieldName, value, FieldKind.NULLABLE_BOOLEAN);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt8(@Nonnull String fieldName, @Nullable Byte value) {
        return write(fieldName, value, FieldKind.NULLABLE_INT8);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat64(@Nonnull String fieldName, @Nullable Double value) {
        return write(fieldName, value, FieldKind.NULLABLE_FLOAT64);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat32(@Nonnull String fieldName, @Nullable Float value) {
        return write(fieldName, value, FieldKind.NULLABLE_FLOAT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt32(@Nonnull String fieldName, @Nullable Integer value) {
        return write(fieldName, value, FieldKind.NULLABLE_INT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt64(@Nonnull String fieldName, @Nullable Long value) {
        return write(fieldName, value, FieldKind.NULLABLE_INT64);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt16(@Nonnull String fieldName, @Nullable Short value) {
        return write(fieldName, value, FieldKind.NULLABLE_INT16);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_INT8);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_FLOAT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_INT32);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_FLOAT64);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_INT64);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_INT16);
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
