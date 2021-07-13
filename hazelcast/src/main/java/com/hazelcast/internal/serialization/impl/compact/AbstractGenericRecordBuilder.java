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

import com.hazelcast.nio.serialization.FieldType;
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
        return write(fieldName, value, FieldType.INT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        return write(fieldName, value, FieldType.LONG);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setString(@Nonnull String fieldName, String value) {
        return write(fieldName, value, FieldType.UTF);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        return write(fieldName, value, FieldType.BOOLEAN);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        return write(fieldName, value, FieldType.BYTE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        return write(fieldName, value, FieldType.CHAR);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        return write(fieldName, value, FieldType.DOUBLE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        return write(fieldName, value, FieldType.FLOAT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        return write(fieldName, value, FieldType.SHORT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value) {
        return write(fieldName, value, FieldType.COMPOSED);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        return write(fieldName, value, FieldType.DECIMAL);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        return write(fieldName, value, FieldType.TIME);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        return write(fieldName, value, FieldType.DATE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        return write(fieldName, value, FieldType.TIMESTAMP);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        return write(fieldName, value, FieldType.TIMESTAMP_WITH_TIMEZONE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setGenericRecordArray(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        return write(fieldName, value, FieldType.COMPOSED_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        return write(fieldName, value, FieldType.BYTE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        return write(fieldName, value, FieldType.BOOLEAN_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        return write(fieldName, value, FieldType.CHAR_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        return write(fieldName, value, FieldType.INT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        return write(fieldName, value, FieldType.LONG_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        return write(fieldName, value, FieldType.DOUBLE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        return write(fieldName, value, FieldType.FLOAT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        return write(fieldName, value, FieldType.SHORT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        return write(fieldName, value, FieldType.UTF_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        return write(fieldName, value, FieldType.DECIMAL_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        return write(fieldName, value, FieldType.TIME_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        return write(fieldName, value, FieldType.DATE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        return write(fieldName, value, FieldType.TIMESTAMP_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        return write(fieldName, value, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    protected abstract GenericRecordBuilder write(@Nonnull String fieldName, Object value, FieldType fieldType);

    public static void checkTypeWithSchema(Schema schema, @Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + "' for " + schema);
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName
                    + "' for " + schema + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
    }

}
