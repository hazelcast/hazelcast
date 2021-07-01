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

import com.hazelcast.nio.serialization.compact.CompactRecord;
import com.hazelcast.nio.serialization.compact.CompactRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.TypeID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

abstract class AbstractCompactRecordBuilder implements CompactRecordBuilder {

    @Nonnull
    @Override
    public CompactRecordBuilder setInt(@Nonnull String fieldName, int value) {
        return write(fieldName, value, TypeID.INT);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setLong(@Nonnull String fieldName, long value) {
        return write(fieldName, value, TypeID.LONG);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setString(@Nonnull String fieldName, String value) {
        return write(fieldName, value, TypeID.STRING);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        return write(fieldName, value, TypeID.BOOLEAN);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        return write(fieldName, value, TypeID.BYTE);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setChar(@Nonnull String fieldName, char value) {
        return write(fieldName, value, TypeID.CHAR);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        return write(fieldName, value, TypeID.DOUBLE);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        return write(fieldName, value, TypeID.FLOAT);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setShort(@Nonnull String fieldName, short value) {
        return write(fieldName, value, TypeID.SHORT);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setCompactRecord(@Nonnull String fieldName, @Nullable CompactRecord value) {
        return write(fieldName, value, TypeID.COMPOSED);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        return write(fieldName, value, TypeID.DECIMAL);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        return write(fieldName, value, TypeID.TIME);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        return write(fieldName, value, TypeID.DATE);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        return write(fieldName, value, TypeID.TIMESTAMP);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        return write(fieldName, value, TypeID.TIMESTAMP_WITH_TIMEZONE);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setCompactRecordArray(@Nonnull String fieldName, @Nullable CompactRecord[] value) {
        return write(fieldName, value, TypeID.COMPOSED_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        return write(fieldName, value, TypeID.BYTE_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        return write(fieldName, value, TypeID.BOOLEAN_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        return write(fieldName, value, TypeID.CHAR_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        return write(fieldName, value, TypeID.INT_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        return write(fieldName, value, TypeID.LONG_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        return write(fieldName, value, TypeID.DOUBLE_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        return write(fieldName, value, TypeID.FLOAT_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        return write(fieldName, value, TypeID.SHORT_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        return write(fieldName, value, TypeID.STRING_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        return write(fieldName, value, TypeID.DECIMAL_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        return write(fieldName, value, TypeID.TIME_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        return write(fieldName, value, TypeID.DATE_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        return write(fieldName, value, TypeID.TIMESTAMP_ARRAY);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        return write(fieldName, value, TypeID.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    protected abstract CompactRecordBuilder write(@Nonnull String fieldName, Object value, TypeID typeID);

    public static void checkTypeWithSchema(Schema schema, @Nonnull String fieldName, TypeID fieldType) {
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
