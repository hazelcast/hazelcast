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
        throw new UnsupportedOperationException("Compact format does not support writing a char field");
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
    public GenericRecordBuilder setArrayOfGenericRecords(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_COMPACTS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfBytes(@Nonnull String fieldName, @Nullable byte[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_BYTES);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfBooleans(@Nonnull String fieldName, @Nullable boolean[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_BOOLEANS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfChars(@Nonnull String fieldName, @Nullable char[] value) {
        throw new UnsupportedOperationException("Compact format does not support writing an array of chars field");
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfInts(@Nonnull String fieldName, @Nullable int[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_INTS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfLongs(@Nonnull String fieldName, @Nullable long[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_LONGS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfDoubles(@Nonnull String fieldName, @Nullable double[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_DOUBLES);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfFloats(@Nonnull String fieldName, @Nullable float[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_FLOATS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfShorts(@Nonnull String fieldName, @Nullable short[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_SHORTS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfStrings(@Nonnull String fieldName, @Nullable String[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_STRINGS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfDecimals(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_DECIMALS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfTimes(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_TIMES);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfDates(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_DATES);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfTimestamps(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_TIMESTAMPS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfTimestampWithTimezones(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES);
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
    public GenericRecordBuilder setArrayOfNullableBooleans(@Nonnull String fieldName, @Nullable Boolean[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_BOOLEANS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBytes(@Nonnull String fieldName, @Nullable Byte[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_BYTES);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloats(@Nonnull String fieldName, @Nullable Float[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_FLOATS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInts(@Nonnull String fieldName, @Nullable Integer[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_INTS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableDoubles(@Nonnull String fieldName, @Nullable Double[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_DOUBLES);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableLongs(@Nonnull String fieldName, @Nullable Long[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_LONGS);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableShorts(@Nonnull String fieldName, @Nullable Short[] value) {
        return write(fieldName, value, FieldKind.ARRAY_OF_NULLABLE_SHORTS);
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
