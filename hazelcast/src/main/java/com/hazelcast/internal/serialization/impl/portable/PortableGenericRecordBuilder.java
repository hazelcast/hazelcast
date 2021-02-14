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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
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

public class PortableGenericRecordBuilder implements GenericRecordBuilder {

    private final ClassDefinition classDefinition;
    private final Object[] objects;
    private final boolean[] isSet;
    private final boolean isClone;

    public PortableGenericRecordBuilder(ClassDefinition classDefinition) {
        this.classDefinition = classDefinition;
        this.objects = new Object[classDefinition.getFieldCount()];
        this.isClone = false;
        this.isSet = new boolean[objects.length];
    }

    PortableGenericRecordBuilder(ClassDefinition classDefinition, Object[] objects) {
        this.classDefinition = classDefinition;
        this.objects = objects;
        this.isClone = true;
        this.isSet = new boolean[objects.length];
    }

    /**
     * @return newly created GenericRecord
     * @throws HazelcastSerializationException if a field is not written when building with builder from
     *                                         {@link GenericRecordBuilder#portable(ClassDefinition)} and
     *                                         {@link GenericRecord#newBuilder()}
     */
    @Nonnull
    @Override
    public GenericRecord build() {
        if (!isClone) {
            for (int i = 0; i < isSet.length; i++) {
                if (!isSet[i]) {
                    throw new HazelcastSerializationException("All fields must be written when building"
                            + " a GenericRecord for portable, unwritten field :" + classDefinition.getField(i));
                }
            }
        }
        return new PortableGenericRecord(classDefinition, objects);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setInt(@Nonnull String fieldName, int value) {
        return set(fieldName, value, FieldType.INT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        return set(fieldName, value, FieldType.LONG);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setString(@Nonnull String fieldName, String value) {
        return set(fieldName, value, FieldType.UTF);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        return set(fieldName, value, FieldType.BOOLEAN);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        return set(fieldName, value, FieldType.BYTE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        return set(fieldName, value, FieldType.CHAR);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        return set(fieldName, value, FieldType.DOUBLE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        return set(fieldName, value, FieldType.FLOAT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        return set(fieldName, value, FieldType.SHORT);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value) {
        return set(fieldName, value, FieldType.PORTABLE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        return set(fieldName, value, FieldType.DECIMAL);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        return set(fieldName, value, FieldType.TIME);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        return set(fieldName, value, FieldType.DATE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        return set(fieldName, value, FieldType.TIMESTAMP);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        return set(fieldName, value, FieldType.TIMESTAMP_WITH_TIMEZONE);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setGenericRecordArray(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        return set(fieldName, value, FieldType.PORTABLE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        return set(fieldName, value, FieldType.BYTE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        return set(fieldName, value, FieldType.BOOLEAN_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        return set(fieldName, value, FieldType.CHAR_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        return set(fieldName, value, FieldType.INT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        return set(fieldName, value, FieldType.LONG_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        return set(fieldName, value, FieldType.DOUBLE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        return set(fieldName, value, FieldType.FLOAT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        return set(fieldName, value, FieldType.SHORT_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        return set(fieldName, value, FieldType.UTF_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        return set(fieldName, value, FieldType.DECIMAL_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        return set(fieldName, value, FieldType.TIME_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        return set(fieldName, value, FieldType.DATE_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        return set(fieldName, value, FieldType.TIMESTAMP_ARRAY);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        return set(fieldName, value, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    private GenericRecordBuilder set(@Nonnull String fieldName, Object value, FieldType fieldType) {
        FieldDefinition fd = check(fieldName, fieldType);
        int index = fd.getIndex();
        if (isSet[index]) {
            if (!isClone) {
                throw new HazelcastSerializationException("It is illegal to the overwrite the field");
            } else {
                throw new HazelcastSerializationException("Field can only overwritten once with `cloneWithBuilder`");
            }
        }
        objects[index] = value;
        isSet[index] = true;
        return this;
    }

    @Nonnull
    private FieldDefinition check(@Nonnull String fieldName, FieldType fieldType) {
        FieldDefinition fd = classDefinition.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId()
                    + ", version: " + classDefinition.getVersion() + "}");
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: "
                    + classDefinition.getVersion() + "}" + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
        return fd;
    }

}
