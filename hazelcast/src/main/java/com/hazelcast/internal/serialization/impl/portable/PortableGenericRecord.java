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

import com.hazelcast.nio.serialization.AbstractGenericRecord;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;

/**
 * This class is only instantiated by the user API's on the GenericRecordBuilder
 */
public class PortableGenericRecord extends AbstractGenericRecord implements GenericRecord {

    private final ClassDefinition classDefinition;
    private final Object[] objects;

    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public PortableGenericRecord(ClassDefinition classDefinition, Object[] objects) {
        this.classDefinition = classDefinition;
        this.objects = objects;
    }

    public ClassDefinition getClassDefinition() {
        return classDefinition;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder newBuilder() {
        return GenericRecordBuilder.portable(classDefinition);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder cloneWithBuilder() {
        return new PortableGenericRecordBuilder(classDefinition, Arrays.copyOf(objects, objects.length));
    }

    @Nonnull
    @Override
    public Set<String> getFieldNames() {
        return classDefinition.getFieldNames();
    }

    @Override
    public GenericRecord[] getGenericRecordArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.PORTABLE_ARRAY);
    }

    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, FieldType.PORTABLE);
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return classDefinition.hasField(fieldName);
    }

    @Override
    @Nonnull
    public FieldType getFieldType(@Nonnull String fieldName) {
        return classDefinition.getFieldType(fieldName);
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BOOLEAN);
    }

    @Override
    public byte getByte(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BYTE);
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        return get(fieldName, FieldType.CHAR);
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DOUBLE);
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        return get(fieldName, FieldType.FLOAT);
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        return get(fieldName, FieldType.INT);
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        return get(fieldName, FieldType.LONG);
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        return get(fieldName, FieldType.SHORT);
    }

    @Override
    @Nullable
    public String getString(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UTF);
    }

    @Override
    @Nullable
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DECIMAL);
    }

    @Override
    @Nullable
    public LocalTime getTime(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIME);
    }

    @Override
    @Nullable
    public LocalDate getDate(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DATE);
    }

    @Override
    @Nullable
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP);
    }

    @Override
    @Nullable
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE);
    }

    @Override
    @Nullable
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    @Nullable
    public byte[] getByteArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BYTE_ARRAY);
    }

    @Override
    @Nullable
    public char[] getCharArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.CHAR_ARRAY);
    }

    @Override
    @Nullable
    public double[] getDoubleArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DOUBLE_ARRAY);
    }

    @Override
    @Nullable
    public float[] getFloatArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.FLOAT_ARRAY);
    }

    @Override
    @Nullable
    public int[] getIntArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.INT_ARRAY);
    }

    @Override
    @Nullable
    public long[] getLongArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.LONG_ARRAY);
    }

    @Override
    @Nullable
    public short[] getShortArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.SHORT_ARRAY);
    }

    @Override
    @Nullable
    public String[] getStringArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UTF_ARRAY);
    }

    @Override
    @Nullable
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DECIMAL_ARRAY);
    }

    @Override
    @Nullable
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIME_ARRAY);
    }

    @Override
    @Nullable
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DATE_ARRAY);
    }

    @Override
    @Nullable
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_ARRAY);
    }

    @Override
    @Nullable
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    @Override
    protected Object getClassIdentifier() {
        return classDefinition;
    }

    @Override
    public String toString() {
        return "PortableGenericRecord:" + super.toString();
    }

    private <T> T get(@Nonnull String fieldName, FieldType fieldType) {
        FieldDefinition fd = check(fieldName, fieldType);
        return (T) objects[fd.getIndex()];
    }

    @Nonnull
    private FieldDefinition check(@Nonnull String fieldName, FieldType fieldType) {
        FieldDefinition fd = classDefinition.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: "
                    + classDefinition.getVersion() + "}");
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: "
                    + classDefinition.getVersion() + "}" + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
        return fd;
    }

    private <T> T getFromArray(T[] array, int index) {
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }
}
