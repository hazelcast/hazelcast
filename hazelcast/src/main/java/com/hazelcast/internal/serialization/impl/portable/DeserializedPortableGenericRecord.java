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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldKind;
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
 * See the javadoc of {@link InternalGenericRecord} for GenericRecord class hierarchy.
 */
public class DeserializedPortableGenericRecord extends PortableGenericRecord {

    private final ClassDefinition classDefinition;
    private final Object[] objects;

    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public DeserializedPortableGenericRecord(ClassDefinition classDefinition, Object[] objects) {
        this.classDefinition = classDefinition;
        this.objects = objects;
    }

    @Override
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
    public GenericRecord[] getArrayOfGenericRecord(@Nonnull String fieldName) {
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
    public FieldKind getFieldKind(@Nonnull String fieldName) {
        return FieldTypeToFieldKind.toFieldKind(classDefinition.getFieldType(fieldName));
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BOOLEAN);
    }

    @Override
    public byte getInt8(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BYTE);
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        return get(fieldName, FieldType.CHAR);
    }

    @Override
    public double getFloat64(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DOUBLE);
    }

    @Override
    public float getFloat32(@Nonnull String fieldName) {
        return get(fieldName, FieldType.FLOAT);
    }

    @Override
    public int getInt32(@Nonnull String fieldName) {
        return get(fieldName, FieldType.INT);
    }

    @Override
    public long getInt64(@Nonnull String fieldName) {
        return get(fieldName, FieldType.LONG);
    }

    @Override
    public short getInt16(@Nonnull String fieldName) {
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

    @Nullable
    @Override
    public LocalTime getTime(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIME);
    }

    @Nullable
    @Override
    public LocalDate getDate(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DATE);
    }

    @Nullable
    @Override
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP);
    }

    @Nullable
    @Override
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE);
    }

    @Override
    @Nullable
    public boolean[] getArrayOfBoolean(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    @Nullable
    public byte[] getArrayOfInt8(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BYTE_ARRAY);
    }

    @Override
    @Nullable
    public char[] getArrayOfChar(@Nonnull String fieldName) {
        return get(fieldName, FieldType.CHAR_ARRAY);
    }

    @Override
    @Nullable
    public double[] getArrayOfFloat64(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DOUBLE_ARRAY);
    }

    @Override
    @Nullable
    public float[] getArrayOfFloat32(@Nonnull String fieldName) {
        return get(fieldName, FieldType.FLOAT_ARRAY);
    }

    @Override
    @Nullable
    public int[] getArrayOfInt32(@Nonnull String fieldName) {
        return get(fieldName, FieldType.INT_ARRAY);
    }

    @Override
    @Nullable
    public long[] getArrayOfInt64(@Nonnull String fieldName) {
        return get(fieldName, FieldType.LONG_ARRAY);
    }

    @Override
    @Nullable
    public short[] getArrayOfInt16(@Nonnull String fieldName) {
        return get(fieldName, FieldType.SHORT_ARRAY);
    }

    @Override
    @Nullable
    public String[] getArrayOfString(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UTF_ARRAY);
    }

    @Override
    @Nullable
    public BigDecimal[] getArrayOfDecimal(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DECIMAL_ARRAY);
    }

    @Override
    @Nullable
    public LocalTime[] getArrayOfTime(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIME_ARRAY);
    }

    @Override
    @Nullable
    public LocalDate[] getArrayOfDate(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DATE_ARRAY);
    }

    @Override
    @Nullable
    public LocalDateTime[] getArrayOfTimestamp(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_ARRAY);
    }

    @Override
    @Nullable
    public OffsetDateTime[] getArrayOfTimestampWithTimezone(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    @Override
    protected Object getClassIdentifier() {
        return classDefinition;
    }

    @Nullable
    @Override
    public InternalGenericRecord getInternalGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, FieldType.PORTABLE);
    }

    @Nullable
    @Override
    public InternalGenericRecord[] getArrayOfInternalGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, FieldType.PORTABLE_ARRAY);
    }

    @Nullable
    @Override
    public Boolean getBooleanFromArray(@Nonnull String fieldName, int index) {
        boolean[] array = getArrayOfBoolean(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Byte getInt8FromArray(@Nonnull String fieldName, int index) {
        byte[] array = getArrayOfInt8(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Character getCharFromArray(@Nonnull String fieldName, int index) {
        char[] array = getArrayOfChar(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Double getFloat64FromArray(@Nonnull String fieldName, int index) {
        double[] array = getArrayOfFloat64(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Float getFloat32FromArray(@Nonnull String fieldName, int index) {
        float[] array = getArrayOfFloat32(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Integer getInt32FromArray(@Nonnull String fieldName, int index) {
        int[] array = getArrayOfInt32(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Long getInt64FromArray(@Nonnull String fieldName, int index) {
        long[] array = getArrayOfInt64(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Short getInt16FromArray(@Nonnull String fieldName, int index) {
        short[] array = getArrayOfInt16(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public String getStringFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfString(fieldName), index);
    }

    @Nullable
    @Override
    public GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfGenericRecord(fieldName), index);
    }

    @Nullable
    @Override
    public InternalGenericRecord getInternalGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfInternalGenericRecord(fieldName), index);
    }

    @Nullable
    @Override
    public Object getObjectFromArray(@Nonnull String fieldName, int index) {
        return getGenericRecordFromArray(fieldName, index);
    }

    @Nullable
    @Override
    public <T> T[] getArrayOfObject(@Nonnull String fieldName, Class<T> componentType) {
        return (T[]) getArrayOfGenericRecord(fieldName);
    }

    @Nullable
    @Override
    public Object getObject(@Nonnull String fieldName) {
        return getGenericRecord(fieldName);
    }

    @Nullable
    @Override
    public BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfDecimal(fieldName), index);
    }

    @Nullable
    @Override
    public LocalTime getTimeFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfTime(fieldName), index);
    }

    @Nullable
    @Override
    public LocalDate getDateFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfDate(fieldName), index);
    }

    @Nullable
    @Override
    public LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfTimestamp(fieldName), index);
    }

    @Nullable
    @Override
    public OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfTimestampWithTimezone(fieldName), index);
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
