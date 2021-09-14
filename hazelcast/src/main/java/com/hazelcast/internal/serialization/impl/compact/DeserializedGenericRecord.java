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
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.TreeMap;

public class DeserializedGenericRecord extends CompactGenericRecord {

    private final TreeMap<String, Object> objects;
    private final Schema schema;

    public DeserializedGenericRecord(Schema schema, TreeMap<String, Object> objects) {
        this.schema = schema;
        this.objects = objects;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder newBuilder() {
        return new DeserializedSchemaBoundGenericRecordBuilder(schema);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder cloneWithBuilder() {
        return new DeserializedGenericRecordCloner(schema, objects);
    }

    @Nonnull
    @Override
    public FieldType getFieldType(@Nonnull String fieldName) {
        return schema.getField(fieldName).getType();
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return objects.containsKey(fieldName);
    }

    @Override
    @Nonnull
    public Set<String> getFieldNames() {
        return objects.keySet();
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
    public int getUnsignedByte(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UNSIGNED_BYTE);
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
    public long getUnsignedInt(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UNSIGNED_INT);
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        return get(fieldName, FieldType.LONG);
    }

    @Override
    public BigInteger getUnsignedLong(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UNSIGNED_LONG);    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        return get(fieldName, FieldType.SHORT);
    }

    @Override
    public int getUnsignedShort(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UNSIGNED_SHORT);    }

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

    @Nullable
    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, FieldType.COMPOSED);
    }

    @Override
    @Nullable
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    public byte[] getByteArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.BYTE_ARRAY);
    }

    @Nullable
    @Override
    public int[] getUnsignedByteArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UNSIGNED_BYTE_ARRAY);
    }

    @Override
    public char[] getCharArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.CHAR_ARRAY);
    }

    @Override
    public double[] getDoubleArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DOUBLE_ARRAY);
    }

    @Override
    public float[] getFloatArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.FLOAT_ARRAY);
    }

    @Override
    public int[] getIntArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.INT_ARRAY);
    }

    @Nullable
    @Override
    public long[] getUnsignedIntArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UNSIGNED_INT_ARRAY);    }

    @Override
    public long[] getLongArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.LONG_ARRAY);
    }

    @Nullable
    @Override
    public BigInteger[] getUnsignedLongArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UNSIGNED_LONG_ARRAY);    }

    @Override
    public short[] getShortArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.SHORT_ARRAY);
    }

    @Nullable
    @Override
    public int[] getUnsignedShortArray(@Nonnull String fieldName) {
        return new int[0];
    }

    @Override
    public String[] getStringArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.UTF_ARRAY);
    }

    @Override
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DECIMAL_ARRAY);
    }

    @Override
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIME_ARRAY);
    }

    @Override
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.DATE_ARRAY);
    }

    @Override
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_ARRAY);
    }

    @Override
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    @Nullable
    @Override
    public GenericRecord[] getGenericRecordArray(@Nonnull String fieldName) {
        return get(fieldName, FieldType.COMPOSED_ARRAY);
    }

    private <T> T get(@Nonnull String fieldName, FieldType fieldType) {
        check(fieldName, fieldType);
        return (T) objects.get(fieldName);
    }

    private void check(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + " for " + schema);
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName + " for " + schema
                    + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
    }

    @Override
    protected Object getClassIdentifier() {
        return schema.getTypeName();
    }
}
