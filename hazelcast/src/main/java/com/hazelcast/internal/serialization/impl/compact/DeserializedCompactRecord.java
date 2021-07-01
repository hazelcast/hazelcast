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
import java.util.Set;
import java.util.TreeMap;

public class DeserializedCompactRecord extends AbstractCompactRecord {

    private final TreeMap<String, Object> objects;
    private final Schema schema;

    public DeserializedCompactRecord(Schema schema, TreeMap<String, Object> objects) {
        this.schema = schema;
        this.objects = objects;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Nonnull
    @Override
    public CompactRecordBuilder newBuilder() {
        return new DeserializedSchemaBoundCompactRecordBuilder(schema);
    }

    @Nonnull
    @Override
    public CompactRecordBuilder cloneWithBuilder() {
        return new DeserializedCompactRecordCloner(schema, objects);
    }

    @Nonnull
    @Override
    public TypeID getFieldType(@Nonnull String fieldName) {
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
        return get(fieldName, TypeID.BOOLEAN);
    }

    @Override
    public byte getByte(@Nonnull String fieldName) {
        return get(fieldName, TypeID.BYTE);
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        return get(fieldName, TypeID.CHAR);
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        return get(fieldName, TypeID.DOUBLE);
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        return get(fieldName, TypeID.FLOAT);
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        return get(fieldName, TypeID.INT);
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        return get(fieldName, TypeID.LONG);
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        return get(fieldName, TypeID.SHORT);
    }

    @Override
    @Nullable
    public String getString(@Nonnull String fieldName) {
        return get(fieldName, TypeID.STRING);
    }

    @Override
    @Nullable
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        return get(fieldName, TypeID.DECIMAL);
    }

    @Override
    @Nullable
    public LocalTime getTime(@Nonnull String fieldName) {
        return get(fieldName, TypeID.TIME);
    }

    @Override
    @Nullable
    public LocalDate getDate(@Nonnull String fieldName) {
        return get(fieldName, TypeID.DATE);
    }

    @Override
    @Nullable
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        return get(fieldName, TypeID.TIMESTAMP);
    }

    @Override
    @Nullable
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        return get(fieldName, TypeID.TIMESTAMP_WITH_TIMEZONE);
    }

    @Nullable
    @Override
    public CompactRecord getCompactRecord(@Nonnull String fieldName) {
        return get(fieldName, TypeID.COMPOSED);
    }

    @Override
    @Nullable
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.BOOLEAN_ARRAY);
    }

    @Override
    public byte[] getByteArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.BYTE_ARRAY);
    }

    @Override
    public char[] getCharArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.CHAR_ARRAY);
    }

    @Override
    public double[] getDoubleArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.DOUBLE_ARRAY);
    }

    @Override
    public float[] getFloatArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.FLOAT_ARRAY);
    }

    @Override
    public int[] getIntArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.INT_ARRAY);
    }

    @Override
    public long[] getLongArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.LONG_ARRAY);
    }

    @Override
    public short[] getShortArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.SHORT_ARRAY);
    }

    @Override
    public String[] getStringArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.STRING_ARRAY);
    }

    @Override
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.DECIMAL_ARRAY);
    }

    @Override
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.TIME_ARRAY);
    }

    @Override
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.DATE_ARRAY);
    }

    @Override
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.TIMESTAMP_ARRAY);
    }

    @Override
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    @Nullable
    @Override
    public CompactRecord[] getCompactRecordArray(@Nonnull String fieldName) {
        return get(fieldName, TypeID.COMPOSED_ARRAY);
    }


    private <T> T get(@Nonnull String fieldName, TypeID fieldType) {
        check(fieldName, fieldType);
        return (T) objects.get(fieldName);
    }

    private void check(@Nonnull String fieldName, TypeID fieldType) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + " for " + schema);
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName + " for " + schema
                    + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
    }

}
