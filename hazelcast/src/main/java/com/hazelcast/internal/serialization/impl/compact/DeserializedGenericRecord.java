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
    public FieldKind getFieldKind(@Nonnull String fieldName) {
        return schema.getField(fieldName).getKind();
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
        return get(fieldName, FieldKind.BOOLEAN);
    }

    @Override
    public byte getByte(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.BYTE);
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.CHAR);
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.DOUBLE);
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.FLOAT);
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.INT);
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.LONG);
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.SHORT);
    }

    @Override
    @Nullable
    public String getString(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.STRING);
    }

    @Override
    @Nullable
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.DECIMAL);
    }

    @Nullable
    @Override
    public LocalTime getTime(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.TIME);
    }

    @Nullable
    @Override
    public LocalDate getDate(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.DATE);
    }

    @Nullable
    @Override
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.TIMESTAMP);
    }

    @Nullable
    @Override
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.TIMESTAMP_WITH_TIMEZONE);
    }

    @Nullable
    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.COMPACT);
    }

    @Override
    @Nullable
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.BOOLEAN_ARRAY);
    }

    @Override
    @Nullable
    public byte[] getByteArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.BYTE_ARRAY);
    }

    @Override
    @Nullable
    public char[] getCharArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.CHAR_ARRAY);
    }

    @Override
    @Nullable
    public double[] getDoubleArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.DOUBLE_ARRAY);
    }

    @Override
    @Nullable
    public float[] getFloatArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.FLOAT_ARRAY);
    }

    @Override
    @Nullable
    public int[] getIntArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.INT_ARRAY);
    }

    @Override
    @Nullable
    public long[] getLongArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.LONG_ARRAY);
    }

    @Override
    @Nullable
    public short[] getShortArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.SHORT_ARRAY);
    }

    @Override
    @Nullable
    public String[] getStringArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.STRING_ARRAY);
    }

    @Override
    @Nullable
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.DECIMAL_ARRAY);
    }

    @Override
    @Nullable
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.TIME_ARRAY);
    }

    @Override
    @Nullable
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.DATE_ARRAY);
    }

    @Override
    @Nullable
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.TIMESTAMP_ARRAY);
    }

    @Override
    @Nullable
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    @Nullable
    @Override
    public GenericRecord[] getGenericRecordArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.COMPACT_ARRAY);
    }

    @Nullable
    @Override
    public Boolean getNullableBoolean(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_BOOLEAN);
    }

    @Nullable
    @Override
    public Byte getNullableByte(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_BYTE);
    }

    @Nullable
    @Override
    public Double getNullableDouble(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_DOUBLE);
    }

    @Nullable
    @Override
    public Float getNullableFloat(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_FLOAT);
    }

    @Nullable
    @Override
    public Integer getNullableInt(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_INT);
    }

    @Nullable
    @Override
    public Long getNullableLong(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_LONG);
    }

    @Nullable
    @Override
    public Short getNullableShort(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_SHORT);
    }

    @Nullable
    @Override
    public Boolean[] getNullableBooleanArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_BOOLEAN_ARRAY);
    }

    @Nullable
    @Override
    public Byte[] getNullableByteArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_BYTE_ARRAY);
    }

    @Nullable
    @Override
    public Double[] getNullableDoubleArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_DOUBLE_ARRAY);
    }

    @Nullable
    @Override
    public Float[] getNullableFloatArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_FLOAT_ARRAY);
    }

    @Nullable
    @Override
    public Integer[] getNullableIntArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_INT_ARRAY);
    }

    @Nullable
    @Override
    public Long[] getNullableLongArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_LONG_ARRAY);
    }

    @Nullable
    @Override
    public Short[] getNullableShortArray(@Nonnull String fieldName) {
        return get(fieldName, FieldKind.NULLABLE_SHORT_ARRAY);
    }

    private <T> T get(@Nonnull String fieldName, @Nonnull FieldKind fieldKind) {
        check(fieldName, fieldKind);
        return (T) objects.get(fieldName);
    }

    private void check(@Nonnull String fieldName, @Nonnull FieldKind kind) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + " for " + schema);
        }
        if (fd.getKind() != kind) {
            throw new HazelcastSerializationException("Invalid field kind: '" + fieldName + " for " + schema
                    + ", expected : " + fd.getKind() + ", given : " + kind);
        }
    }

    @Override
    protected Object getClassIdentifier() {
        return schema.getTypeName();
    }
}
