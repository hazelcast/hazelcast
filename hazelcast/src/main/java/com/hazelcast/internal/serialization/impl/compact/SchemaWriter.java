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

import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.nio.serialization.compact.TypeID;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.TreeMap;

public final class SchemaWriter implements CompactWriter {

    private final TreeMap<String, FieldDescriptor> fieldDefinitionMap = new TreeMap<>(Comparator.naturalOrder());
    private final String className;

    public SchemaWriter(String className) {
        this.className = className;
    }

    public Schema build() {
        return new Schema(className, fieldDefinitionMap);
    }

    public void addField(FieldDescriptor fieldDefinition) {
        fieldDefinitionMap.put(fieldDefinition.getFieldName(), fieldDefinition);
    }

    @Override
    public void writeInt(@Nonnull String fieldName, int value) {
        addField(new FieldDescriptor(fieldName, TypeID.INT));
    }

    @Override
    public void writeLong(@Nonnull String fieldName, long value) {
        addField(new FieldDescriptor(fieldName, TypeID.LONG));
    }

    @Override
    public void writeString(@Nonnull String fieldName, String str) {
        addField(new FieldDescriptor(fieldName, TypeID.STRING));
    }

    @Override
    public void writeBoolean(@Nonnull String fieldName, boolean value) {
        addField(new FieldDescriptor(fieldName, TypeID.BOOLEAN));
    }

    @Override
    public void writeByte(@Nonnull String fieldName, byte value) {
        addField(new FieldDescriptor(fieldName, TypeID.BYTE));
    }

    @Override
    public void writeChar(@Nonnull String fieldName, char value) {
        addField(new FieldDescriptor(fieldName, TypeID.CHAR));
    }

    @Override
    public void writeDouble(@Nonnull String fieldName, double value) {
        addField(new FieldDescriptor(fieldName, TypeID.DOUBLE));
    }

    @Override
    public void writeFloat(@Nonnull String fieldName, float value) {
        addField(new FieldDescriptor(fieldName, TypeID.FLOAT));
    }

    @Override
    public void writeShort(@Nonnull String fieldName, short value) {
        addField(new FieldDescriptor(fieldName, TypeID.SHORT));
    }

    @Override
    public void writeObject(@Nonnull String fieldName, Object object) {
        addField(new FieldDescriptor(fieldName, TypeID.COMPOSED));
    }

    @Override
    public void writeDecimal(@Nonnull String fieldName, BigDecimal value) {
        addField(new FieldDescriptor(fieldName, TypeID.DECIMAL));
    }

    @Override
    public void writeTime(@Nonnull String fieldName, LocalTime value) {
        addField(new FieldDescriptor(fieldName, TypeID.TIME));
    }

    @Override
    public void writeDate(@Nonnull String fieldName, LocalDate value) {
        addField(new FieldDescriptor(fieldName, TypeID.DATE));
    }

    @Override
    public void writeTimestamp(@Nonnull String fieldName, LocalDateTime value) {
        addField(new FieldDescriptor(fieldName, TypeID.TIMESTAMP));
    }

    @Override
    public void writeTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value) {
        addField(new FieldDescriptor(fieldName, TypeID.TIMESTAMP_WITH_TIMEZONE));
    }

    @Override
    public void writeByteArray(@Nonnull String fieldName, byte[] bytes) {
        addField(new FieldDescriptor(fieldName, TypeID.BYTE_ARRAY));
    }

    @Override
    public void writeBooleanArray(@Nonnull String fieldName, boolean[] booleans) {
        addField(new FieldDescriptor(fieldName, TypeID.BOOLEAN_ARRAY));
    }

    @Override
    public void writeCharArray(@Nonnull String fieldName, char[] chars) {
        addField(new FieldDescriptor(fieldName, TypeID.CHAR_ARRAY));
    }

    @Override
    public void writeIntArray(@Nonnull String fieldName, int[] ints) {
        addField(new FieldDescriptor(fieldName, TypeID.INT_ARRAY));
    }

    @Override
    public void writeLongArray(@Nonnull String fieldName, long[] longs) {
        addField(new FieldDescriptor(fieldName, TypeID.LONG_ARRAY));
    }

    @Override
    public void writeDoubleArray(@Nonnull String fieldName, double[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.DOUBLE_ARRAY));
    }

    @Override
    public void writeFloatArray(@Nonnull String fieldName, float[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.FLOAT_ARRAY));
    }

    @Override
    public void writeShortArray(@Nonnull String fieldName, short[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.SHORT_ARRAY));
    }

    @Override
    public void writeStringArray(@Nonnull String fieldName, String[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.STRING_ARRAY));
    }

    @Override
    public void writeDecimalArray(@Nonnull String fieldName, BigDecimal[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.DECIMAL_ARRAY));
    }

    @Override
    public void writeTimeArray(@Nonnull String fieldName, LocalTime[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.TIME_ARRAY));
    }

    @Override
    public void writeDateArray(@Nonnull String fieldName, LocalDate[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.DATE_ARRAY));
    }

    @Override
    public void writeTimestampArray(@Nonnull String fieldName, LocalDateTime[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.TIMESTAMP_ARRAY));
    }

    @Override
    public void writeTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.TIMESTAMP_WITH_TIMEZONE_ARRAY));
    }

    @Override
    public void writeObjectArray(@Nonnull String fieldName, Object[] values) {
        addField(new FieldDescriptor(fieldName, TypeID.COMPOSED_ARRAY));
    }

}
