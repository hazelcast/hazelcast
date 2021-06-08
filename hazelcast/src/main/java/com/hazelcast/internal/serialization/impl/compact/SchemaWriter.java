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
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class SchemaWriter implements CompactWriter {

    private final TreeMap<String, FieldDescriptor> fieldDefinitionMap = new TreeMap<>(Comparator.naturalOrder());
    private final String className;

    public SchemaWriter(String className) {
        this.className = className;
    }

    public Schema build() {
        List<FieldDescriptor> booleanFieldsList = fieldDefinitionMap.values().stream()
                .filter(fieldDescriptor -> fieldDescriptor.getType().equals(FieldType.BOOLEAN))
                .collect(Collectors.toList());
        int offset = 0;
        int bitOffset = 0;
        for (FieldDescriptor fieldDefinition : booleanFieldsList) {
            fieldDefinition.setOffset(offset);
            fieldDefinition.setBitOffset((byte) (bitOffset % Byte.SIZE));
            bitOffset++;
            if (bitOffset % Byte.SIZE == 0) {
                offset += 1;
            }
        }
        if (bitOffset % Byte.SIZE != 0) {
            offset++;
        }

        List<FieldDescriptor> definiteSizedList = fieldDefinitionMap.values().stream()
                .filter(fieldDescriptor -> fieldDescriptor.getType().hasDefiniteSize())
                .filter(fieldDescriptor -> !fieldDescriptor.getType().equals(FieldType.BOOLEAN))
                .sorted(Comparator.comparingInt(o -> o.getType().getTypeSize())).collect(Collectors.toList());
        for (FieldDescriptor fieldDefinition : definiteSizedList) {
            fieldDefinition.setOffset(offset);
            offset += fieldDefinition.getType().getTypeSize();
        }

        int index = 0;
        List<FieldDescriptor> varSizeList = fieldDefinitionMap.values().stream()
                .filter(fieldDescriptor -> !fieldDescriptor.getType().hasDefiniteSize())
                .collect(Collectors.toList());

        for (FieldDescriptor fieldDefinition : varSizeList) {
            fieldDefinition.setIndex(index++);
        }
        return new Schema(className, fieldDefinitionMap);
    }

    public void addField(FieldDescriptor fieldDefinition) {
        fieldDefinitionMap.put(fieldDefinition.getFieldName(), fieldDefinition);
    }

    @Override
    public void writeInt(@Nonnull String fieldName, int value) {
        addField(new FieldDescriptor(fieldName, FieldType.INT));
    }

    @Override
    public void writeLong(@Nonnull String fieldName, long value) {
        addField(new FieldDescriptor(fieldName, FieldType.LONG));
    }

    @Override
    public void writeString(@Nonnull String fieldName, String str) {
        addField(new FieldDescriptor(fieldName, FieldType.UTF));
    }

    @Override
    public void writeBoolean(@Nonnull String fieldName, boolean value) {
        addField(new FieldDescriptor(fieldName, FieldType.BOOLEAN));
    }

    @Override
    public void writeByte(@Nonnull String fieldName, byte value) {
        addField(new FieldDescriptor(fieldName, FieldType.BYTE));
    }

    @Override
    public void writeChar(@Nonnull String fieldName, char value) {
        addField(new FieldDescriptor(fieldName, FieldType.CHAR));
    }

    @Override
    public void writeDouble(@Nonnull String fieldName, double value) {
        addField(new FieldDescriptor(fieldName, FieldType.DOUBLE));
    }

    @Override
    public void writeFloat(@Nonnull String fieldName, float value) {
        addField(new FieldDescriptor(fieldName, FieldType.FLOAT));
    }

    @Override
    public void writeShort(@Nonnull String fieldName, short value) {
        addField(new FieldDescriptor(fieldName, FieldType.SHORT));
    }

    @Override
    public void writeObject(@Nonnull String fieldName, Object object) {
        addField(new FieldDescriptor(fieldName, FieldType.COMPOSED));
    }

    @Override
    public void writeDecimal(@Nonnull String fieldName, BigDecimal value) {
        addField(new FieldDescriptor(fieldName, FieldType.DECIMAL));
    }

    @Override
    public void writeTime(@Nonnull String fieldName, LocalTime value) {
        addField(new FieldDescriptor(fieldName, FieldType.TIME));
    }

    @Override
    public void writeDate(@Nonnull String fieldName, LocalDate value) {
        addField(new FieldDescriptor(fieldName, FieldType.DATE));
    }

    @Override
    public void writeTimestamp(@Nonnull String fieldName, LocalDateTime value) {
        addField(new FieldDescriptor(fieldName, FieldType.TIMESTAMP));
    }

    @Override
    public void writeTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value) {
        addField(new FieldDescriptor(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE));
    }

    @Override
    public void writeByteArray(@Nonnull String fieldName, byte[] bytes) {
        addField(new FieldDescriptor(fieldName, FieldType.BYTE_ARRAY));
    }

    @Override
    public void writeBooleanArray(@Nonnull String fieldName, boolean[] booleans) {
        addField(new FieldDescriptor(fieldName, FieldType.BOOLEAN_ARRAY));
    }

    @Override
    public void writeCharArray(@Nonnull String fieldName, char[] chars) {
        addField(new FieldDescriptor(fieldName, FieldType.CHAR_ARRAY));
    }

    @Override
    public void writeIntArray(@Nonnull String fieldName, int[] ints) {
        addField(new FieldDescriptor(fieldName, FieldType.INT_ARRAY));
    }

    @Override
    public void writeLongArray(@Nonnull String fieldName, long[] longs) {
        addField(new FieldDescriptor(fieldName, FieldType.LONG_ARRAY));
    }

    @Override
    public void writeDoubleArray(@Nonnull String fieldName, double[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.DOUBLE_ARRAY));
    }

    @Override
    public void writeFloatArray(@Nonnull String fieldName, float[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.FLOAT_ARRAY));
    }

    @Override
    public void writeShortArray(@Nonnull String fieldName, short[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.SHORT_ARRAY));
    }

    @Override
    public void writeStringArray(@Nonnull String fieldName, String[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.UTF_ARRAY));
    }

    @Override
    public void writeDecimalArray(@Nonnull String fieldName, BigDecimal[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.DECIMAL_ARRAY));
    }

    @Override
    public void writeTimeArray(@Nonnull String fieldName, LocalTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.TIME_ARRAY));
    }

    @Override
    public void writeDateArray(@Nonnull String fieldName, LocalDate[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.DATE_ARRAY));
    }

    @Override
    public void writeTimestampArray(@Nonnull String fieldName, LocalDateTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.TIMESTAMP_ARRAY));
    }

    @Override
    public void writeTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY));
    }

    @Override
    public void writeObjectArray(@Nonnull String fieldName, Object[] values) {
        addField(new FieldDescriptor(fieldName, FieldType.COMPOSED_ARRAY));
    }

}
