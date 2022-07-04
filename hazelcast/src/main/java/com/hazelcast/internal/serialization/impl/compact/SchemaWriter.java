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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * A CompactWriter that constructs a schema from Compact
 * serializable objects.
 */
public final class SchemaWriter implements CompactWriter {

    private final TreeMap<String, FieldDescriptor> fieldDefinitionMap = new TreeMap<>(Comparator.naturalOrder());
    private final String typeName;

    public SchemaWriter(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Builds the schema from the written fields and returns it.
     */
    public Schema build() {
        return new Schema(typeName, fieldDefinitionMap);
    }

    /**
     * Adds a field to the schema.
     */
    public void addField(FieldDescriptor fieldDefinition) {
        fieldDefinitionMap.put(fieldDefinition.getFieldName(), fieldDefinition);
    }

    @Override
    public void writeInt32(@Nonnull String fieldName, int value) {
        addField(new FieldDescriptor(fieldName, FieldKind.INT32));
    }

    @Override
    public void writeInt64(@Nonnull String fieldName, long value) {
        addField(new FieldDescriptor(fieldName, FieldKind.INT64));
    }

    @Override
    public void writeString(@Nonnull String fieldName, @Nullable String str) {
        addField(new FieldDescriptor(fieldName, FieldKind.STRING));
    }

    @Override
    public void writeBoolean(@Nonnull String fieldName, boolean value) {
        addField(new FieldDescriptor(fieldName, FieldKind.BOOLEAN));
    }

    @Override
    public void writeInt8(@Nonnull String fieldName, byte value) {
        addField(new FieldDescriptor(fieldName, FieldKind.INT8));
    }

    @Override
    public void writeFloat64(@Nonnull String fieldName, double value) {
        addField(new FieldDescriptor(fieldName, FieldKind.FLOAT64));
    }

    @Override
    public void writeFloat32(@Nonnull String fieldName, float value) {
        addField(new FieldDescriptor(fieldName, FieldKind.FLOAT32));
    }

    @Override
    public void writeInt16(@Nonnull String fieldName, short value) {
        addField(new FieldDescriptor(fieldName, FieldKind.INT16));
    }

    @Override
    public void writeCompact(@Nonnull String fieldName, @Nullable Object object) {
        addField(new FieldDescriptor(fieldName, FieldKind.COMPACT));
    }

    @Override
    public void writeDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        addField(new FieldDescriptor(fieldName, FieldKind.DECIMAL));
    }

    @Override
    public void writeTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        addField(new FieldDescriptor(fieldName, FieldKind.TIME));
    }

    @Override
    public void writeDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        addField(new FieldDescriptor(fieldName, FieldKind.DATE));
    }

    @Override
    public void writeTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        addField(new FieldDescriptor(fieldName, FieldKind.TIMESTAMP));
    }

    @Override
    public void writeTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        addField(new FieldDescriptor(fieldName, FieldKind.TIMESTAMP_WITH_TIMEZONE));
    }

    @Override
    public void writeArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_INT8));
    }

    @Override
    public void writeArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_BOOLEAN));
    }

    @Override
    public void writeArrayOfInt32(@Nonnull String fieldName, @Nullable int[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_INT32));
    }

    @Override
    public void writeArrayOfInt64(@Nonnull String fieldName, @Nullable long[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_INT64));
    }

    @Override
    public void writeArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_FLOAT64));
    }

    @Override
    public void writeArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_FLOAT32));
    }

    @Override
    public void writeArrayOfInt16(@Nonnull String fieldName, @Nullable short[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_INT16));
    }

    @Override
    public void writeArrayOfString(@Nonnull String fieldName, @Nullable String[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_STRING));
    }

    @Override
    public void writeArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_DECIMAL));
    }

    @Override
    public void writeArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_TIME));
    }

    @Override
    public void writeArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_DATE));
    }

    @Override
    public void writeArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_TIMESTAMP));
    }

    @Override
    public void writeArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE));
    }

    @Override
    public void writeArrayOfCompact(@Nonnull String fieldName, @Nullable Object[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_COMPACT));
    }

    @Override
    public void writeNullableInt8(@Nonnull String fieldName, @Nullable Byte value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_INT8));
    }

    @Override
    public void writeNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_BOOLEAN));
    }

    @Override
    public void writeNullableInt16(@Nonnull String fieldName, @Nullable Short value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_INT16));
    }

    @Override
    public void writeNullableInt32(@Nonnull String fieldName, @Nullable Integer value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_INT32));
    }

    @Override
    public void writeNullableInt64(@Nonnull String fieldName, @Nullable Long value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_INT64));
    }

    @Override
    public void writeNullableFloat32(@Nonnull String fieldName, @Nullable Float value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_FLOAT32));
    }

    @Override
    public void writeNullableFloat64(@Nonnull String fieldName, @Nullable Double value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_FLOAT64));
    }

    @Override
    public void writeArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_INT8));
    }

    @Override
    public void writeArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN));
    }

    @Override
    public void writeArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_INT16));
    }

    @Override
    public void writeArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_INT32));
    }

    @Override
    public void writeArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_INT64));
    }

    @Override
    public void writeArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_FLOAT32));
    }

    @Override
    public void writeArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_FLOAT64));
    }
}
