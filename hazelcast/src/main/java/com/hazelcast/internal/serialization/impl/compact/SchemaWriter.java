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
    private final String className;

    public SchemaWriter(String className) {
        this.className = className;
    }

    /**
     * Builds the schema from the written fields and returns it.
     */
    public Schema build() {
        return new Schema(className, fieldDefinitionMap);
    }

    /**
     * Adds a field to the schema.
     */
    public void addField(FieldDescriptor fieldDefinition) {
        fieldDefinitionMap.put(fieldDefinition.getFieldName(), fieldDefinition);
    }

    @Override
    public void writeInt(@Nonnull String fieldName, int value) {
        addField(new FieldDescriptor(fieldName, FieldKind.INT));
    }

    @Override
    public void writeLong(@Nonnull String fieldName, long value) {
        addField(new FieldDescriptor(fieldName, FieldKind.LONG));
    }

    @Override
    public void writeString(@Nonnull String fieldName, String str) {
        addField(new FieldDescriptor(fieldName, FieldKind.STRING));
    }

    @Override
    public void writeBoolean(@Nonnull String fieldName, boolean value) {
        addField(new FieldDescriptor(fieldName, FieldKind.BOOLEAN));
    }

    @Override
    public void writeByte(@Nonnull String fieldName, byte value) {
        addField(new FieldDescriptor(fieldName, FieldKind.BYTE));
    }

    @Override
    public void writeDouble(@Nonnull String fieldName, double value) {
        addField(new FieldDescriptor(fieldName, FieldKind.DOUBLE));
    }

    @Override
    public void writeFloat(@Nonnull String fieldName, float value) {
        addField(new FieldDescriptor(fieldName, FieldKind.FLOAT));
    }

    @Override
    public void writeShort(@Nonnull String fieldName, short value) {
        addField(new FieldDescriptor(fieldName, FieldKind.SHORT));
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
    public void writeArrayOfBytes(@Nonnull String fieldName, @Nullable byte[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_BYTES));
    }

    @Override
    public void writeArrayOfBooleans(@Nonnull String fieldName, @Nullable boolean[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_BOOLEANS));
    }

    @Override
    public void writeArrayOfInts(@Nonnull String fieldName, @Nullable int[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_INTS));
    }

    @Override
    public void writeArrayOfLongs(@Nonnull String fieldName, @Nullable long[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_LONGS));
    }

    @Override
    public void writeArrayOfDoubles(@Nonnull String fieldName, @Nullable double[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_DOUBLES));
    }

    @Override
    public void writeArrayOfFloats(@Nonnull String fieldName, @Nullable float[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_FLOATS));
    }

    @Override
    public void writeArrayOfShorts(@Nonnull String fieldName, @Nullable short[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_SHORTS));
    }

    @Override
    public void writeArrayOfStrings(@Nonnull String fieldName, @Nullable String[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_STRINGS));
    }

    @Override
    public void writeArrayOfDecimals(@Nonnull String fieldName, @Nullable BigDecimal[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_DECIMALS));
    }

    @Override
    public void writeArrayOfTimes(@Nonnull String fieldName, @Nullable LocalTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_TIMES));
    }

    @Override
    public void writeArrayOfDates(@Nonnull String fieldName, @Nullable LocalDate[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_DATES));
    }

    @Override
    public void writeArrayOfTimestamps(@Nonnull String fieldName, @Nullable LocalDateTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_TIMESTAMPS));
    }

    @Override
    public void writeArrayOfTimestampWithTimezones(@Nonnull String fieldName, @Nullable OffsetDateTime[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES));
    }

    @Override
    public void writeArrayOfCompacts(@Nonnull String fieldName, @Nullable Object[] values) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_COMPACTS));
    }

    @Override
    public void writeNullableByte(@Nonnull String fieldName, @Nullable Byte value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_BYTE));
    }

    @Override
    public void writeNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_BOOLEAN));
    }

    @Override
    public void writeNullableShort(@Nonnull String fieldName, @Nullable Short value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_SHORT));
    }

    @Override
    public void writeNullableInt(@Nonnull String fieldName, @Nullable Integer value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_INT));
    }

    @Override
    public void writeNullableLong(@Nonnull String fieldName, @Nullable Long value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_LONG));
    }

    @Override
    public void writeNullableFloat(@Nonnull String fieldName, @Nullable Float value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_FLOAT));
    }

    @Override
    public void writeNullableDouble(@Nonnull String fieldName, @Nullable Double value) {
        addField(new FieldDescriptor(fieldName, FieldKind.NULLABLE_DOUBLE));
    }

    @Override
    public void writeArrayOfNullableBytes(@Nonnull String fieldName, @Nullable Byte[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_BYTES));
    }

    @Override
    public void writeArrayOfNullableBooleans(@Nonnull String fieldName, @Nullable Boolean[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_BOOLEANS));
    }

    @Override
    public void writeArrayOfNullableShorts(@Nonnull String fieldName, @Nullable Short[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_SHORTS));
    }

    @Override
    public void writeArrayOfNullableInts(@Nonnull String fieldName, @Nullable Integer[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_INTS));
    }

    @Override
    public void writeArrayOfNullableLongs(@Nonnull String fieldName, @Nullable Long[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_LONGS));
    }

    @Override
    public void writeArrayOfNullableFloats(@Nonnull String fieldName, @Nullable Float[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_FLOATS));
    }

    @Override
    public void writeArrayOfNullableDoubles(@Nonnull String fieldName, @Nullable Double[] value) {
        addField(new FieldDescriptor(fieldName, FieldKind.ARRAY_OF_NULLABLE_DOUBLES));
    }
}
