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

import com.hazelcast.internal.json.JsonEscape;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.AbstractGenericRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * An extension of the {@link AbstractGenericRecord} that requires the
 * implementors to provide a {@link Schema} that represents the
 * Compact serialized objects.
 * <p>
 * See the javadoc of {@link InternalGenericRecord} for GenericRecord class hierarchy.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public abstract class CompactGenericRecord extends AbstractGenericRecord {

    /**
     * Returns the schema associated with this GenericRecord.
     */
    public abstract Schema getSchema();

    /**
     * WARNING: The following getter methods are for optimizing compact generic records query reading. They
     * allow a field descriptor to be reused and not having to read the same field descriptor twice from schema.
     * These methods should only be used if the field descriptor passed to these methods are known to be a valid
     * field descriptor in the generic record and its field kind is correct.
     */
    public abstract boolean getBoolean(@Nonnull FieldDescriptor fd);

    public abstract byte getInt8(@Nonnull FieldDescriptor fd);

    public abstract short getInt16(@Nonnull FieldDescriptor fd);

    public abstract int getInt32(@Nonnull FieldDescriptor fd);

    public abstract long getInt64(@Nonnull FieldDescriptor fd);

    public abstract float getFloat32(@Nonnull FieldDescriptor fd);

    public abstract double getFloat64(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract String getString(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract BigDecimal getDecimal(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract LocalTime getTime(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract LocalDate getDate(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract LocalDateTime getTimestamp(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract OffsetDateTime getTimestampWithTimezone(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract boolean[] getArrayOfBoolean(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract byte[] getArrayOfInt8(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract short[] getArrayOfInt16(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract int[] getArrayOfInt32(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract long[] getArrayOfInt64(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract float[] getArrayOfFloat32(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract double[] getArrayOfFloat64(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract String[] getArrayOfString(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract BigDecimal[] getArrayOfDecimal(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract LocalTime[] getArrayOfTime(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract LocalDate[] getArrayOfDate(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract LocalDateTime[] getArrayOfTimestamp(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract OffsetDateTime[] getArrayOfTimestampWithTimezone(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Boolean getNullableBoolean(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Byte getNullableInt8(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Short getNullableInt16(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Integer getNullableInt32(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Long getNullableInt64(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Float getNullableFloat32(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Double getNullableFloat64(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Boolean[] getArrayOfNullableBoolean(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Byte[] getArrayOfNullableInt8(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Short[] getArrayOfNullableInt16(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Integer[] getArrayOfNullableInt32(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Long[] getArrayOfNullableInt64(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Float[] getArrayOfNullableFloat32(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract Double[] getArrayOfNullableFloat64(@Nonnull FieldDescriptor fd);

    @Nullable
    public abstract <T> T getObject(@Nonnull FieldDescriptor fd);

    @Nonnull
    public abstract FieldDescriptor getFieldDescriptor(@Nonnull String fieldName);

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('{');
        JsonEscape.writeEscaped(stringBuilder, getSchema().getTypeName());
        stringBuilder.append(": ");
        writeFieldsToStringBuilder(stringBuilder);
        stringBuilder.append('}');
        return stringBuilder.toString();
    }
}
