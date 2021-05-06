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


import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.function.Function;
import java.util.function.Supplier;

public class SerializingGenericRecordBuilder implements GenericRecordBuilder {

    private final DefaultCompactWriter defaultCompactWriter;
    private final CompactStreamSerializer serializer;
    private final Schema schema;
    private final Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc;

    public SerializingGenericRecordBuilder(CompactStreamSerializer serializer, Schema schema,
                                           Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc,
                                           Supplier<BufferObjectDataOutput> bufferObjectDataOutputSupplier) {
        this.serializer = serializer;
        this.schema = schema;
        this.defaultCompactWriter = new DefaultCompactWriter(serializer, bufferObjectDataOutputSupplier.get(),
                schema, false);
        this.bufferObjectDataInputFunc = bufferObjectDataInputFunc;
    }

    @Override
    @Nonnull
    public GenericRecord build() {
        defaultCompactWriter.end();
        byte[] bytes = defaultCompactWriter.toByteArray();
        return new DefaultCompactReader(serializer, bufferObjectDataInputFunc.apply(bytes), schema,
                null, false);
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setInt(@Nonnull String fieldName, int value) {
        defaultCompactWriter.writeInt(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        defaultCompactWriter.writeLong(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setString(@Nonnull String fieldName, String value) {
        defaultCompactWriter.writeString(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        defaultCompactWriter.writeBoolean(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        defaultCompactWriter.writeByte(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        defaultCompactWriter.writeChar(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        defaultCompactWriter.writeDouble(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        defaultCompactWriter.writeFloat(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        defaultCompactWriter.writeShort(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, BigDecimal value) {
        defaultCompactWriter.writeDecimal(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTime(@Nonnull String fieldName, LocalTime value) {
        defaultCompactWriter.writeTime(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDate(@Nonnull String fieldName, LocalDate value) {
        defaultCompactWriter.writeDate(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, LocalDateTime value) {
        defaultCompactWriter.writeTimestamp(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value) {
        defaultCompactWriter.writeTimestampWithTimezone(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, GenericRecord value) {
        defaultCompactWriter.writeGenericRecord(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecordArray(@Nonnull String fieldName, GenericRecord[] value) {
        defaultCompactWriter.writeGenericRecordArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        defaultCompactWriter.writeByteArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        defaultCompactWriter.writeBooleanArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        defaultCompactWriter.writeCharArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        defaultCompactWriter.writeIntArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        defaultCompactWriter.writeLongArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        defaultCompactWriter.writeDoubleArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        defaultCompactWriter.writeFloatArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        defaultCompactWriter.writeShortArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        defaultCompactWriter.writeStringArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        defaultCompactWriter.writeDecimalArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        defaultCompactWriter.writeTimeArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        defaultCompactWriter.writeDateArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        defaultCompactWriter.writeTimestampArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        defaultCompactWriter.writeTimestampWithTimezoneArray(fieldName, value);
        return this;
    }

}
