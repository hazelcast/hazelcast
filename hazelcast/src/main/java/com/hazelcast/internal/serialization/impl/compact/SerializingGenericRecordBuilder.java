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
import org.jetbrains.annotations.NotNull;

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
        this.defaultCompactWriter = new DefaultCompactWriter(serializer, bufferObjectDataOutputSupplier.get(), (Schema) schema);
        this.bufferObjectDataInputFunc = bufferObjectDataInputFunc;
    }

    public @NotNull GenericRecord build() {
        defaultCompactWriter.end();
        byte[] bytes = defaultCompactWriter.toByteArray();
        return new DefaultCompactReader(serializer, bufferObjectDataInputFunc.apply(bytes), schema, null);
    }

    @Override
    public GenericRecordBuilder setInt(@NotNull String fieldName, int value) {
        defaultCompactWriter.writeInt(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setLong(@NotNull String fieldName, long value) {
        defaultCompactWriter.writeLong(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setString(@NotNull String fieldName, String value) {
        defaultCompactWriter.writeString(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setBoolean(@NotNull String fieldName, boolean value) {
        defaultCompactWriter.writeBoolean(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setByte(@NotNull String fieldName, byte value) {
        defaultCompactWriter.writeByte(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setChar(@NotNull String fieldName, char value) {
        defaultCompactWriter.writeChar(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setDouble(@NotNull String fieldName, double value) {
        defaultCompactWriter.writeDouble(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setFloat(@NotNull String fieldName, float value) {
        defaultCompactWriter.writeFloat(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setShort(@NotNull String fieldName, short value) {
        defaultCompactWriter.writeShort(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setDecimal(@NotNull String fieldName, BigDecimal value) {
        defaultCompactWriter.writeDecimal(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setTime(@NotNull String fieldName, LocalTime value) {
        defaultCompactWriter.writeTime(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setDate(@NotNull String fieldName, LocalDate value) {
        defaultCompactWriter.writeDate(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setTimestamp(@NotNull String fieldName, LocalDateTime value) {
        defaultCompactWriter.writeTimestamp(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setTimestampWithTimezone(@NotNull String fieldName, OffsetDateTime value) {
        defaultCompactWriter.writeTimestampWithTimezone(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setGenericRecord(@NotNull String fieldName, GenericRecord value) {
        defaultCompactWriter.writeGenericRecord(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setGenericRecordArray(@NotNull String fieldName, GenericRecord[] value) {
        defaultCompactWriter.writeGenericRecordArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setByteArray(@NotNull String fieldName, byte[] value) {
        defaultCompactWriter.writeByteArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setBooleanArray(@NotNull String fieldName, boolean[] value) {
        defaultCompactWriter.writeBooleanArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setCharArray(@NotNull String fieldName, char[] value) {
        defaultCompactWriter.writeCharArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setIntArray(@NotNull String fieldName, int[] value) {
        defaultCompactWriter.writeIntArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setLongArray(@NotNull String fieldName, long[] value) {
        defaultCompactWriter.writeLongArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setDoubleArray(@NotNull String fieldName, double[] value) {
        defaultCompactWriter.writeDoubleArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setFloatArray(@NotNull String fieldName, float[] value) {
        defaultCompactWriter.writeFloatArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setShortArray(@NotNull String fieldName, short[] value) {
        defaultCompactWriter.writeShortArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setStringArray(@NotNull String fieldName, String[] value) {
        defaultCompactWriter.writeStringArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setDecimalArray(@NotNull String fieldName, BigDecimal[] value) {
        defaultCompactWriter.writeDecimalArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setTimeArray(@NotNull String fieldName, LocalTime[] value) {
        defaultCompactWriter.writeTimeArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setDateArray(@NotNull String fieldName, LocalDate[] value) {
        defaultCompactWriter.writeDateArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setTimestampArray(@NotNull String fieldName, LocalDateTime[] value) {
        defaultCompactWriter.writeTimestampArray(fieldName, value);
        return this;
    }

    @Override
    public GenericRecordBuilder setTimestampWithTimezoneArray(@NotNull String fieldName, OffsetDateTime[] value) {
        defaultCompactWriter.writeTimestampWithTimezoneArray(fieldName, value);
        return this;
    }

}
