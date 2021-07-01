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
import com.hazelcast.nio.serialization.compact.CompactRecord;
import com.hazelcast.nio.serialization.compact.CompactRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class SerializingCompactRecordBuilder implements CompactRecordBuilder {

    private final DefaultCompactWriter defaultCompactWriter;
    private final CompactStreamSerializer serializer;
    private final Schema schema;
    private final Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc;
    private final Set<String> writtenFields = new HashSet<>();

    public SerializingCompactRecordBuilder(CompactStreamSerializer serializer, Schema schema,
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
    public CompactRecord build() {
        Set<String> fieldNames = schema.getFieldNames();
        for (String fieldName : fieldNames) {
            if (!writtenFields.contains(fieldName)) {
                throw new HazelcastSerializationException("Found an unset field " + fieldName
                        + ". All the fields must be set before build");
            }
        }
        defaultCompactWriter.end();
        byte[] bytes = defaultCompactWriter.toByteArray();
        return new DefaultCompactReader(serializer, bufferObjectDataInputFunc.apply(bytes), schema,
                null, false);
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setInt(@Nonnull String fieldName, int value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeInt(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setLong(@Nonnull String fieldName, long value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeLong(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setString(@Nonnull String fieldName, String value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeString(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeBoolean(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeByte(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setChar(@Nonnull String fieldName, char value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeChar(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDouble(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeFloat(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setShort(@Nonnull String fieldName, short value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeShort(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDecimal(@Nonnull String fieldName, BigDecimal value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDecimal(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTime(@Nonnull String fieldName, LocalTime value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTime(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDate(@Nonnull String fieldName, LocalDate value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDate(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestamp(@Nonnull String fieldName, LocalDateTime value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTimestamp(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTimestampWithTimezone(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setCompactRecord(@Nonnull String fieldName, CompactRecord value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeCompactRecord(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setCompactRecordArray(@Nonnull String fieldName, CompactRecord[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeCompactRecordArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeByteArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeBooleanArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeCharArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeIntArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeLongArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDoubleArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeFloatArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeShortArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeStringArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDecimalArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTimeArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDateArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTimestampArray(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTimestampWithTimezoneArray(fieldName, value);
        return this;
    }

    private void checkIfAlreadyWritten(@Nonnull String fieldName) {
        if (!writtenFields.add(fieldName)) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
    }

}
