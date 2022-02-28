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


import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
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
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A GenericRecordBuilder that serializes the fields.
 * <p>
 * The given schema is respected. So, the fields that are not defined
 * in the schema cannot be written at all.
 * <p>
 * It also verifies that no fields are overwritten as we don't allow
 * it.
 */
public class SerializingGenericRecordBuilder implements GenericRecordBuilder {

    private final DefaultCompactWriter defaultCompactWriter;
    private final CompactStreamSerializer serializer;
    private final Schema schema;
    private final Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc;
    private final Set<String> writtenFields = new HashSet<>();

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
    public GenericRecordBuilder setInt32(@Nonnull String fieldName, int value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeInt32(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setInt64(@Nonnull String fieldName, long value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeInt64(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setString(@Nonnull String fieldName, String value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeString(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeBoolean(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setInt8(@Nonnull String fieldName, byte value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeInt8(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        throw new UnsupportedOperationException("Compact format does not support writing a char field");
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat64(@Nonnull String fieldName, double value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeFloat64(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat32(@Nonnull String fieldName, float value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeFloat32(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setInt16(@Nonnull String fieldName, short value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeInt16(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableBoolean(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt8(@Nonnull String fieldName, @Nullable Byte value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableInt8(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat64(@Nonnull String fieldName, @Nullable Double value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableFloat64(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat32(@Nonnull String fieldName, @Nullable Float value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableFloat32(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt32(@Nonnull String fieldName, @Nullable Integer value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableInt32(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt64(@Nonnull String fieldName, @Nullable Long value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableInt64(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt16(@Nonnull String fieldName, @Nullable Short value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableInt16(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDecimal(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTime(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDate(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTimestamp(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeTimestampWithTimezone(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeGenericRecord(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfGenericRecord(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfInt8(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfBoolean(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfChar(@Nonnull String fieldName, @Nullable char[] value) {
        throw new UnsupportedOperationException("Compact format does not support writing an array of chars field");
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInt32(@Nonnull String fieldName, @Nullable int[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfInt32(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInt64(@Nonnull String fieldName, @Nullable long[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfInt64(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfFloat64(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfFloat32(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInt16(@Nonnull String fieldName, @Nullable short[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfInt16(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableBoolean(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableInt8(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableFloat32(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableInt32(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableFloat64(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableInt64(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableInt16(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfString(@Nonnull String fieldName, @Nullable String[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfString(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfDecimal(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfTime(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfDate(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfTimestamp(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfTimestampWithTimezone(fieldName, value);
        return this;
    }

    private void checkIfAlreadyWritten(@Nonnull String fieldName) {
        if (!writtenFields.add(fieldName)) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
    }
}
