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
    public GenericRecordBuilder setInt(@Nonnull String fieldName, int value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeInt(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeLong(fieldName, value);
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
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeByte(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        throw new UnsupportedOperationException("Compact format does not support writing a char field");
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeDouble(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeFloat(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeShort(fieldName, value);
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
    public GenericRecordBuilder setNullableByte(@Nonnull String fieldName, @Nullable Byte value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableByte(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableDouble(@Nonnull String fieldName, @Nullable Double value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableDouble(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat(@Nonnull String fieldName, @Nullable Float value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableFloat(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt(@Nonnull String fieldName, @Nullable Integer value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableInt(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableLong(@Nonnull String fieldName, @Nullable Long value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableLong(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableShort(@Nonnull String fieldName, @Nullable Short value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeNullableShort(fieldName, value);
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
    public GenericRecordBuilder setArrayOfGenericRecords(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfGenericRecords(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfBytes(@Nonnull String fieldName, @Nullable byte[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfBytes(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfBooleans(@Nonnull String fieldName, @Nullable boolean[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfBooleans(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfChars(@Nonnull String fieldName, @Nullable char[] value) {
        throw new UnsupportedOperationException("Compact format does not support writing an array of chars field");
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInts(@Nonnull String fieldName, @Nullable int[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfInts(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfLongs(@Nonnull String fieldName, @Nullable long[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfLongs(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDoubles(@Nonnull String fieldName, @Nullable double[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfDoubles(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfFloats(@Nonnull String fieldName, @Nullable float[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfFloats(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfShorts(@Nonnull String fieldName, @Nullable short[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfShorts(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBooleans(@Nonnull String fieldName, @Nullable Boolean[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableBooleans(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBytes(@Nonnull String fieldName, @Nullable Byte[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableBytes(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloats(@Nonnull String fieldName, @Nullable Float[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableFloats(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInts(@Nonnull String fieldName, @Nullable Integer[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableInts(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableDoubles(@Nonnull String fieldName, @Nullable Double[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableDoubles(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableLongs(@Nonnull String fieldName, @Nullable Long[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableLongs(fieldName, value);
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableShorts(@Nonnull String fieldName, @Nullable Short[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfNullableShorts(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfStrings(@Nonnull String fieldName, @Nullable String[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfStrings(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDecimals(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfDecimals(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimes(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfTimes(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDates(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfDates(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestamps(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfTimestamps(fieldName, value);
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestampWithTimezones(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        checkIfAlreadyWritten(fieldName);
        defaultCompactWriter.writeArrayOfTimestampWithTimezones(fieldName, value);
        return this;
    }

    private void checkIfAlreadyWritten(@Nonnull String fieldName) {
        if (!writtenFields.add(fieldName)) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
    }
}
