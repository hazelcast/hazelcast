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
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;

public class SerializingGenericRecordCloner implements GenericRecordBuilder {

    interface Writer {
        void write() throws IOException;
    }

    private final Schema schema;
    private final DefaultCompactReader genericRecord;
    private final DefaultCompactWriter compactWriter;
    private final CompactStreamSerializer serializer;
    private final Map<String, Writer> overwrittenFields = new HashMap<>();
    private final Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc;

    public SerializingGenericRecordCloner(CompactStreamSerializer serializer, Schema schema, DefaultCompactReader record,
                                          Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc,
                                          Supplier<BufferObjectDataOutput> bufferObjectDataOutputSupplier) {
        this.serializer = serializer;
        this.schema = schema;
        this.genericRecord = record;
        this.compactWriter = new DefaultCompactWriter(serializer, bufferObjectDataOutputSupplier.get(), schema, false);
        this.bufferObjectDataInputFunc = bufferObjectDataInputFunc;
    }

    @Override
    @Nonnull
    public GenericRecord build() {
        try {
            for (FieldDescriptor field : schema.getFields()) {
                String fieldName = field.getFieldName();
                Writer writer = overwrittenFields.get(fieldName);
                if (writer != null) {
                    writer.write();
                    continue;
                }
                FieldType fieldType = field.getType();
                fieldOperations(fieldType).readFromGenericRecordToWriter(compactWriter, genericRecord, fieldName);
            }
            compactWriter.end();
            byte[] bytes = compactWriter.toByteArray();
            Class associatedClass = genericRecord.getAssociatedClass();
            BufferObjectDataInput dataInput = bufferObjectDataInputFunc.apply(bytes);
            return new DefaultCompactReader(serializer, dataInput, schema, associatedClass, false);
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        }
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setInt(@Nonnull String fieldName, int value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeInt(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeLong(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setString(@Nonnull String fieldName, String value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeString(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeBoolean(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeByte(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeChar(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeDouble(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeFloat(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeShort(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, BigDecimal value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeDecimal(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTime(@Nonnull String fieldName, LocalTime value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeTime(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDate(@Nonnull String fieldName, LocalDate value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeDate(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, LocalDateTime value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeTimestamp(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeTimestampWithTimezone(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, GenericRecord value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeGenericRecord(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecordArray(@Nonnull String fieldName, GenericRecord[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeGenericRecordArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeByteArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeBooleanArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeCharArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeIntArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeLongArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeDoubleArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeFloatArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeShortArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeStringArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeDecimalArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeTimeArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeDateArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeTimestampArray(fieldName, value));
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        overwrittenFields.put(fieldName, () -> compactWriter.writeTimestampWithTimezoneArray(fieldName, value));
        return this;
    }

}
