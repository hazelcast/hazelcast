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
import static com.hazelcast.internal.serialization.impl.compact.AbstractGenericRecordBuilder.checkTypeWithSchema;

public class SerializingGenericRecordCloner implements GenericRecordBuilder {

    interface Writer {
        void write() throws IOException;
    }

    private final Schema schema;
    private final CompactInternalGenericRecord genericRecord;
    private final DefaultCompactWriter cw;
    private final CompactStreamSerializer serializer;
    private final Map<String, Writer> fields = new HashMap<>();
    private final Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc;

    public SerializingGenericRecordCloner(CompactStreamSerializer serializer, Schema schema,
                                          CompactInternalGenericRecord record,
                                          Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc,
                                          Supplier<BufferObjectDataOutput> bufferObjectDataOutputSupplier) {
        this.serializer = serializer;
        this.schema = schema;
        this.genericRecord = record;
        this.cw = new DefaultCompactWriter(serializer, bufferObjectDataOutputSupplier.get(), schema, false);
        this.bufferObjectDataInputFunc = bufferObjectDataInputFunc;
    }

    @Override
    @Nonnull
    public GenericRecord build() {
        try {
            for (FieldDescriptor field : schema.getFields()) {
                String fieldName = field.getFieldName();
                Writer writer = fields.get(fieldName);
                if (writer != null) {
                    writer.write();
                    continue;
                }
                FieldType fieldType = field.getType();
                fieldOperations(fieldType).readFromGenericRecordToWriter(cw, genericRecord, fieldName);
            }
            cw.end();
            byte[] bytes = cw.toByteArray();
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
        checkTypeWithSchema(schema, fieldName, FieldType.INT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeInt(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        checkTypeWithSchema(schema, fieldName, FieldType.LONG);
        if (fields.putIfAbsent(fieldName, () -> cw.writeLong(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setString(@Nonnull String fieldName, String value) {
        checkTypeWithSchema(schema, fieldName, FieldType.UTF);
        if (fields.putIfAbsent(fieldName, () -> cw.writeString(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        checkTypeWithSchema(schema, fieldName, FieldType.BOOLEAN);
        if (fields.putIfAbsent(fieldName, () -> cw.writeBoolean(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        checkTypeWithSchema(schema, fieldName, FieldType.BYTE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeByte(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        checkTypeWithSchema(schema, fieldName, FieldType.CHAR);
        if (fields.putIfAbsent(fieldName, () -> cw.writeChar(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        checkTypeWithSchema(schema, fieldName, FieldType.DOUBLE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDouble(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        checkTypeWithSchema(schema, fieldName, FieldType.FLOAT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloat(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        checkTypeWithSchema(schema, fieldName, FieldType.SHORT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeShort(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, BigDecimal value) {
        checkTypeWithSchema(schema, fieldName, FieldType.DECIMAL);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDecimal(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTime(@Nonnull String fieldName, LocalTime value) {
        checkTypeWithSchema(schema, fieldName, FieldType.TIME);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTime(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDate(@Nonnull String fieldName, LocalDate value) {
        checkTypeWithSchema(schema, fieldName, FieldType.DATE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDate(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, LocalDateTime value) {
        checkTypeWithSchema(schema, fieldName, FieldType.TIMESTAMP);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestamp(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value) {
        checkTypeWithSchema(schema, fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampWithTimezone(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, GenericRecord value) {
        checkTypeWithSchema(schema, fieldName, FieldType.COMPOSED);
        if (fields.putIfAbsent(fieldName, () -> cw.writeGenericRecord(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecordArray(@Nonnull String fieldName, GenericRecord[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.COMPOSED_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeGenericRecordArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.BYTE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeByteArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.BOOLEAN_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeBooleanArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.CHAR_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeCharArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.INT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeIntArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.LONG_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeLongArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.DOUBLE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDoubleArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.FLOAT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloatArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.SHORT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeShortArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.UTF_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeStringArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.DECIMAL_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDecimalArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.TIME_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimeArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.DATE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDateArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.TIMESTAMP_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampWithTimezoneArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

}
