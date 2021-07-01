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
import com.hazelcast.nio.serialization.compact.TypeID;

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

import static com.hazelcast.internal.serialization.impl.compact.schema.FieldOperations.fieldOperations;
import static com.hazelcast.internal.serialization.impl.compact.AbstractCompactRecordBuilder.checkTypeWithSchema;

public class SerializingCompactRecordCloner implements CompactRecordBuilder {

    interface Writer {
        void write() throws IOException;
    }

    private final Schema schema;
    private final InternalCompactRecord compactRecord;
    private final DefaultCompactWriter cw;
    private final CompactStreamSerializer serializer;
    private final Map<String, Writer> fields = new HashMap<>();
    private final Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc;

    public SerializingCompactRecordCloner(CompactStreamSerializer serializer, Schema schema,
                                          InternalCompactRecord record,
                                          Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc,
                                          Supplier<BufferObjectDataOutput> bufferObjectDataOutputSupplier) {
        this.serializer = serializer;
        this.schema = schema;
        this.compactRecord = record;
        this.cw = new DefaultCompactWriter(serializer, bufferObjectDataOutputSupplier.get(), schema, false);
        this.bufferObjectDataInputFunc = bufferObjectDataInputFunc;
    }

    @Override
    @Nonnull
    public CompactRecord build() {
        try {
            for (FieldDescriptor field : schema.getFields()) {
                String fieldName = field.getFieldName();
                Writer writer = fields.get(fieldName);
                if (writer != null) {
                    writer.write();
                    continue;
                }
                TypeID fieldType = field.getType();
                fieldOperations(fieldType).readFromCompactRecordToWriter(cw, compactRecord, fieldName);
            }
            cw.end();
            byte[] bytes = cw.toByteArray();
            Class associatedClass = compactRecord.getAssociatedClass();
            BufferObjectDataInput dataInput = bufferObjectDataInputFunc.apply(bytes);
            return new DefaultCompactReader(serializer, dataInput, schema, associatedClass, false);
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        }
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setInt(@Nonnull String fieldName, int value) {
        checkTypeWithSchema(schema, fieldName, TypeID.INT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeInt(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setLong(@Nonnull String fieldName, long value) {
        checkTypeWithSchema(schema, fieldName, TypeID.LONG);
        if (fields.putIfAbsent(fieldName, () -> cw.writeLong(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setString(@Nonnull String fieldName, String value) {
        checkTypeWithSchema(schema, fieldName, TypeID.STRING);
        if (fields.putIfAbsent(fieldName, () -> cw.writeString(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        checkTypeWithSchema(schema, fieldName, TypeID.BOOLEAN);
        if (fields.putIfAbsent(fieldName, () -> cw.writeBoolean(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        checkTypeWithSchema(schema, fieldName, TypeID.BYTE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeByte(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setChar(@Nonnull String fieldName, char value) {
        checkTypeWithSchema(schema, fieldName, TypeID.CHAR);
        if (fields.putIfAbsent(fieldName, () -> cw.writeChar(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        checkTypeWithSchema(schema, fieldName, TypeID.DOUBLE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDouble(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        checkTypeWithSchema(schema, fieldName, TypeID.FLOAT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloat(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setShort(@Nonnull String fieldName, short value) {
        checkTypeWithSchema(schema, fieldName, TypeID.SHORT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeShort(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDecimal(@Nonnull String fieldName, BigDecimal value) {
        checkTypeWithSchema(schema, fieldName, TypeID.DECIMAL);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDecimal(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTime(@Nonnull String fieldName, LocalTime value) {
        checkTypeWithSchema(schema, fieldName, TypeID.TIME);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTime(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDate(@Nonnull String fieldName, LocalDate value) {
        checkTypeWithSchema(schema, fieldName, TypeID.DATE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDate(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestamp(@Nonnull String fieldName, LocalDateTime value) {
        checkTypeWithSchema(schema, fieldName, TypeID.TIMESTAMP);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestamp(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value) {
        checkTypeWithSchema(schema, fieldName, TypeID.TIMESTAMP_WITH_TIMEZONE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampWithTimezone(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setCompactRecord(@Nonnull String fieldName, CompactRecord value) {
        checkTypeWithSchema(schema, fieldName, TypeID.COMPOSED);
        if (fields.putIfAbsent(fieldName, () -> cw.writeCompactRecord(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setCompactRecordArray(@Nonnull String fieldName, CompactRecord[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.COMPOSED_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeCompactRecordArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setByteArray(@Nonnull String fieldName, byte[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.BYTE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeByteArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setBooleanArray(@Nonnull String fieldName, boolean[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.BOOLEAN_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeBooleanArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setCharArray(@Nonnull String fieldName, char[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.CHAR_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeCharArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setIntArray(@Nonnull String fieldName, int[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.INT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeIntArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setLongArray(@Nonnull String fieldName, long[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.LONG_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeLongArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDoubleArray(@Nonnull String fieldName, double[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.DOUBLE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDoubleArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setFloatArray(@Nonnull String fieldName, float[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.FLOAT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloatArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setShortArray(@Nonnull String fieldName, short[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.SHORT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeShortArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setStringArray(@Nonnull String fieldName, String[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.STRING_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeStringArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDecimalArray(@Nonnull String fieldName, BigDecimal[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.DECIMAL_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDecimalArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimeArray(@Nonnull String fieldName, LocalTime[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.TIME_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimeArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setDateArray(@Nonnull String fieldName, LocalDate[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.DATE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDateArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestampArray(@Nonnull String fieldName, LocalDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.TIMESTAMP_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public CompactRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, TypeID.TIMESTAMP_WITH_TIMEZONE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampWithTimezoneArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

}
