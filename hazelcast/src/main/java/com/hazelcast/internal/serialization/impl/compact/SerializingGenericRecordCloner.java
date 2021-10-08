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
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

/**
 * A GenericRecordBuilder that clones a GenericRecord and serializes
 * its fields along with overwritten fields.
 * <p>
 * The given schema is respected. So, the fields that are not defined
 * in the schema cannot be written at all.
 * <p>
 * It also verifies that no fields written more than once as we don't
 * allow it. Since this is a cloner, a field might not be set at all.
 * In this case, the builder will use the field from the cloned
 * GenericRecord itself.
 */
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
                } else {
                    // Field is not overwritten. Write the field from the generic record.
                    FieldKind fieldKind = field.getKind();
                    fieldOperations(fieldKind).writeFieldFromRecordToWriter(cw, genericRecord, fieldName);
                }
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
        checkTypeWithSchema(schema, fieldName, FieldKind.INT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeInt(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLong(@Nonnull String fieldName, long value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.LONG);
        if (fields.putIfAbsent(fieldName, () -> cw.writeLong(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setString(@Nonnull String fieldName, String value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.STRING);
        if (fields.putIfAbsent(fieldName, () -> cw.writeString(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.BOOLEAN);
        if (fields.putIfAbsent(fieldName, () -> cw.writeBoolean(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByte(@Nonnull String fieldName, byte value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.BYTE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeByte(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setChar(@Nonnull String fieldName, char value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.CHAR);
        if (fields.putIfAbsent(fieldName, () -> cw.writeChar(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDouble(@Nonnull String fieldName, double value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.DOUBLE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDouble(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat(@Nonnull String fieldName, float value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.FLOAT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloat(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShort(@Nonnull String fieldName, short value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.SHORT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeShort(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_BOOLEAN);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableBoolean(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableByte(@Nonnull String fieldName, @Nullable Byte value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_BYTE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableByte(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableDouble(@Nonnull String fieldName, @Nullable Double value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_DOUBLE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableDouble(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat(@Nonnull String fieldName, @Nullable Float value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_FLOAT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableFloat(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt(@Nonnull String fieldName, @Nullable Integer value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_INT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableInt(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableLong(@Nonnull String fieldName, @Nullable Long value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_LONG);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableLong(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableShort(@Nonnull String fieldName, @Nullable Short value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_SHORT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableShort(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.DECIMAL);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDecimal(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.TIME);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTime(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.DATE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDate(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.TIMESTAMP);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestamp(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.TIMESTAMP_WITH_TIMEZONE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampWithTimezone(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.COMPACT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeGenericRecord(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setGenericRecordArray(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.COMPACT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeGenericRecordArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setByteArray(@Nonnull String fieldName, @Nullable byte[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.BYTE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeByteArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setBooleanArray(@Nonnull String fieldName, @Nullable boolean[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.BOOLEAN_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeBooleanArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setCharArray(@Nonnull String fieldName, @Nullable char[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.CHAR_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeCharArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setIntArray(@Nonnull String fieldName, @Nullable int[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.INT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeIntArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setLongArray(@Nonnull String fieldName, @Nullable long[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.LONG_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeLongArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDoubleArray(@Nonnull String fieldName, @Nullable double[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.DOUBLE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDoubleArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloatArray(@Nonnull String fieldName, @Nullable float[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.FLOAT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloatArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setShortArray(@Nonnull String fieldName, @Nullable short[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.SHORT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeShortArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }


    @Nonnull
    @Override
    public GenericRecordBuilder setNullableBooleanArray(@Nonnull String fieldName, @Nullable Boolean[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_BOOLEAN_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableBooleanArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableByteArray(@Nonnull String fieldName, @Nullable Byte[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_BYTE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableByteArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableDoubleArray(@Nonnull String fieldName, @Nullable Double[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_DOUBLE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableDoubleArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloatArray(@Nonnull String fieldName, @Nullable Float[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_FLOAT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableFloatArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableIntArray(@Nonnull String fieldName, @Nullable Integer[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_INT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableIntArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableLongArray(@Nonnull String fieldName, @Nullable Long[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_LONG_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableLongArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableShortArray(@Nonnull String fieldName, @Nullable Short[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_SHORT_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableShortArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setStringArray(@Nonnull String fieldName, @Nullable String[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.STRING_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeStringArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.DECIMAL_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDecimalArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.TIME_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimeArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setDateArray(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.DATE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeDateArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.TIMESTAMP_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.TIMESTAMP_WITH_TIMEZONE_ARRAY);
        if (fields.putIfAbsent(fieldName, () -> cw.writeTimestampWithTimezoneArray(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }
}
