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
    public GenericRecordBuilder setInt32(@Nonnull String fieldName, int value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.INT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeInt32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setInt64(@Nonnull String fieldName, long value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.INT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeInt64(fieldName, value)) != null) {
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
    public GenericRecordBuilder setInt8(@Nonnull String fieldName, byte value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.INT8);
        if (fields.putIfAbsent(fieldName, () -> cw.writeInt8(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
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
        checkTypeWithSchema(schema, fieldName, FieldKind.FLOAT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloat64(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setFloat32(@Nonnull String fieldName, float value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.FLOAT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeFloat32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setInt16(@Nonnull String fieldName, short value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.INT16);
        if (fields.putIfAbsent(fieldName, () -> cw.writeInt16(fieldName, value)) != null) {
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
    public GenericRecordBuilder setNullableInt8(@Nonnull String fieldName, @Nullable Byte value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_INT8);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableInt8(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat64(@Nonnull String fieldName, @Nullable Double value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_FLOAT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableFloat64(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableFloat32(@Nonnull String fieldName, @Nullable Float value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_FLOAT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableFloat32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt32(@Nonnull String fieldName, @Nullable Integer value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_INT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableInt32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt64(@Nonnull String fieldName, @Nullable Long value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_INT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableInt64(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setNullableInt16(@Nonnull String fieldName, @Nullable Short value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.NULLABLE_INT16);
        if (fields.putIfAbsent(fieldName, () -> cw.writeNullableInt16(fieldName, value)) != null) {
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
    public GenericRecordBuilder setArrayOfGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_COMPACT);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfGenericRecord(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_INT8);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfInt8(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_BOOLEAN);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfBoolean(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
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
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_INT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfInt32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInt64(@Nonnull String fieldName, @Nullable long[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_INT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfInt64(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_FLOAT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfFloat64(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_FLOAT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfFloat32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfInt16(@Nonnull String fieldName, @Nullable short[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_INT16);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfInt16(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }


    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableBoolean(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_INT8);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableInt8(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_FLOAT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableFloat64(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_FLOAT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableFloat32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_INT32);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableInt32(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_INT64);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableInt64(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_INT16);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableInt16(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfString(@Nonnull String fieldName, @Nullable String[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_STRING);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfString(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_DECIMAL);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfDecimal(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_TIME);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfTime(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_DATE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfDate(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_TIMESTAMP);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfTimestamp(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfTimestampWithTimezone(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }
}
