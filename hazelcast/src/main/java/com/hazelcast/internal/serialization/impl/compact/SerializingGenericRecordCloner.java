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
        throw new UnsupportedOperationException("Compact format does not support writing a char field");
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
    public GenericRecordBuilder setArrayOfGenericRecords(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_COMPACTS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfGenericRecords(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfBytes(@Nonnull String fieldName, @Nullable byte[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_BYTES);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfBytes(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfBooleans(@Nonnull String fieldName, @Nullable boolean[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_BOOLEANS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfBooleans(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
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
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_INTS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfInts(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfLongs(@Nonnull String fieldName, @Nullable long[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_LONGS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfLongs(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDoubles(@Nonnull String fieldName, @Nullable double[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_DOUBLES);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfDoubles(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfFloats(@Nonnull String fieldName, @Nullable float[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_FLOATS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfFloats(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfShorts(@Nonnull String fieldName, @Nullable short[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_SHORTS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfShorts(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }


    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBooleans(@Nonnull String fieldName, @Nullable Boolean[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_BOOLEANS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableBooleans(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableBytes(@Nonnull String fieldName, @Nullable Byte[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_BYTES);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableBytes(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableDoubles(@Nonnull String fieldName, @Nullable Double[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_DOUBLES);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableDoubles(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableFloats(@Nonnull String fieldName, @Nullable Float[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_FLOATS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableFloats(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableInts(@Nonnull String fieldName, @Nullable Integer[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_INTS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableInts(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableLongs(@Nonnull String fieldName, @Nullable Long[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_LONGS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableLongs(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder setArrayOfNullableShorts(@Nonnull String fieldName, @Nullable Short[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_NULLABLE_SHORTS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfNullableShorts(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfStrings(@Nonnull String fieldName, @Nullable String[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_STRINGS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfStrings(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDecimals(@Nonnull String fieldName, @Nullable BigDecimal[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_DECIMALS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfDecimals(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimes(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_TIMES);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfTimes(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfDates(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_DATES);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfDates(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestamps(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_TIMESTAMPS);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfTimestamps(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder setArrayOfTimestampWithTimezones(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        checkTypeWithSchema(schema, fieldName, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES);
        if (fields.putIfAbsent(fieldName, () -> cw.writeArrayOfTimestampWithTimezones(fieldName, value)) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }
}
