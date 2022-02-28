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

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.BYTE_OFFSET_READER_RANGE;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.SHORT_OFFSET_READER_RANGE;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT8;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT32;
import static com.hazelcast.nio.serialization.FieldKind.INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT16;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRING;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIME;

/**
 * Default implementation of the {@link CompactWriter} that writes
 * the serialized fields into a {@link BufferObjectDataOutput}.
 * <p>
 * The writer can also handle compact serializable classes that we want to
 * include schema in it.
 */
public class DefaultCompactWriter implements CompactWriter {

    private final CompactStreamSerializer serializer;
    private final Schema schema;
    private final BufferObjectDataOutput out;
    private final int dataStartPosition;
    private final int[] fieldOffsets;
    private final boolean includeSchemaOnBinary;

    public DefaultCompactWriter(CompactStreamSerializer serializer,
                                BufferObjectDataOutput out, Schema schema, boolean includeSchemaOnBinary) {
        this.serializer = serializer;
        this.out = out;
        this.schema = schema;
        if (schema.getNumberOfVariableSizeFields() != 0) {
            this.fieldOffsets = new int[schema.getNumberOfVariableSizeFields()];
            dataStartPosition = out.position() + INT_SIZE_IN_BYTES;
            // Skip for length and primitives.
            out.writeZeroBytes(schema.getFixedSizeFieldsLength() + INT_SIZE_IN_BYTES);
        } else {
            this.fieldOffsets = null;
            dataStartPosition = out.position();
            // Skip for primitives. No need to write data length, when there is no
            // variable-size fields.
            out.writeZeroBytes(schema.getFixedSizeFieldsLength());
        }
        this.includeSchemaOnBinary = includeSchemaOnBinary;
    }

    /**
     * Returns the byte array representation of the serialized object.
     */
    public byte[] toByteArray() {
        return out.toByteArray();
    }

    /**
     * Ends the serialization of the compact objects by writing
     * the offsets of the variable-size fields as well as the
     * data length, if there are some variable-size fields.
     */
    public void end() {
        try {
            if (schema.getNumberOfVariableSizeFields() == 0) {
                //There are no variable size fields
                return;
            }
            int position = out.position();
            int dataLength = position - dataStartPosition;
            writeOffsets(dataLength, fieldOffsets);
            //write dataLength
            out.writeInt(dataStartPosition - INT_SIZE_IN_BYTES, dataLength);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    private void writeOffsets(int dataLength, int[] offsets) throws IOException {
        if (dataLength < BYTE_OFFSET_READER_RANGE) {
            for (int offset : offsets) {
                out.writeByte(offset);
            }
        } else if (dataLength < SHORT_OFFSET_READER_RANGE) {
            for (int offset : offsets) {
                out.writeShort(offset);
            }
        } else {
            for (int offset : offsets) {
                out.writeInt(offset);
            }
        }
    }

    IllegalStateException illegalStateException(IOException cause) {
        return new IllegalStateException("IOException is not expected from BufferObjectDataOutput ", cause);
    }

    @Override
    public void writeBoolean(@Nonnull String fieldName, boolean value) {
        FieldDescriptor fieldDefinition = checkFieldDefinition(fieldName, BOOLEAN);
        int offsetInBytes = fieldDefinition.getOffset();
        int offsetInBits = fieldDefinition.getBitOffset();
        int writeOffset = offsetInBytes + dataStartPosition;
        try {
            out.writeBooleanBit(writeOffset, offsetInBits, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeInt8(@Nonnull String fieldName, byte value) {
        int position = getFixedSizeFieldPosition(fieldName, INT8);
        try {
            out.writeByte(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeInt16(@Nonnull String fieldName, short value) {
        int position = getFixedSizeFieldPosition(fieldName, INT16);
        try {
            out.writeShort(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeInt32(@Nonnull String fieldName, int value) {
        int position = getFixedSizeFieldPosition(fieldName, INT32);
        try {
            out.writeInt(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeInt64(@Nonnull String fieldName, long value) {
        int position = getFixedSizeFieldPosition(fieldName, INT64);
        try {
            out.writeLong(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeFloat32(@Nonnull String fieldName, float value) {
        int position = getFixedSizeFieldPosition(fieldName, FLOAT32);
        try {
            out.writeFloat(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeFloat64(@Nonnull String fieldName, double value) {
        int position = getFixedSizeFieldPosition(fieldName, FLOAT64);
        try {
            out.writeDouble(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    protected <T> void writeVariableSizeField(@Nonnull String fieldName, @Nonnull FieldKind fieldKind,
                                              @Nullable T object, @Nonnull Writer<T> writer) {
        try {
            if (object == null) {
                setPositionAsNull(fieldName, fieldKind);
            } else {
                setPosition(fieldName, fieldKind);
                writer.write(out, object);
            }
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeString(@Nonnull String fieldName, @Nullable String str) {
        writeVariableSizeField(fieldName, STRING, str, ObjectDataOutput::writeString);
    }

    @Override
    public void writeCompact(@Nonnull String fieldName, @Nullable Object value) {
        writeVariableSizeField(fieldName, COMPACT, value,
                (out, val) -> serializer.writeObject(out, val, includeSchemaOnBinary));
    }

    public void writeGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value) {
        writeVariableSizeField(fieldName, COMPACT, value,
                (out, val) -> serializer.writeGenericRecord(out, (CompactGenericRecord) val, includeSchemaOnBinary));
    }

    @Override
    public void writeDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        writeVariableSizeField(fieldName, DECIMAL, value, IOUtil::writeBigDecimal);
    }

    @Override
    public void writeTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        writeVariableSizeField(fieldName, TIME, value, IOUtil::writeLocalTime);
    }

    @Override
    public void writeDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        writeVariableSizeField(fieldName, DATE, value, IOUtil::writeLocalDate);
    }

    @Override
    public void writeTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        writeVariableSizeField(fieldName, TIMESTAMP, value, IOUtil::writeLocalDateTime);
    }

    @Override
    public void writeTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        writeVariableSizeField(fieldName, TIMESTAMP_WITH_TIMEZONE, value, IOUtil::writeOffsetDateTime);
    }

    @Override
    public void writeArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_INT8, values, ObjectDataOutput::writeByteArray);
    }

    @Override
    public void writeArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_BOOLEAN, values, DefaultCompactWriter::writeBooleanBits);
    }

    @Override
    public void writeArrayOfInt32(@Nonnull String fieldName, @Nullable int[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_INT32, values, ObjectDataOutput::writeIntArray);
    }

    @Override
    public void writeArrayOfInt64(@Nonnull String fieldName, @Nullable long[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_INT64, values, ObjectDataOutput::writeLongArray);
    }

    @Override
    public void writeArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_FLOAT64, values, ObjectDataOutput::writeDoubleArray);
    }

    @Override
    public void writeArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_FLOAT32, values, ObjectDataOutput::writeFloatArray);
    }

    @Override
    public void writeArrayOfInt16(@Nonnull String fieldName, @Nullable short[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_INT16, values, ObjectDataOutput::writeShortArray);
    }

    @Override
    public void writeArrayOfString(@Nonnull String fieldName, @Nullable String[] values) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_STRING, values, ObjectDataOutput::writeString);
    }

    interface Writer<T> {
        void write(BufferObjectDataOutput out, T value) throws IOException;
    }

    protected <T> void writeArrayOfVariableSize(@Nonnull String fieldName, @Nonnull FieldKind fieldKind,
                                                 @Nullable T[] values, @Nonnull Writer<T> writer) {
        if (values == null) {
            setPositionAsNull(fieldName, fieldKind);
            return;
        }
        try {
            setPosition(fieldName, fieldKind);
            int dataLengthOffset = out.position();
            out.writeZeroBytes(INT_SIZE_IN_BYTES);
            int itemCount = values.length;
            out.writeInt(itemCount);

            int offset = out.position();
            int[] offsets = new int[itemCount];
            for (int i = 0; i < itemCount; i++) {
                if (values[i] != null) {
                    offsets[i] = out.position() - offset;
                    writer.write(out, values[i]);
                } else {
                    offsets[i] = -1;
                }
            }
            int dataLength = out.position() - offset;
            out.writeInt(dataLengthOffset, dataLength);
            writeOffsets(dataLength, offsets);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] values) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_DECIMAL, values, IOUtil::writeBigDecimal);
    }

    @Override
    public void writeArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_TIME, value, IOUtil::writeLocalTime);
    }

    @Override
    public void writeArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_DATE, value, IOUtil::writeLocalDate);
    }

    @Override
    public void writeArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_TIMESTAMP, value, IOUtil::writeLocalDateTime);
    }

    @Override
    public void writeArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONE, value, IOUtil::writeOffsetDateTime);
    }

    protected void setPositionAsNull(@Nonnull String fieldName, @Nonnull FieldKind fieldKind) {
        FieldDescriptor field = checkFieldDefinition(fieldName, fieldKind);
        int index = field.getIndex();
        fieldOffsets[index] = -1;
    }

    protected void setPosition(@Nonnull String fieldName, @Nonnull FieldKind fieldKind) {
        FieldDescriptor field = checkFieldDefinition(fieldName, fieldKind);
        int pos = out.position();
        int fieldPosition = pos - dataStartPosition;
        int index = field.getIndex();
        fieldOffsets[index] = fieldPosition;
    }

    private int getFixedSizeFieldPosition(@Nonnull String fieldName, @Nonnull FieldKind fieldKind) {
        FieldDescriptor fieldDefinition = checkFieldDefinition(fieldName, fieldKind);
        return fieldDefinition.getOffset() + dataStartPosition;
    }

    protected FieldDescriptor checkFieldDefinition(@Nonnull String fieldName, @Nonnull FieldKind kind) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + " for " + schema);
        }
        if (field.getKind() != kind) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName + " for " + schema);
        }
        return field;
    }

    @Override
    public <T> void writeArrayOfCompact(@Nonnull String fieldName, @Nullable T[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_COMPACT, value,
                (out, val) -> serializer.writeObject(out, val, includeSchemaOnBinary));
    }

    public void writeArrayOfGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_COMPACT, value,
                (out, val) -> serializer.writeGenericRecord(out, (CompactGenericRecord) val, includeSchemaOnBinary));
    }

    @Override
    public void writeNullableInt8(@Nonnull String fieldName, @Nullable Byte value) {
        writeVariableSizeField(fieldName, NULLABLE_INT8, value, (Writer<Byte>) DataOutput::writeByte);
    }

    @Override
    public void writeNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        writeVariableSizeField(fieldName, NULLABLE_BOOLEAN, value, DataOutput::writeBoolean);
    }

    @Override
    public void writeNullableInt16(@Nonnull String fieldName, @Nullable Short value) {
        writeVariableSizeField(fieldName, NULLABLE_INT16, value, (Writer<Short>) DataOutput::writeShort);
    }

    @Override
    public void writeNullableInt32(@Nonnull String fieldName, @Nullable Integer value) {
        writeVariableSizeField(fieldName, NULLABLE_INT32, value, DataOutput::writeInt);
    }

    @Override
    public void writeNullableInt64(@Nonnull String fieldName, @Nullable Long value) {
        writeVariableSizeField(fieldName, NULLABLE_INT64, value, DataOutput::writeLong);
    }

    @Override
    public void writeNullableFloat32(@Nonnull String fieldName, @Nullable Float value) {
        writeVariableSizeField(fieldName, NULLABLE_FLOAT32, value, DataOutput::writeFloat);
    }

    @Override
    public void writeNullableFloat64(@Nonnull String fieldName, @Nullable Double value) {
        writeVariableSizeField(fieldName, NULLABLE_FLOAT64, value, DataOutput::writeDouble);
    }

    @Override
    public void writeArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_NULLABLE_INT8, value, (Writer<Byte>) DataOutput::writeByte);
    }

    @Override
    public void writeArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_NULLABLE_BOOLEAN, value, DataOutput::writeBoolean);
    }

    @Override
    public void writeArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_NULLABLE_INT16, value, (Writer<Short>) DataOutput::writeShort);
    }

    @Override
    public void writeArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_NULLABLE_INT32, value, DataOutput::writeInt);
    }

    @Override
    public void writeArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_NULLABLE_INT64, value, DataOutput::writeLong);
    }

    @Override
    public void writeArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_NULLABLE_FLOAT32, value, DataOutput::writeFloat);
    }

    @Override
    public void writeArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value) {
        writeArrayOfVariableSize(fieldName, ARRAY_OF_NULLABLE_FLOAT64, value, DataOutput::writeDouble);
    }

    static void writeBooleanBits(BufferObjectDataOutput out, @Nullable boolean[] booleans) throws IOException {
        int len = (booleans != null) ? booleans.length : NULL_ARRAY_LENGTH;
        out.writeInt(len);
        int position = out.position();
        if (len > 0) {
            int index = 0;
            out.writeZeroBytes(1);
            for (boolean v : booleans) {
                if (index == Byte.SIZE) {
                    index = 0;
                    out.writeZeroBytes(1);
                    position++;
                }
                out.writeBooleanBit(position, index, v);
                index++;
            }
        }
    }
}
