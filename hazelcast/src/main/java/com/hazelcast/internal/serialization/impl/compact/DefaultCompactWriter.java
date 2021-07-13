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

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
import static com.hazelcast.nio.serialization.FieldType.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldType.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.BYTE;
import static com.hazelcast.nio.serialization.FieldType.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.CHAR;
import static com.hazelcast.nio.serialization.FieldType.CHAR_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.COMPOSED;
import static com.hazelcast.nio.serialization.FieldType.COMPOSED_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DATE;
import static com.hazelcast.nio.serialization.FieldType.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.FLOAT;
import static com.hazelcast.nio.serialization.FieldType.FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.INT;
import static com.hazelcast.nio.serialization.FieldType.INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.LONG;
import static com.hazelcast.nio.serialization.FieldType.LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.SHORT;
import static com.hazelcast.nio.serialization.FieldType.SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UTF;
import static com.hazelcast.nio.serialization.FieldType.UTF_ARRAY;

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

    @Override
    public void writeInt(@Nonnull String fieldName, int value) {
        int position = getFixedSizeFieldPosition(fieldName, INT);
        try {
            out.writeInt(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeLong(@Nonnull String fieldName, long value) {
        int position = getFixedSizeFieldPosition(fieldName, LONG);
        try {
            out.writeLong(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
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

    private IllegalStateException illegalStateException(IOException cause) {
        return new IllegalStateException("IOException is not expected from BufferObjectDataOutput ", cause);
    }

    @Override
    public void writeByte(@Nonnull String fieldName, byte value) {
        int position = getFixedSizeFieldPosition(fieldName, BYTE);
        try {
            out.writeByte(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeChar(@Nonnull String fieldName, char value) {
        int position = getFixedSizeFieldPosition(fieldName, CHAR);
        try {
            out.writeChar(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeDouble(@Nonnull String fieldName, double value) {
        int position = getFixedSizeFieldPosition(fieldName, DOUBLE);
        try {
            out.writeDouble(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeFloat(@Nonnull String fieldName, float value) {
        int position = getFixedSizeFieldPosition(fieldName, FLOAT);
        try {
            out.writeFloat(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeShort(@Nonnull String fieldName, short value) {
        int position = getFixedSizeFieldPosition(fieldName, SHORT);
        try {
            out.writeShort(position, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    protected <T> void writeVariableSizeField(@Nonnull String fieldName, FieldType fieldType, T object, Writer<T> writer) {
        try {
            if (object == null) {
                setPositionAsNull(fieldName, fieldType);
            } else {
                setPosition(fieldName, fieldType);
                writer.write(out, object);
            }
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public void writeString(@Nonnull String fieldName, String str) {
        writeVariableSizeField(fieldName, UTF, str, ObjectDataOutput::writeString);
    }

    @Override
    public void writeObject(@Nonnull String fieldName, Object value) {
        writeVariableSizeField(fieldName, COMPOSED, value,
                (out, val) -> serializer.writeObject(out, val, includeSchemaOnBinary));
    }

    public void writeGenericRecord(@Nonnull String fieldName, GenericRecord value) {
        writeVariableSizeField(fieldName, COMPOSED, value,
                (out, val) -> serializer.writeGenericRecord(out, (CompactGenericRecord) val, includeSchemaOnBinary));
    }

    @Override
    public void writeDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        writeVariableSizeField(fieldName, DECIMAL, value, IOUtil::writeBigDecimal);
    }

    @Override
    public void writeTime(@Nonnull String fieldName, @Nonnull LocalTime value) {
        int lastPos = out.position();
        try {
            out.position(getFixedSizeFieldPosition(fieldName, TIME));
            IOUtil.writeLocalTime(out, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            out.position(lastPos);
        }
    }

    @Override
    public void writeDate(@Nonnull String fieldName, @Nonnull LocalDate value) {
        int lastPos = out.position();
        try {
            out.position(getFixedSizeFieldPosition(fieldName, DATE));
            IOUtil.writeLocalDate(out, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            out.position(lastPos);
        }
    }

    @Override
    public void writeTimestamp(@Nonnull String fieldName, @Nonnull LocalDateTime value) {
        int lastPos = out.position();
        try {
            out.position(getFixedSizeFieldPosition(fieldName, TIMESTAMP));
            IOUtil.writeLocalDateTime(out, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            out.position(lastPos);
        }
    }

    @Override
    public void writeTimestampWithTimezone(@Nonnull String fieldName, @Nonnull OffsetDateTime value) {
        int lastPos = out.position();
        try {
            out.position(getFixedSizeFieldPosition(fieldName, TIMESTAMP_WITH_TIMEZONE));
            IOUtil.writeOffsetDateTime(out, value);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            out.position(lastPos);
        }
    }

    @Override
    public void writeByteArray(@Nonnull String fieldName, @Nullable byte[] values) {
        writeVariableSizeField(fieldName, BYTE_ARRAY, values, ObjectDataOutput::writeByteArray);
    }

    @Override
    public void writeBooleanArray(@Nonnull String fieldName, @Nullable boolean[] values) {
        writeVariableSizeField(fieldName, BOOLEAN_ARRAY, values, DefaultCompactWriter::writeBooleanBits);
    }

    @Override
    public void writeCharArray(@Nonnull String fieldName, @Nullable char[] values) {
        writeVariableSizeField(fieldName, CHAR_ARRAY, values, ObjectDataOutput::writeCharArray);
    }

    @Override
    public void writeIntArray(@Nonnull String fieldName, @Nullable int[] values) {
        writeVariableSizeField(fieldName, INT_ARRAY, values, ObjectDataOutput::writeIntArray);
    }

    @Override
    public void writeLongArray(@Nonnull String fieldName, @Nullable long[] values) {
        writeVariableSizeField(fieldName, LONG_ARRAY, values, ObjectDataOutput::writeLongArray);
    }

    @Override
    public void writeDoubleArray(@Nonnull String fieldName, @Nullable double[] values) {
        writeVariableSizeField(fieldName, DOUBLE_ARRAY, values, ObjectDataOutput::writeDoubleArray);
    }

    @Override
    public void writeFloatArray(@Nonnull String fieldName, @Nullable float[] values) {
        writeVariableSizeField(fieldName, FLOAT_ARRAY, values, ObjectDataOutput::writeFloatArray);
    }

    @Override
    public void writeShortArray(@Nonnull String fieldName, @Nullable short[] values) {
        writeVariableSizeField(fieldName, SHORT_ARRAY, values, ObjectDataOutput::writeShortArray);
    }

    @Override
    public void writeStringArray(@Nonnull String fieldName, @Nullable String[] values) {
        writeVariableSizeArray(fieldName, UTF_ARRAY, values, ObjectDataOutput::writeString);
    }

    interface Writer<T> {
        void write(BufferObjectDataOutput out, T value) throws IOException;
    }

    protected <T> void writeVariableSizeArray(@Nonnull String fieldName, FieldType fieldType, T[] values, Writer<T> writer) {
        if (values == null) {
            setPositionAsNull(fieldName, fieldType);
            return;
        }
        try {
            setPosition(fieldName, fieldType);
            int itemCount = values.length;
            out.writeInt(itemCount);
            int dataLengthOffset = out.position();
            out.writeZeroBytes(INT_SIZE_IN_BYTES);

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
    public void writeDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] values) {
        writeVariableSizeArray(fieldName, DECIMAL_ARRAY, values, IOUtil::writeBigDecimal);
    }

    @Override
    public void writeTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] values) {
        writeVariableSizeField(fieldName, TIME_ARRAY, values, DefaultCompactWriter::writeLocalTimeArray0);
    }

    @Override
    public void writeDateArray(@Nonnull String fieldName, @Nullable LocalDate[] values) {
        writeVariableSizeField(fieldName, DATE_ARRAY, values, DefaultCompactWriter::writeLocalDateArray0);
    }

    @Override
    public void writeTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] values) {
        writeVariableSizeField(fieldName, TIMESTAMP_ARRAY, values, DefaultCompactWriter::writeLocalDateTimeArray0);
    }

    @Override
    public void writeTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] values) {
        writeVariableSizeField(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, values, DefaultCompactWriter::writeOffsetDateTimeArray0);
    }

    protected void setPositionAsNull(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor field = checkFieldDefinition(fieldName, fieldType);
        int index = field.getIndex();
        fieldOffsets[index] = -1;
    }

    protected void setPosition(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor field = checkFieldDefinition(fieldName, fieldType);
        int pos = out.position();
        int fieldPosition = pos - dataStartPosition;
        int index = field.getIndex();
        fieldOffsets[index] = fieldPosition;
    }

    private int getFixedSizeFieldPosition(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor fieldDefinition = checkFieldDefinition(fieldName, fieldType);
        return fieldDefinition.getOffset() + dataStartPosition;
    }

    protected FieldDescriptor checkFieldDefinition(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + " for " + schema);
        }
        if (!field.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName + " for " + schema);
        }
        return field;
    }

    @Override
    public <T> void writeObjectArray(@Nonnull String fieldName, T[] values) {
        writeVariableSizeArray(fieldName, COMPOSED_ARRAY, values,
                (out, val) -> serializer.writeObject(out, val, includeSchemaOnBinary));
    }

    public void writeGenericRecordArray(@Nonnull String fieldName, GenericRecord[] values) {
        writeVariableSizeArray(fieldName, COMPOSED_ARRAY, values,
                (out, val) -> serializer.writeGenericRecord(out, (CompactGenericRecord) val, includeSchemaOnBinary));
    }

    private static void writeLocalDateArray0(ObjectDataOutput out, LocalDate[] value) throws IOException {
        out.writeInt(value.length);
        for (LocalDate localDate : value) {
            IOUtil.writeLocalDate(out, localDate);
        }
    }

    private static void writeLocalTimeArray0(ObjectDataOutput out, LocalTime[] value) throws IOException {
        out.writeInt(value.length);
        for (LocalTime localTime : value) {
            IOUtil.writeLocalTime(out, localTime);
        }
    }

    private static void writeLocalDateTimeArray0(ObjectDataOutput out, LocalDateTime[] value) throws IOException {
        out.writeInt(value.length);
        for (LocalDateTime localDateTime : value) {
            IOUtil.writeLocalDateTime(out, localDateTime);
        }
    }

    private static void writeOffsetDateTimeArray0(ObjectDataOutput out, OffsetDateTime[] value) throws IOException {
        out.writeInt(value.length);
        for (OffsetDateTime offsetDateTime : value) {
            IOUtil.writeOffsetDateTime(out, offsetDateTime);
        }
    }

    private static void writeBooleanBits(BufferObjectDataOutput out, boolean[] booleans) throws IOException {
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
