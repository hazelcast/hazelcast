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
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.BYTE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACTS;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATES;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMALS;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.INT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INTS;
import static com.hazelcast.nio.serialization.FieldKind.LONG;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BYTE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INTS;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_LONG;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_SHORT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.SHORT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRINGS;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMPS;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMES;

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
    public void writeByte(@Nonnull String fieldName, byte value) {
        int position = getFixedSizeFieldPosition(fieldName, BYTE);
        try {
            out.writeByte(position, value);
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
    public void writeFloat(@Nonnull String fieldName, float value) {
        int position = getFixedSizeFieldPosition(fieldName, FLOAT);
        try {
            out.writeFloat(position, value);
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
    public void writeArrayOfBytes(@Nonnull String fieldName, @Nullable byte[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_BYTES, values, ObjectDataOutput::writeByteArray);
    }

    @Override
    public void writeArrayOfBooleans(@Nonnull String fieldName, @Nullable boolean[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_BOOLEANS, values, DefaultCompactWriter::writeBooleanBits);
    }

    @Override
    public void writeArrayOfInts(@Nonnull String fieldName, @Nullable int[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_INTS, values, ObjectDataOutput::writeIntArray);
    }

    @Override
    public void writeArrayOfLongs(@Nonnull String fieldName, @Nullable long[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_LONGS, values, ObjectDataOutput::writeLongArray);
    }

    @Override
    public void writeArrayOfDoubles(@Nonnull String fieldName, @Nullable double[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_DOUBLES, values, ObjectDataOutput::writeDoubleArray);
    }

    @Override
    public void writeArrayOfFloats(@Nonnull String fieldName, @Nullable float[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_FLOATS, values, ObjectDataOutput::writeFloatArray);
    }

    @Override
    public void writeArrayOfShorts(@Nonnull String fieldName, @Nullable short[] values) {
        writeVariableSizeField(fieldName, ARRAY_OF_SHORTS, values, ObjectDataOutput::writeShortArray);
    }

    @Override
    public void writeArrayOfStrings(@Nonnull String fieldName, @Nullable String[] values) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_STRINGS, values, ObjectDataOutput::writeString);
    }

    interface Writer<T> {
        void write(BufferObjectDataOutput out, T value) throws IOException;
    }

    protected <T> void writeArrayOfVariableSizes(@Nonnull String fieldName, @Nonnull FieldKind fieldKind,
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
    public void writeArrayOfDecimals(@Nonnull String fieldName, @Nullable BigDecimal[] values) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_DECIMALS, values, IOUtil::writeBigDecimal);
    }

    @Override
    public void writeArrayOfTimes(@Nonnull String fieldName, @Nullable LocalTime[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_TIMES, value, IOUtil::writeLocalTime);
    }

    @Override
    public void writeArrayOfDates(@Nonnull String fieldName, @Nullable LocalDate[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_DATES, value, IOUtil::writeLocalDate);
    }

    @Override
    public void writeArrayOfTimestamps(@Nonnull String fieldName, @Nullable LocalDateTime[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_TIMESTAMPS, value, IOUtil::writeLocalDateTime);
    }

    @Override
    public void writeArrayOfTimestampWithTimezones(@Nonnull String fieldName, @Nullable OffsetDateTime[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONES, value, IOUtil::writeOffsetDateTime);
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
    public <T> void writeArrayOfCompacts(@Nonnull String fieldName, @Nullable T[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_COMPACTS, value,
                (out, val) -> serializer.writeObject(out, val, includeSchemaOnBinary));
    }

    public void writeArrayOfGenericRecords(@Nonnull String fieldName, @Nullable GenericRecord[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_COMPACTS, value,
                (out, val) -> serializer.writeGenericRecord(out, (CompactGenericRecord) val, includeSchemaOnBinary));
    }

    @Override
    public void writeNullableByte(@Nonnull String fieldName, @Nullable Byte value) {
        writeVariableSizeField(fieldName, NULLABLE_BYTE, value, (Writer<Byte>) DataOutput::writeByte);
    }

    @Override
    public void writeNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value) {
        writeVariableSizeField(fieldName, NULLABLE_BOOLEAN, value, DataOutput::writeBoolean);
    }

    @Override
    public void writeNullableShort(@Nonnull String fieldName, @Nullable Short value) {
        writeVariableSizeField(fieldName, NULLABLE_SHORT, value, (Writer<Short>) DataOutput::writeShort);
    }

    @Override
    public void writeNullableInt(@Nonnull String fieldName, @Nullable Integer value) {
        writeVariableSizeField(fieldName, NULLABLE_INT, value, DataOutput::writeInt);
    }

    @Override
    public void writeNullableLong(@Nonnull String fieldName, @Nullable Long value) {
        writeVariableSizeField(fieldName, NULLABLE_LONG, value, DataOutput::writeLong);
    }

    @Override
    public void writeNullableFloat(@Nonnull String fieldName, @Nullable Float value) {
        writeVariableSizeField(fieldName, NULLABLE_FLOAT, value, DataOutput::writeFloat);
    }

    @Override
    public void writeNullableDouble(@Nonnull String fieldName, @Nullable Double value) {
        writeVariableSizeField(fieldName, NULLABLE_DOUBLE, value, DataOutput::writeDouble);
    }

    @Override
    public void writeArrayOfNullableBytes(@Nonnull String fieldName, @Nullable Byte[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_BYTES, value, (Writer<Byte>) DataOutput::writeByte);
    }

    @Override
    public void writeArrayOfNullableBooleans(@Nonnull String fieldName, @Nullable Boolean[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_BOOLEANS, value, DataOutput::writeBoolean);
    }

    @Override
    public void writeArrayOfNullableShorts(@Nonnull String fieldName, @Nullable Short[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_SHORTS, value, (Writer<Short>) DataOutput::writeShort);
    }

    @Override
    public void writeArrayOfNullableInts(@Nonnull String fieldName, @Nullable Integer[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_INTS, value, DataOutput::writeInt);
    }

    @Override
    public void writeArrayOfNullableLongs(@Nonnull String fieldName, @Nullable Long[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_LONGS, value, DataOutput::writeLong);
    }

    @Override
    public void writeArrayOfNullableFloats(@Nonnull String fieldName, @Nullable Float[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_FLOATS, value, DataOutput::writeFloat);
    }

    @Override
    public void writeArrayOfNullableDoubles(@Nonnull String fieldName, @Nullable Double[] value) {
        writeArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_DOUBLES, value, DataOutput::writeDouble);
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
