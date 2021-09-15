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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.impl.FieldOperations;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.BYTE_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.BYTE_OFFSET_READER_RANGE;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.INT_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.NULL_OFFSET;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.SHORT_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.SHORT_OFFSET_READER_RANGE;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.BYTE;
import static com.hazelcast.nio.serialization.FieldKind.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.CHAR;
import static com.hazelcast.nio.serialization.FieldKind.CHAR_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.INT;
import static com.hazelcast.nio.serialization.FieldKind.INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.LONG;
import static com.hazelcast.nio.serialization.FieldKind.LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.SHORT;
import static com.hazelcast.nio.serialization.FieldKind.SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.STRING_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldKind.TIME_ARRAY;

/**
 * A base class that has the capability of representing Compact serialized
 * objects as {@link InternalGenericRecord}s. This class is not instantiated
 * directly, but its subclass {@link DefaultCompactReader} is used in the
 * query system, as well as in returning a GenericRecord to the user.
 */
public class CompactInternalGenericRecord extends CompactGenericRecord implements InternalGenericRecord {

    private final OffsetReader offsetReader;
    private final Schema schema;
    private final BufferObjectDataInput in;
    private final int finalPosition;
    private final int dataStartPosition;
    private final int variableOffsetsPosition;
    private final CompactStreamSerializer serializer;
    private final boolean schemaIncludedInBinary;
    private final @Nullable
    Class associatedClass;

    public CompactInternalGenericRecord(CompactStreamSerializer serializer, BufferObjectDataInput in, Schema schema,
                                        @Nullable Class associatedClass, boolean schemaIncludedInBinary) {
        this.in = in;
        this.serializer = serializer;
        this.schema = schema;
        this.associatedClass = associatedClass;
        this.schemaIncludedInBinary = schemaIncludedInBinary;
        try {
            int numberOfVariableLengthFields = schema.getNumberOfVariableSizeFields();
            if (numberOfVariableLengthFields != 0) {
                int dataLength = in.readInt();
                dataStartPosition = in.position();
                variableOffsetsPosition = dataStartPosition + dataLength;
                if (dataLength < BYTE_OFFSET_READER_RANGE) {
                    offsetReader = BYTE_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + numberOfVariableLengthFields;
                } else if (dataLength < SHORT_OFFSET_READER_RANGE) {
                    offsetReader = SHORT_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + (numberOfVariableLengthFields * SHORT_SIZE_IN_BYTES);
                } else {
                    offsetReader = INT_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + (numberOfVariableLengthFields * INT_SIZE_IN_BYTES);
                }
                //set the position to final so that the next one to read something from `in` can start from
                //correct position
                in.position(finalPosition);
            } else {
                offsetReader = INT_OFFSET_READER;
                variableOffsetsPosition = 0;
                dataStartPosition = in.position();
                finalPosition = dataStartPosition + schema.getFixedSizeFieldsLength();
            }
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Nullable
    public Class getAssociatedClass() {
        return associatedClass;
    }

    public BufferObjectDataInput getIn() {
        return in;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder newBuilder() {
        return serializer.createGenericRecordBuilder(schema);
    }

    @Override
    @Nonnull
    public GenericRecordBuilder cloneWithBuilder() {
        return serializer.createGenericRecordCloner(schema, this);
    }

    @Override
    @Nonnull
    public FieldKind getFieldKind(@Nonnull String fieldName) {
        return schema.getField(fieldName).getKind();
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return schema.hasField(fieldName);
    }

    @Nonnull
    @Override
    public Set<String> getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public byte getByte(@Nonnull String fieldName) {
        try {
            return in.readByte(readFixedSizePosition(fieldName, BYTE));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    boolean isFieldExists(@Nonnull String fieldName, @Nonnull FieldKind kind) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            return false;
        }
        return field.getKind() == kind;
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        try {
            return in.readShort(readFixedSizePosition(fieldName, SHORT));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        try {
            return in.readInt(readFixedSizePosition(fieldName, INT));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        try {
            return in.readLong(readFixedSizePosition(fieldName, LONG));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        try {
            return in.readFloat(readFixedSizePosition(fieldName, FLOAT));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        try {
            return in.readDouble(readFixedSizePosition(fieldName, DOUBLE));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        try {
            FieldDescriptor fd = getFieldDefinition(fieldName, BOOLEAN);
            int booleanOffset = fd.getOffset();
            int bitOffset = fd.getBitOffset();
            int getOffset = booleanOffset + dataStartPosition;
            byte lastByte = in.readByte(getOffset);
            return ((lastByte >>> bitOffset) & 1) != 0;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        try {
            return in.readChar(readFixedSizePosition(fieldName, CHAR));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public String getString(@Nonnull String fieldName) {
        return getVariableLength(fieldName, STRING, BufferObjectDataInput::readString);
    }

    private <T> T getVariableLength(@Nonnull String fieldName, FieldKind fieldKind,
                                    Reader<T> reader) {
        int currentPos = in.position();
        try {
            int pos = readVariableSizeFieldPosition(fieldName, fieldKind);
            if (pos == NULL_OFFSET) {
                return null;
            }
            in.position(pos);
            return reader.read(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        return getVariableLength(fieldName, DECIMAL, IOUtil::readBigDecimal);
    }

    @Override
    @Nonnull
    public LocalTime getTime(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readFixedSizePosition(fieldName, TIME));
            return IOUtil.readLocalTime(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @Nonnull
    public LocalDate getDate(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readFixedSizePosition(fieldName, DATE));
            return IOUtil.readLocalDate(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @Nonnull
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readFixedSizePosition(fieldName, TIMESTAMP));
            return IOUtil.readLocalDateTime(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @Nonnull
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readFixedSizePosition(fieldName, TIMESTAMP_WITH_TIMEZONE));
            return IOUtil.readOffsetDateTime(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }


    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return getVariableLength(fieldName, COMPACT, in -> serializer.readGenericRecord(in, schemaIncludedInBinary));
    }

    @Override
    public <T> T getObject(@Nonnull String fieldName) {
        return (T) getVariableLength(fieldName, COMPACT, in -> serializer.read(in, schemaIncludedInBinary));
    }

    @Override
    public byte[] getByteArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, BYTE_ARRAY, ObjectDataInput::readByteArray);
    }

    @Override
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, BOOLEAN_ARRAY, CompactInternalGenericRecord::readBooleanBits);
    }

    @Override
    public char[] getCharArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, CHAR_ARRAY, ObjectDataInput::readCharArray);
    }

    @Override
    public int[] getIntArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, INT_ARRAY, ObjectDataInput::readIntArray);
    }

    @Override
    public long[] getLongArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, LONG_ARRAY, ObjectDataInput::readLongArray);
    }

    @Override
    public double[] getDoubleArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, DOUBLE_ARRAY, ObjectDataInput::readDoubleArray);
    }

    @Override
    public float[] getFloatArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, FLOAT_ARRAY, ObjectDataInput::readFloatArray);
    }

    @Override
    public short[] getShortArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, SHORT_ARRAY, ObjectDataInput::readShortArray);
    }

    @Override
    public String[] getStringArray(@Nonnull String fieldName) {
        return getVariableSizeArray(fieldName, STRING_ARRAY, String[]::new, ObjectDataInput::readString);
    }

    @Override
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        return getVariableSizeArray(fieldName, DECIMAL_ARRAY, BigDecimal[]::new, IOUtil::readBigDecimal);
    }

    @Override
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIME_ARRAY, CompactInternalGenericRecord::getTimeArray);
    }

    @Override
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, DATE_ARRAY, CompactInternalGenericRecord::getDateArray);
    }

    @Override
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIMESTAMP_ARRAY, CompactInternalGenericRecord::getTimestampArray);
    }

    @Override
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY,
                CompactInternalGenericRecord::getTimestampWithTimezoneArray);
    }

    @Override
    public GenericRecord[] getGenericRecordArray(@Nonnull String fieldName) {
        return getVariableSizeArray(fieldName, COMPACT_ARRAY, GenericRecord[]::new,
                in -> serializer.readGenericRecord(in, schemaIncludedInBinary));
    }

    @Override
    public <T> T[] getObjectArray(@Nonnull String fieldName, Class<T> componentType) {
        return (T[]) getVariableSizeArray(fieldName, COMPACT_ARRAY,
                length -> (T[]) Array.newInstance(componentType, length),
                in -> serializer.read(in, schemaIncludedInBinary));
    }

    protected interface Reader<R> {
        R read(BufferObjectDataInput t) throws IOException;
    }

    private <T> T[] getVariableSizeArray(@Nonnull String fieldName, FieldKind fieldKind,
                                         Function<Integer, T[]> constructor,
                                         Reader<T> reader) {
        int currentPos = in.position();
        try {
            int position = readVariableSizeFieldPosition(fieldName, fieldKind);
            if (position == NULL_ARRAY_LENGTH) {
                return null;
            }
            in.position(position);
            int itemCount = in.readInt();
            int dataLength = in.readInt();
            int dataStartPosition = in.position();
            T[] values = constructor.apply(itemCount);

            OffsetReader offsetReader = getOffsetReader(dataLength);
            int offsetsPosition = dataStartPosition + dataLength;
            for (int i = 0; i < itemCount; i++) {
                int offset = offsetReader.getOffset(in, offsetsPosition, i);
                if (offset != NULL_ARRAY_LENGTH) {
                    in.position(offset + dataStartPosition);
                    values[i] = reader.read(in);
                }
            }
            return values;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    private static OffsetReader getOffsetReader(int dataLength) {
        if (dataLength < BYTE_OFFSET_READER_RANGE) {
            return BYTE_OFFSET_READER;
        } else if (dataLength < SHORT_OFFSET_READER_RANGE) {
            return SHORT_OFFSET_READER;
        } else {
            return INT_OFFSET_READER;
        }
    }

    private int readFixedSizePosition(@Nonnull String fieldName, FieldKind fieldKind) {
        FieldDescriptor fd = getFieldDefinition(fieldName, fieldKind);
        int primitiveOffset = fd.getOffset();
        return primitiveOffset + dataStartPosition;
    }

    @Nonnull
    private FieldDescriptor getFieldDefinition(@Nonnull String fieldName, FieldKind fieldKind) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        if (fd.getKind() != fieldKind) {
            throw new HazelcastSerializationException("Not a '" + fieldKind + "' field: " + fieldName);
        }
        return fd;
    }

    private int readVariableSizeFieldPosition(@Nonnull String fieldName, FieldKind fieldKind) {
        try {
            FieldDescriptor fd = getFieldDefinition(fieldName, fieldKind);
            int index = fd.getIndex();
            int offset = offsetReader.getOffset(in, variableOffsetsPosition, index);
            return offset == NULL_OFFSET ? NULL_OFFSET : offset + dataStartPosition;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    private HazelcastSerializationException throwUnknownFieldException(@Nonnull String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for " + schema);
    }

    //indexed methods//

    private int readLength(int beginPosition) {
        try {
            return in.readInt(beginPosition);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    public Byte getByteFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, BYTE_ARRAY, ObjectDataInput::readByte, index);
    }

    public Boolean getBooleanFromArray(@Nonnull String fieldName, int index) {
        int position = readVariableSizeFieldPosition(fieldName, BOOLEAN_ARRAY);
        if (position == NULL_OFFSET) {
            return null;
        }
        if (readLength(position) <= index) {
            return null;
        }
        int currentPos = in.position();
        try {
            int booleanOffsetInBytes = index / Byte.SIZE;
            int booleanOffsetWithinLastByte = index % Byte.SIZE;
            byte b = in.readByte(INT_SIZE_IN_BYTES + position + booleanOffsetInBytes);
            return ((b >>> booleanOffsetWithinLastByte) & 1) != 0;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    public Character getCharFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, CHAR_ARRAY, ObjectDataInput::readChar, index);
    }

    public Integer getIntFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, INT_ARRAY, ObjectDataInput::readInt, index);
    }

    public Long getLongFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, LONG_ARRAY, ObjectDataInput::readLong, index);
    }

    public Double getDoubleFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, DOUBLE_ARRAY, ObjectDataInput::readDouble, index);
    }

    public Float getFloatFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, FLOAT_ARRAY, ObjectDataInput::readFloat, index);
    }

    public Short getShortFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, SHORT_ARRAY, ObjectDataInput::readShort, index);
    }

    private <T> T getFixedSizeFieldFromArray(@Nonnull String fieldName, FieldKind fieldKind,
                                             Reader<T> reader, int index) {
        checkNotNegative(index, "Array indexes can not be negative");

        int position = readVariableSizeFieldPosition(fieldName, fieldKind);
        if (position == NULL_OFFSET) {
            return null;
        }
        if (readLength(position) <= index) {
            return null;
        }
        int currentPos = in.position();
        try {
            FieldKind singleType = FieldOperations.getSingleType(fieldKind);
            int typeSize = FieldOperations.fieldOperations(singleType).typeSizeInBytes();
            in.position(INT_SIZE_IN_BYTES + position + index * typeSize);
            return reader.read(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public String getStringFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, STRING_ARRAY, BufferObjectDataInput::readString, index);
    }

    @Override
    public GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, COMPACT_ARRAY,
                in -> serializer.readGenericRecord(in, schemaIncludedInBinary), index);
    }

    @Override
    public BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, DECIMAL_ARRAY, IOUtil::readBigDecimal, index);
    }

    @Override
    public LocalTime getTimeFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, TIME_ARRAY, IOUtil::readLocalTime, index);
    }

    @Override
    public LocalDate getDateFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, DATE_ARRAY, IOUtil::readLocalDate, index);
    }

    @Override
    public LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, TIMESTAMP_ARRAY, IOUtil::readLocalDateTime, index);
    }

    @Override
    public OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, IOUtil::readOffsetDateTime, index);
    }

    @Override
    public Object getObjectFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, COMPACT_ARRAY,
                in -> serializer.read(in, schemaIncludedInBinary), index);
    }

    private <T> T getVarSizeFromArray(@Nonnull String fieldName, FieldKind fieldKind,
                                      Reader<T> reader, int index) {
        int currentPos = in.position();
        try {
            int pos = readVariableSizeFieldPosition(fieldName, fieldKind);

            if (pos == NULL_OFFSET) {
                return null;
            }
            int itemCount = in.readInt(pos);
            checkNotNegative(index, "Array index can not be negative");
            if (itemCount <= index) {
                return null;
            }
            int dataLength = in.readInt(pos + INT_SIZE_IN_BYTES);
            int dataStartPosition = pos + (2 * INT_SIZE_IN_BYTES);
            OffsetReader offsetReader = getOffsetReader(dataLength);
            int offsetsPosition = dataStartPosition + dataLength;
            int indexedItemOffset = offsetReader.getOffset(in, offsetsPosition, index);
            if (indexedItemOffset != NULL_OFFSET) {
                in.position(indexedItemOffset + dataStartPosition);
                return reader.read(in);
            }
            return null;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    protected Object getClassIdentifier() {
        return schema.getTypeName();
    }

    protected IllegalStateException illegalStateException(IOException e) {
        return new IllegalStateException("IOException is not expected since we get from a well known format and position");
    }

    private static LocalDate[] getDateArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        LocalDate[] values = new LocalDate[len];
        for (int i = 0; i < len; i++) {
            values[i] = IOUtil.readLocalDate(in);
        }
        return values;
    }

    private static LocalTime[] getTimeArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        LocalTime[] values = new LocalTime[len];
        for (int i = 0; i < len; i++) {
            values[i] = IOUtil.readLocalTime(in);
        }
        return values;
    }

    private static LocalDateTime[] getTimestampArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        LocalDateTime[] values = new LocalDateTime[len];
        for (int i = 0; i < len; i++) {
            values[i] = IOUtil.readLocalDateTime(in);
        }
        return values;
    }

    private static OffsetDateTime[] getTimestampWithTimezoneArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        OffsetDateTime[] values = new OffsetDateTime[len];
        for (int i = 0; i < len; i++) {
            values[i] = IOUtil.readOffsetDateTime(in);
        }
        return values;
    }

    private static boolean[] readBooleanBits(BufferObjectDataInput input) throws IOException {
        int len = input.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len == 0) {
            return new boolean[0];
        }
        boolean[] values = new boolean[len];
        int index = 0;
        byte currentByte = input.readByte();
        for (int i = 0; i < len; i++) {
            if (index == Byte.SIZE) {
                index = 0;
                currentByte = input.readByte();
            }
            boolean result = ((currentByte >>> index) & 1) != 0;
            index++;
            values[i] = result;
        }
        return values;
    }
}
