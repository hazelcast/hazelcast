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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.FieldType;
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

public class CompactInternalGenericRecord extends CompactGenericRecord implements InternalGenericRecord {

    private static final int NULL_POSITION = -1;
    private final Schema schema;
    private final BufferObjectDataInput in;
    private final int finalPosition;
    private final int offset;
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
            if (schema.getNumberOfVariableLengthFields() != 0) {
                int length = in.readInt();
                offset = in.position();
                finalPosition = length + offset;
                //set the position to final so that the next one to read something from `in` can start from
                //correct position
                in.position(finalPosition);
            } else {
                offset = in.position();
                finalPosition = offset + schema.getPrimitivesLength();
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
    public FieldType getFieldType(@Nonnull String fieldName) {
        return schema.getField(fieldName).getType();
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
            return in.readByte(readPrimitivePosition(fieldName, BYTE));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    boolean isFieldExists(@Nonnull String fieldName, @Nonnull FieldType type) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            return false;
        }
        return field.getType().equals(type);
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        try {
            return in.readShort(readPrimitivePosition(fieldName, SHORT));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        try {
            return in.readInt(readPrimitivePosition(fieldName, INT));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        try {
            return in.readLong(readPrimitivePosition(fieldName, LONG));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        try {
            return in.readFloat(readPrimitivePosition(fieldName, FLOAT));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        try {
            return in.readDouble(readPrimitivePosition(fieldName, DOUBLE));
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
            int getOffset = booleanOffset + offset;
            byte lastByte = in.readByte(getOffset);
            return ((lastByte >>> bitOffset) & 1) != 0;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        try {
            return in.readChar(readPrimitivePosition(fieldName, CHAR));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public String getString(@Nonnull String fieldName) {
        return getVariableLength(fieldName, UTF, BufferObjectDataInput::readString);
    }

    private <T> T getVariableLength(@Nonnull String fieldName, FieldType fieldType,
                                    Reader<T> geter) {
        int currentPos = in.position();
        try {
            int pos = getPosition(fieldName, fieldType);
            if (pos == NULL_POSITION) {
                return null;
            }
            in.position(pos);
            return geter.read(in);
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
    public LocalTime getTime(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readPrimitivePosition(fieldName, TIME));
            return IOUtil.readLocalTime(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public LocalDate getDate(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readPrimitivePosition(fieldName, DATE));
            return IOUtil.readLocalDate(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readPrimitivePosition(fieldName, TIMESTAMP));
            return IOUtil.readLocalDateTime(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            in.position(readPrimitivePosition(fieldName, TIMESTAMP_WITH_TIMEZONE));
            return IOUtil.readOffsetDateTime(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }


    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return getVariableLength(fieldName, COMPOSED, in -> serializer.readGenericRecord(in, schemaIncludedInBinary));
    }

    @Override
    public <T> T getObject(@Nonnull String fieldName) {
        return (T) getVariableLength(fieldName, COMPOSED, in -> serializer.read(in, schemaIncludedInBinary));
    }

    @Override
    public byte[] getByteArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, BYTE_ARRAY, ObjectDataInput::readByteArray);
    }

    @Override
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, BOOLEAN_ARRAY, DefaultCompactReader::readBooleanBits);
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
        return getVariableLengthArray(fieldName, UTF_ARRAY, String[]::new, ObjectDataInput::readString);
    }

    @Override
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        return getVariableLengthArray(fieldName, DECIMAL_ARRAY, BigDecimal[]::new, IOUtil::readBigDecimal);
    }

    @Override
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIME_ARRAY, DefaultCompactReader::getTimeArray);
    }

    @Override
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, DATE_ARRAY, DefaultCompactReader::getDateArray);
    }

    @Override
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIMESTAMP_ARRAY, DefaultCompactReader::getTimestampArray);
    }

    @Override
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, DefaultCompactReader::getTimestampWithTimezoneArray);
    }

    @Override
    public GenericRecord[] getGenericRecordArray(@Nonnull String fieldName) {
        return getVariableLengthArray(fieldName, COMPOSED_ARRAY, GenericRecord[]::new,
                in -> serializer.readGenericRecord(in, schemaIncludedInBinary));
    }

    @Override
    public <T> T[] getObjectArray(@Nonnull String fieldName, Class<T> componentType) {
        return (T[]) getVariableLengthArray(fieldName, COMPOSED_ARRAY,
                length -> (T[]) Array.newInstance(componentType, length),
                in -> serializer.read(in, schemaIncludedInBinary));
    }

    protected interface Reader<R> {
        R read(BufferObjectDataInput t) throws IOException;
    }

    private <T> T[] getVariableLengthArray(@Nonnull String fieldName, FieldType fieldType,
                                           Function<Integer, T[]> constructor,
                                           Reader<T> reader) {
        int currentPos = in.position();
        try {
            int position = getPosition(fieldName, fieldType);
            if (position == NULL_ARRAY_LENGTH) {
                return null;
            }
            in.position(position);
            int len = in.readInt();
            T[] values = constructor.apply(len);
            int offset = in.position();
            for (int i = 0; i < len; i++) {
                int pos = in.readInt(offset + i * Bits.INT_SIZE_IN_BYTES);
                if (pos != NULL_ARRAY_LENGTH) {
                    in.position(pos);
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

    private int readPrimitivePosition(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor fd = getFieldDefinition(fieldName, fieldType);
        int primitiveOffset = fd.getOffset();
        return primitiveOffset + offset;
    }

    @Nonnull
    protected FieldDescriptor getFieldDefinition(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        if (fd.getType() != fieldType) {
            throw new HazelcastSerializationException("Not a '" + fieldType + "' field: " + fieldName);
        }
        return fd;
    }

    protected int getPosition(@Nonnull String fieldName, FieldType fieldType) {
        try {
            FieldDescriptor fd = getFieldDefinition(fieldName, fieldType);
            int index = fd.getIndex();
            int pos = in.readInt(finalPosition - (index + 1) * INT_SIZE_IN_BYTES);
            return pos == NULL_POSITION ? NULL_POSITION : pos + offset;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    protected HazelcastSerializationException throwUnknownFieldException(@Nonnull String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for " + schema);
    }

    //indexed methods//

    private boolean doesNotHaveIndex(int beginPosition, int index) {
        try {
            int numberOfItems = in.readInt(beginPosition);
            return numberOfItems <= index;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    public Byte getByteFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, BYTE_ARRAY, ObjectDataInput::readByte, index);
    }

    public Boolean getBooleanFromArray(@Nonnull String fieldName, int index) {
        int position = getPosition(fieldName, BOOLEAN_ARRAY);
        if (position == NULL_POSITION || doesNotHaveIndex(position, index)) {
            return null;
        }
        int currentPos = in.position();
        try {
            int booleanOffsetInBytes = index == 0 ? 0 : (index / Byte.SIZE);
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
        return getFixedSizeFromArray(fieldName, CHAR_ARRAY, ObjectDataInput::readChar, index);
    }

    public Integer getIntFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, INT_ARRAY, ObjectDataInput::readInt, index);
    }

    public Long getLongFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, LONG_ARRAY, ObjectDataInput::readLong, index);
    }

    public Double getDoubleFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, DOUBLE_ARRAY, ObjectDataInput::readDouble, index);
    }

    public Float getFloatFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, FLOAT_ARRAY, ObjectDataInput::readFloat, index);
    }

    public Short getShortFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, SHORT_ARRAY, ObjectDataInput::readShort, index);
    }

    private <T> T getFixedSizeFromArray(@Nonnull String fieldName, FieldType fieldType,
                                        Reader<T> geter, int index) {
        int position = getPosition(fieldName, fieldType);
        if (position == NULL_POSITION || doesNotHaveIndex(position, index)) {
            return null;
        }
        int currentPos = in.position();
        try {
            in.position(INT_SIZE_IN_BYTES + position + (index * fieldType.getSingleType().getTypeSize()));
            return geter.read(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public String getStringFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, UTF_ARRAY, BufferObjectDataInput::readString, index);
    }

    @Override
    public GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, COMPOSED_ARRAY,
                in -> serializer.readGenericRecord(in, schemaIncludedInBinary), index);
    }

    @Override
    public BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, DECIMAL_ARRAY, IOUtil::readBigDecimal, index);
    }

    @Override
    public LocalTime getTimeFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, TIME_ARRAY, IOUtil::readLocalTime, index);
    }

    @Override
    public LocalDate getDateFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, DATE_ARRAY, IOUtil::readLocalDate, index);
    }

    @Override
    public LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, TIMESTAMP_ARRAY, IOUtil::readLocalDateTime, index);
    }

    @Override
    public OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFromArray(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, IOUtil::readOffsetDateTime, index);
    }

    @Override
    public Object getObjectFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, COMPOSED_ARRAY,
                in -> serializer.read(in, schemaIncludedInBinary), index);
    }

    private <T> T getVarSizeFromArray(@Nonnull String fieldName, FieldType fieldType,
                                      Reader<T> geter, int index) {
        int currentPos = in.position();
        try {
            int pos = getPosition(fieldName, fieldType);
            if (pos == NULL_POSITION || doesNotHaveIndex(pos, index)) {
                return null;
            }
            int indexedItemPosition = in.readInt((pos + INT_SIZE_IN_BYTES) + index * INT_SIZE_IN_BYTES);
            if (indexedItemPosition != NULL_POSITION) {
                in.position(indexedItemPosition);
                return geter.read(in);
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

    public static LocalDate[] getDateArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            LocalDate[] values = new LocalDate[len];
            for (int i = 0; i < len; i++) {
                values[i] = IOUtil.readLocalDate(in);
            }
            return values;
        }
        return new LocalDate[0];
    }

    public static LocalTime[] getTimeArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            LocalTime[] values = new LocalTime[len];
            for (int i = 0; i < len; i++) {
                values[i] = IOUtil.readLocalTime(in);
            }
            return values;
        }
        return new LocalTime[0];
    }

    public static LocalDateTime[] getTimestampArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            LocalDateTime[] values = new LocalDateTime[len];
            for (int i = 0; i < len; i++) {
                values[i] = IOUtil.readLocalDateTime(in);
            }
            return values;
        }
        return new LocalDateTime[0];
    }

    public static OffsetDateTime[] getTimestampWithTimezoneArray(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            OffsetDateTime[] values = new OffsetDateTime[len];
            for (int i = 0; i < len; i++) {
                values[i] = IOUtil.readOffsetDateTime(in);
            }
            return values;
        }
        return new OffsetDateTime[0];
    }

    static boolean[] readBooleanBits(BufferObjectDataInput input) throws IOException {
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

    @Override
    public String toString() {
        return "CompactGenericRecord{"
                + "schema=" + schema
                + ", finalPosition=" + finalPosition
                + ", offset=" + offset
                + '}';
    }
}
