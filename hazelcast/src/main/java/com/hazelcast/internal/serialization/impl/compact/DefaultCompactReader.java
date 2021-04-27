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
import com.hazelcast.nio.serialization.compact.CompactReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;
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

public class DefaultCompactReader extends CompactGenericRecord implements InternalGenericRecord, CompactReader {

    protected static final int NULL_POSITION = -1;
    protected final Schema schema;
    protected final BufferObjectDataInput in;
    protected final int finalPosition;
    protected final int offset;
    private final boolean isDebug = System.getProperty("com.hazelcast.serialization.compact.debug") != null;
    private final CompactStreamSerializer serializer;

    private final @Nullable
    Class associatedClass;

    public DefaultCompactReader(CompactStreamSerializer serializer, BufferObjectDataInput in, Schema schema,
                                @Nullable Class associatedClass) {
        this.in = in;
        this.serializer = serializer;
        this.schema = (Schema) schema;
        this.associatedClass = associatedClass;

        try {
            int length = in.readInt();
            offset = in.position();
            finalPosition = length + offset;
            in.position(finalPosition);
            if (isDebug) {
                System.out.println("DEBUG READ " + schema.getTypeName() + " finalPosition " + finalPosition
                        + " offset " + offset + " length " + length);
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
    public GenericRecordBuilder newBuilder() {
        return serializer.createGenericRecordBuilder(schema);
    }

    @Override
    public GenericRecordBuilder cloneWithBuilder() {
        return new SerializingGenericRecordCloner(serializer, schema, this,
                bytes -> serializer.getInternalSerializationService().createObjectDataInput(bytes),
                () -> serializer.getInternalSerializationService().createObjectDataOutput());
    }

    @Override
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
    public byte getByte(String fieldName, byte defaultValue) {
        return isFieldExists(fieldName, BYTE) ? getByte(fieldName) : defaultValue;
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
    public short getShort(String fieldName, short defaultValue) {
        return isFieldExists(fieldName, SHORT) ? getShort(fieldName) : defaultValue;
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
    public int getInt(String fieldName, int defaultValue) {
        return isFieldExists(fieldName, INT) ? getInt(fieldName) : defaultValue;
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
    public long getLong(String fieldName, long defaultValue) {
        return isFieldExists(fieldName, LONG) ? getLong(fieldName) : defaultValue;
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
    public float getFloat(String fieldName, float defaultValue) {
        return isFieldExists(fieldName, FLOAT) ? getFloat(fieldName) : defaultValue;
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
    public double getDouble(String fieldName, double defaultValue) {
        return isFieldExists(fieldName, DOUBLE) ? getDouble(fieldName) : defaultValue;
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        try {
            return in.readBoolean(readPrimitivePosition(fieldName, BOOLEAN));
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public boolean getBoolean(String fieldName, boolean defaultValue) {
        return isFieldExists(fieldName, BOOLEAN) ? getBoolean(fieldName) : defaultValue;
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
    public char getChar(String fieldName, char defaultValue) {
        return isFieldExists(fieldName, CHAR) ? getChar(fieldName) : defaultValue;
    }

    @Override
    public String getString(@Nonnull String fieldName) {
        return getVariableLength(fieldName, UTF, BufferObjectDataInput::readUTF);
    }

    @Override
    public String getString(String fieldName, String defaultValue) {
        return isFieldExists(fieldName, UTF) ? getString(fieldName) : defaultValue;
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
    public BigDecimal getDecimal(String fieldName, BigDecimal defaultValue) {
        return isFieldExists(fieldName, DECIMAL) ? this.getDecimal(fieldName) : defaultValue;
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
    public LocalTime getTime(String fieldName, LocalTime defaultValue) {
        return isFieldExists(fieldName, TIME) ? this.getTime(fieldName) : defaultValue;
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
    public LocalDate getDate(String fieldName, LocalDate defaultValue) {
        return isFieldExists(fieldName, DATE) ? this.getDate(fieldName) : defaultValue;
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
    public LocalDateTime getTimestamp(String fieldName, LocalDateTime defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP) ? this.getTimestamp(fieldName) : defaultValue;
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
    public OffsetDateTime getTimestampWithTimezone(String fieldName, OffsetDateTime defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_WITH_TIMEZONE) ? this.getTimestampWithTimezone(fieldName) : defaultValue;
    }

    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return getVariableLength(fieldName, COMPOSED, serializer::readGenericRecord);
    }

    @Override
    public <T> T getObject(@Nonnull String fieldName) {
        return getVariableLength(fieldName, COMPOSED, serializer::readObject);
    }

    @Override
    public <T> T getObject(String fieldName, T defaultValue) {
        return isFieldExists(fieldName, COMPOSED) ? getObject(fieldName) : defaultValue;
    }

    @Override
    public byte[] getByteArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, BYTE_ARRAY, ObjectDataInput::readByteArray);
    }

    @Override
    public byte[] getByteArray(String fieldName, byte[] defaultValue) {
        return isFieldExists(fieldName, BYTE_ARRAY) ? getByteArray(fieldName) : defaultValue;
    }

    @Override
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, BOOLEAN_ARRAY, ObjectDataInput::readBooleanArray);
    }

    @Override
    public boolean[] getBooleanArray(String fieldName, boolean[] defaultValue) {
        return isFieldExists(fieldName, BOOLEAN_ARRAY) ? getBooleanArray(fieldName) : defaultValue;
    }

    @Override
    public char[] getCharArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, CHAR_ARRAY, ObjectDataInput::readCharArray);
    }

    @Override
    public char[] getCharArray(String fieldName, char[] defaultValue) {
        return isFieldExists(fieldName, CHAR_ARRAY) ? getCharArray(fieldName) : defaultValue;
    }

    @Override
    public int[] getIntArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, INT_ARRAY, ObjectDataInput::readIntArray);
    }

    @Override
    public int[] getIntArray(String fieldName, int[] defaultValue) {
        return isFieldExists(fieldName, INT_ARRAY) ? getIntArray(fieldName) : defaultValue;
    }

    @Override
    public long[] getLongArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, LONG_ARRAY, ObjectDataInput::readLongArray);
    }

    @Override
    public long[] getLongArray(String fieldName, long[] defaultValue) {
        return isFieldExists(fieldName, LONG_ARRAY) ? getLongArray(fieldName) : defaultValue;
    }

    @Override
    public double[] getDoubleArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, DOUBLE_ARRAY, ObjectDataInput::readDoubleArray);
    }

    @Override
    public double[] getDoubleArray(String fieldName, double[] defaultValue) {
        return isFieldExists(fieldName, DOUBLE_ARRAY) ? getDoubleArray(fieldName) : defaultValue;
    }

    @Override
    public float[] getFloatArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, FLOAT_ARRAY, ObjectDataInput::readFloatArray);
    }

    @Override
    public float[] getFloatArray(String fieldName, float[] defaultValue) {
        return isFieldExists(fieldName, FLOAT_ARRAY) ? getFloatArray(fieldName) : defaultValue;
    }

    @Override
    public short[] getShortArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, SHORT_ARRAY, ObjectDataInput::readShortArray);
    }

    @Override
    public short[] getShortArray(String fieldName, short[] defaultValue) {
        return isFieldExists(fieldName, SHORT_ARRAY) ? getShortArray(fieldName) : defaultValue;
    }

    @Override
    public String[] getStringArray(@Nonnull String fieldName) {
        return getVariableLengthArray(fieldName, UTF_ARRAY, String[]::new, ObjectDataInput::readUTF);
    }

    @Override
    public String[] getStringArray(String fieldName, String[] defaultValue) {
        return isFieldExists(fieldName, UTF_ARRAY) ? this.getStringArray(fieldName) : defaultValue;
    }

    @Override
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        return getVariableLengthArray(fieldName, DECIMAL_ARRAY, BigDecimal[]::new, IOUtil::readBigDecimal);
    }

    @Override
    public BigDecimal[] getDecimalArray(String fieldName, BigDecimal[] defaultValue) {
        return isFieldExists(fieldName, DECIMAL_ARRAY) ? this.getDecimalArray(fieldName) : defaultValue;
    }

    @Override
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIME_ARRAY, DefaultCompactReader::getTimeArray);
    }

    @Override
    public LocalTime[] getTimeArray(String fieldName, LocalTime[] defaultValue) {
        return isFieldExists(fieldName, TIME_ARRAY) ? this.getTimeArray(fieldName) : defaultValue;
    }

    @Override
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, DATE_ARRAY, DefaultCompactReader::getDateArray);
    }

    @Override
    public LocalDate[] getDateArray(String fieldName, LocalDate[] defaultValue) {
        return isFieldExists(fieldName, DATE_ARRAY) ? this.getDateArray(fieldName) : defaultValue;
    }

    @Override
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIMESTAMP_ARRAY, DefaultCompactReader::getTimestampArray);
    }

    @Override
    public LocalDateTime[] getTimestampArray(String fieldName, LocalDateTime[] defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_ARRAY) ? this.getTimestampArray(fieldName) : defaultValue;
    }

    @Override
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        return getVariableLength(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, DefaultCompactReader::getTimestampWithTimezoneArray);
    }

    @Override
    public OffsetDateTime[] getTimestampWithTimezoneArray(String fieldName, OffsetDateTime[] defaultValue) {
        return isFieldExists(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY)
                ? this.getTimestampWithTimezoneArray(fieldName) : defaultValue;
    }

    @Override
    public GenericRecord[] getGenericRecordArray(@Nonnull String fieldName) {
        return getVariableLengthArray(fieldName, COMPOSED_ARRAY, GenericRecord[]::new, serializer::readGenericRecord);
    }

    @Override
    public <T> T[] getObjectArray(@Nonnull String fieldName, Class<T> componentType) {
        return getVariableLengthArray(fieldName, COMPOSED_ARRAY,
                length -> (T[]) Array.newInstance(componentType, length), serializer::readObject);
    }

    @Override
    public <T> T[] getObjectArray(String fieldName, Class<T> componentType, T[] defaultValue) {
        return isFieldExists(fieldName, COMPOSED_ARRAY) ? getObjectArray(fieldName, componentType) : defaultValue;
    }

    @Override
    public <T> Collection<T> getObjectCollection(String fieldName, Function<Integer, Collection<T>> constructor) {
        int currentPos = in.position();
        try {
            int position = getPosition(fieldName, COMPOSED_ARRAY);
            if (position == NULL_ARRAY_LENGTH) {
                return null;
            }
            in.position(position);
            int len = in.readInt();
            Collection<T> objects = constructor.apply(len);
            int offset = in.position();
            for (int i = 0; i < len; i++) {
                int pos = in.readInt(offset + i * Bits.INT_SIZE_IN_BYTES);
                if (pos != NULL_ARRAY_LENGTH) {
                    in.position(pos);
                    objects.add(serializer.readObject(in));
                }
            }
            return objects;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public <T> Collection<T> getObjectCollection(String fieldName, Function<Integer, Collection<T>> constructor,
                                                 Collection<T> defaultValue) {
        return isFieldExists(fieldName, COMPOSED_ARRAY) ? getObjectCollection(fieldName, constructor) : defaultValue;
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
        int getOffset = primitiveOffset + offset;
        if (isDebug) {
            System.out.println("DEBUG READ " + schema.getTypeName() + " "
                    + fieldType + " " + fieldName + " " + primitiveOffset + " withOffset " + getOffset);
        }
        return getOffset;
    }

    @Nonnull
    protected FieldDescriptor getFieldDefinition(@Nonnull String fieldName, FieldType fieldType) {
        FieldDescriptor fd = (FieldDescriptor) schema.getField(fieldName);
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
            if (isDebug) {
                System.out.println("DEBUG READ " + schema.getTypeName() + "  "
                        + fieldName + " pos " + pos + " with offset " + pos + offset);
            }
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
        return getFixedSizeFromArray(fieldName, BOOLEAN_ARRAY, ObjectDataInput::readBoolean, index);
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
        return getVarSizeFromArray(fieldName, UTF_ARRAY, BufferObjectDataInput::readUTF, index);
    }

    @Override
    public GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getVarSizeFromArray(fieldName, COMPOSED_ARRAY, serializer::readGenericRecord, index);
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
        return getVarSizeFromArray(fieldName, COMPOSED_ARRAY, serializer::readObject, index);
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


    @Override
    public String toString() {
        return "CompactGenericRecord{"
                + "schema=" + schema
                + ", finalPosition=" + finalPosition
                + ", offset=" + offset
                + '}';
    }
}
