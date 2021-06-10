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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.AbstractGenericRecord;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;

import static com.hazelcast.internal.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.nio.serialization.FieldType.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME_ARRAY;

/**
 * Can't be accessed concurrently.
 * Reimplementation of DefaultPortableReader to make it conform InternalGenericRecord.
 * It adds functionality of reading from indexes of  an array to DefaultPortableReader
 */
public class PortableInternalGenericRecord extends AbstractGenericRecord implements InternalGenericRecord {
    protected final ClassDefinition cd;
    protected final PortableSerializer serializer;

    private final BufferObjectDataInput in;
    private final boolean readGenericLazy;
    private final DefaultPortableReader reader;

    PortableInternalGenericRecord(PortableSerializer serializer, BufferObjectDataInput in,
                                  ClassDefinition cd, boolean readGenericLazy) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;
        this.readGenericLazy = readGenericLazy;
        this.reader = new DefaultPortableReader(serializer, in, cd);
    }

    public final void end() {
        reader.end();
    }

    public ClassDefinition getClassDefinition() {
        return cd;
    }

    public int getVersion() {
        return cd.getVersion();
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return cd.hasField(fieldName);
    }

    @Override
    @Nonnull
    public FieldType getFieldType(@Nonnull String fieldName) {
        return cd.getFieldType(fieldName);
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        try {
            return reader.readBoolean(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public byte getByte(@Nonnull String fieldName) {
        try {
            return reader.readByte(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        try {
            return reader.readChar(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        try {
            return reader.readDouble(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        try {
            return reader.readFloat(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        try {
            return reader.readInt(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        try {
            return reader.readLong(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        try {
            return reader.readShort(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public String getString(@Nonnull String fieldName) {
        try {
            return reader.readString(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @FunctionalInterface
    private interface Reader<T, R> {
        R read(T t) throws IOException;
    }


    @Override
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        try {
            return reader.readDecimal(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public LocalTime getTime(@Nonnull String fieldName) {
        try {
            return reader.readTime(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public LocalDate getDate(@Nonnull String fieldName) {
        try {
            return reader.readDate(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        try {
            return reader.readTimestamp(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        try {
            return reader.readTimestampWithTimezone(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    private boolean isNullOrEmpty(int pos) {
        return pos == -1;
    }

    @Override
    public boolean[] getBooleanArray(@Nonnull String fieldName) {
        try {
            return reader.readBooleanArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public byte[] getByteArray(@Nonnull String fieldName) {
        try {
            return reader.readByteArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public char[] getCharArray(@Nonnull String fieldName) {
        try {
            return reader.readCharArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public double[] getDoubleArray(@Nonnull String fieldName) {
        try {
            return reader.readDoubleArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public float[] getFloatArray(@Nonnull String fieldName) {
        try {
            return reader.readFloatArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public int[] getIntArray(@Nonnull String fieldName) {
        try {
            return reader.readIntArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public long[] getLongArray(@Nonnull String fieldName) {
        try {
            return reader.readLongArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public short[] getShortArray(@Nonnull String fieldName) {
        try {
            return reader.readShortArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public String[] getStringArray(@Nonnull String fieldName) {
        try {
            return reader.readStringArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public BigDecimal[] getDecimalArray(@Nonnull String fieldName) {
        try {
            return reader.readDecimalArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public LocalTime[] getTimeArray(@Nonnull String fieldName) {
        try {
            return reader.readTimeArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public LocalDate[] getDateArray(@Nonnull String fieldName) {
        try {
            return reader.readDateArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public LocalDateTime[] getTimestampArray(@Nonnull String fieldName) {
        try {
            return reader.readTimestampArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName) {
        try {
            return reader.readTimestampWithTimezoneArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    private IllegalStateException newIllegalStateException(IOException e) {
        return new IllegalStateException("IOException is not expected since we read from a well known format and position", e);
    }

    private HazelcastSerializationException newUnknownFieldException(@Nonnull String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    @Nonnull
    @Override
    public GenericRecordBuilder newBuilder() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public GenericRecordBuilder cloneWithBuilder() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Set<String> getFieldNames() {
        return cd.getFieldNames();
    }

    @Override
    public GenericRecord[] getGenericRecordArray(@Nonnull String fieldName) {
        try {
            return reader.readNestedArray(fieldName, GenericRecord[]::new, false, readGenericLazy);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        try {
            return reader.readNested(fieldName, false, readGenericLazy);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public <T> T[] getObjectArray(@Nonnull String fieldName, Class<T> componentType) {
        try {
            return (T[]) reader.readPortableArray(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public <T> T getObject(@Nonnull String fieldName) {
        try {
            return (T) reader.readPortable(fieldName);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    private boolean doesNotHaveIndex(int beginPosition, int index) {
        try {
            int numberOfItems = in.readInt(beginPosition);
            return numberOfItems <= index;
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }

    }

    @Override
    public Byte getByteFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.BYTE_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readByte(INT_SIZE_IN_BYTES + position + (index * BYTE_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @SuppressFBWarnings({"NP_BOOLEAN_RETURN_NULL"})
    @Override
    public Boolean getBooleanFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.BOOLEAN_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readBoolean(INT_SIZE_IN_BYTES + position + (index * BOOLEAN_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Character getCharFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.CHAR_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readChar(INT_SIZE_IN_BYTES + position + (index * CHAR_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Double getDoubleFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.DOUBLE_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readDouble(INT_SIZE_IN_BYTES + position + (index * DOUBLE_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Float getFloatFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.FLOAT_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readFloat(INT_SIZE_IN_BYTES + position + (index * FLOAT_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Integer getIntFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.INT_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readInt(INT_SIZE_IN_BYTES + position + (index * INT_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Long getLongFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.LONG_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readLong(INT_SIZE_IN_BYTES + position + (index * LONG_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Short getShortFromArray(@Nonnull String fieldName, int index) {
        try {
            int position = reader.readPosition(fieldName, FieldType.SHORT_ARRAY);
            if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
                return null;
            }
            return in.readShort(INT_SIZE_IN_BYTES + position + (index * SHORT_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public String getStringFromArray(@Nonnull String fieldName, int index) {
        int currentPos = in.position();
        try {
            int pos = reader.readPosition(fieldName, FieldType.UTF_ARRAY);
            in.position(pos);
            int length = in.readInt();
            if (length <= index) {
                return null;
            }
            if (isNullOrEmpty(pos)) {
                return null;
            }
            for (int i = 0; i < index; i++) {
                int itemLength = in.readInt();
                if (itemLength > 0) {
                    in.position(in.position() + itemLength);
                }
            }
            return in.readString();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return readNestedFromArray(fieldName, index, false);
    }

    @Override
    public Object getObjectFromArray(@Nonnull String fieldName, int index) {
        return readNestedFromArray(fieldName, index, true);
    }

    private <T> T readNestedFromArray(@Nonnull String fieldName, int index, boolean asPortable) {
        int currentPos = in.position();
        try {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd == null) {
                throw newUnknownFieldException(fieldName);
            }
            if (fd.getType() != FieldType.PORTABLE_ARRAY) {
                throw new HazelcastSerializationException("Not a Portable array field: " + fieldName);
            }

            int position = reader.readPosition(fd);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            int len = in.readInt();
            if (len == Bits.NULL_ARRAY_LENGTH || len == 0 || len <= index) {
                return null;
            }
            int factoryId = in.readInt();
            int classId = in.readInt();

            reader.checkFactoryAndClass(fd, factoryId, classId);

            int offset = in.position();
            int start = in.readInt(offset + index * Bits.INT_SIZE_IN_BYTES);
            in.position(start);
            if (asPortable) {
                return serializer.readAsObject(in, factoryId, classId);
            } else {
                return serializer.readAndInitialize(in, factoryId, classId, readGenericLazy);
            }
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    private <T> T readObjectFromArrayField(@Nonnull String fieldName, FieldType fieldType,
                                           Reader<ObjectDataInput, T> fieldReader, int index) {
        int currentPos = in.position();
        try {
            int position = reader.readPosition(fieldName, fieldType);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            int len = in.readInt();
            if (len == Bits.NULL_ARRAY_LENGTH || len == 0 || len <= index) {
                return null;
            }

            int offset = in.position();
            int pos = in.readInt(offset + index * Bits.INT_SIZE_IN_BYTES);
            in.position(pos);
            return fieldReader.read(in);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, DECIMAL_ARRAY, IOUtil::readBigDecimal, index);
    }

    @Override
    public LocalTime getTimeFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, TIME_ARRAY, IOUtil::readLocalTime, index);
    }

    @Override
    public LocalDate getDateFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, DATE_ARRAY, IOUtil::readLocalDate, index);
    }

    @Override
    public LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, TIMESTAMP_ARRAY, IOUtil::readLocalDateTime, index);
    }

    @Override
    public OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, IOUtil::readOffsetDateTime, index);
    }

    @Override
    protected Object getClassIdentifier() {
        return cd;
    }
}
