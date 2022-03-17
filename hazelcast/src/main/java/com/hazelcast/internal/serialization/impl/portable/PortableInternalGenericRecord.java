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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.PortableUtil;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
 * See the javadoc of {@link InternalGenericRecord} for GenericRecord class hierarchy.
 */
public class PortableInternalGenericRecord extends PortableGenericRecord implements InternalGenericRecord {
    protected final ClassDefinition cd;
    protected final PortableSerializer serializer;

    private final BufferObjectDataInput in;
    private final int offset;
    private final int finalPosition;

    PortableInternalGenericRecord(PortableSerializer serializer, BufferObjectDataInput in,
                                  ClassDefinition cd) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;

        int fieldCount;
        try {
            // final position after portable is read
            finalPosition = in.readInt();
            // field count
            fieldCount = in.readInt();
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        }
        if (fieldCount != cd.getFieldCount()) {
            throw new IllegalStateException("Field count[" + fieldCount + "] in stream does not match " + cd);
        }
        this.offset = in.position();
    }

    public final void end() {
        in.position(finalPosition);
    }

    @Override
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
    public FieldKind getFieldKind(@Nonnull String fieldName) {
        return FieldTypeToFieldKind.toFieldKind(cd.getFieldType(fieldName));
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        try {
            return in.readBoolean(readPosition(fieldName, FieldType.BOOLEAN));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public byte getInt8(@Nonnull String fieldName) {
        try {
            return in.readByte(readPosition(fieldName, FieldType.BYTE));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        try {
            return in.readChar(readPosition(fieldName, FieldType.CHAR));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public double getFloat64(@Nonnull String fieldName) {
        try {
            return in.readDouble(readPosition(fieldName, FieldType.DOUBLE));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public float getFloat32(@Nonnull String fieldName) {
        try {
            return in.readFloat(readPosition(fieldName, FieldType.FLOAT));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public int getInt32(@Nonnull String fieldName) {
        try {
            return in.readInt(readPosition(fieldName, FieldType.INT));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public long getInt64(@Nonnull String fieldName) {
        try {
            return in.readLong(readPosition(fieldName, FieldType.LONG));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public short getInt16(@Nonnull String fieldName) {
        try {
            return in.readShort(readPosition(fieldName, FieldType.SHORT));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public String getString(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.UTF);
            in.position(pos);
            return in.readString();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @FunctionalInterface
    private interface Reader<T, R> {
        R read(T t) throws IOException;
    }

    @Nullable
    private <T> T readNullableField(@Nonnull String fieldName, FieldType fieldType, Reader<ObjectDataInput, T> reader) {
        int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, fieldType);
            in.position(pos);
            boolean isNull = in.readBoolean();
            if (isNull) {
                return null;
            }
            return reader.read(in);
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        return readNullableField(fieldName, FieldType.DECIMAL, IOUtil::readBigDecimal);
    }

    @Override
    public LocalTime getTime(@Nonnull String fieldName) {
        return readNullableField(fieldName, FieldType.TIME, PortableUtil::readLocalTime);
    }

    @Override
    public LocalDate getDate(@Nonnull String fieldName) {
        return readNullableField(fieldName, FieldType.DATE, PortableUtil::readLocalDate);
    }

    @Override
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        return readNullableField(fieldName, FieldType.TIMESTAMP, PortableUtil::readLocalDateTime);
    }

    @Override
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        return readNullableField(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE, PortableUtil::readOffsetDateTime);
    }

    private boolean isNullOrEmpty(int pos) {
        return pos == -1;
    }

    @Override
    public boolean[] getArrayOfBoolean(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.BOOLEAN_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readBooleanArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public byte[] getArrayOfInt8(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.BYTE_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readByteArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }

    }

    @Override
    public char[] getArrayOfChar(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.CHAR_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readCharArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public double[] getArrayOfFloat64(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.DOUBLE_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readDoubleArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public float[] getArrayOfFloat32(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.FLOAT_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readFloatArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public int[] getArrayOfInt32(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.INT_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readIntArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public long[] getArrayOfInt64(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.LONG_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readLongArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public short[] getArrayOfInt16(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.SHORT_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readShortArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public String[] getArrayOfString(@Nonnull String fieldName) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, FieldType.UTF_ARRAY);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            return in.readStringArray();
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }


    private <T> T[] readObjectArrayField(@Nonnull String fieldName, FieldType fieldType, Function<Integer, T[]> constructor,
                                         Reader<ObjectDataInput, T> reader) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, fieldType);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            int len = in.readInt();

            if (len == Bits.NULL_ARRAY_LENGTH) {
                return null;
            }

            T[] values = constructor.apply(len);
            if (len > 0) {
                int offset = in.position();
                for (int i = 0; i < len; i++) {
                    int pos = in.readInt(offset + i * Bits.INT_SIZE_IN_BYTES);
                    in.position(pos);
                    values[i] = reader.read(in);
                }
            }
            return values;
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public BigDecimal[] getArrayOfDecimal(@Nonnull String fieldName) {
        return readObjectArrayField(fieldName, DECIMAL_ARRAY, BigDecimal[]::new, IOUtil::readBigDecimal);
    }

    @Override
    public LocalTime[] getArrayOfTime(@Nonnull String fieldName) {
        return readObjectArrayField(fieldName, TIME_ARRAY, LocalTime[]::new, PortableUtil::readLocalTime);
    }

    @Override
    public LocalDate[] getArrayOfDate(@Nonnull String fieldName) {
        return readObjectArrayField(fieldName, DATE_ARRAY, LocalDate[]::new, PortableUtil::readLocalDate);
    }

    @Override
    public LocalDateTime[] getArrayOfTimestamp(@Nonnull String fieldName) {
        return readObjectArrayField(fieldName, TIMESTAMP_ARRAY, LocalDateTime[]::new, PortableUtil::readLocalDateTime);
    }

    @Override
    public OffsetDateTime[] getArrayOfTimestampWithTimezone(@Nonnull String fieldName) {
        return readObjectArrayField(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, OffsetDateTime[]::new,
                PortableUtil::readOffsetDateTime);
    }

    private void checkFactoryAndClass(FieldDefinition fd, int factoryId, int classId) {
        if (factoryId != fd.getFactoryId()) {
            throw new IllegalArgumentException("Invalid factoryId! Expected: "
                    + fd.getFactoryId() + ", Current: " + factoryId);
        }
        if (classId != fd.getClassId()) {
            throw new IllegalArgumentException("Invalid classId! Expected: "
                    + fd.getClassId() + ", Current: " + classId);
        }
    }


    private int readPosition(@Nonnull String fieldName, FieldType fieldType) {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            throw newUnknownFieldException(fieldName);
        }
        if (fd.getType() != fieldType) {
            throw new HazelcastSerializationException("Not a '" + fieldType + "' field: " + fieldName);
        }
        return readPosition(fd);
    }

    private IllegalStateException newIllegalStateException(IOException e) {
        return new IllegalStateException("IOException is not expected since we read from a well known format and position", e);
    }

    private HazelcastSerializationException newUnknownFieldException(@Nonnull String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    private int readPosition(FieldDefinition fd) {
        try {
            int pos = in.readInt(offset + fd.getIndex() * Bits.INT_SIZE_IN_BYTES);
            short len = in.readShort(pos);
            // name + len + type
            return pos + Bits.SHORT_SIZE_IN_BYTES + len + 1;
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
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
    public GenericRecord[] getArrayOfGenericRecord(@Nonnull String fieldName) {
        return readNestedArray(fieldName, GenericRecord[]::new, PortableReadMethod.GENERIC_RECORD);
    }

    @Nullable
    @Override
    public InternalGenericRecord[] getArrayOfInternalGenericRecord(@Nonnull String fieldName) {
        return readNestedArray(fieldName, InternalGenericRecord[]::new, PortableReadMethod.INTERNAL_GENERIC_RECORD);
    }

    enum PortableReadMethod {
        OBJECT,
        GENERIC_RECORD,
        INTERNAL_GENERIC_RECORD
    }

    private <T> T[] readNestedArray(@Nonnull String fieldName, Function<Integer, T[]> constructor,
                                    PortableReadMethod readMethod) {
        int currentPos = in.position();
        try {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd == null) {
                throw newUnknownFieldException(fieldName);
            }
            if (fd.getType() != FieldType.PORTABLE_ARRAY) {
                throw new HazelcastSerializationException("Not a Portable array field: " + fieldName);
            }

            int position = readPosition(fd);
            if (isNullOrEmpty(position)) {
                return null;
            }
            in.position(position);
            int len = in.readInt();
            int factoryId = in.readInt();
            int classId = in.readInt();

            if (len == Bits.NULL_ARRAY_LENGTH) {
                return null;
            }

            checkFactoryAndClass(fd, factoryId, classId);

            T[] portables = constructor.apply(len);
            if (len > 0) {
                int offset = in.position();
                for (int i = 0; i < len; i++) {
                    int start = in.readInt(offset + i * Bits.INT_SIZE_IN_BYTES);
                    in.position(start);
                    switch (readMethod) {
                        case OBJECT:
                            portables[i] = serializer.read(in, factoryId, classId);
                            break;
                        case GENERIC_RECORD:
                            portables[i] = serializer.readAsGenericRecord(in, factoryId, classId);
                            break;
                        case INTERNAL_GENERIC_RECORD:
                            portables[i] = serializer.readAsInternalGenericRecord(in, factoryId, classId);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                }
            }
            return portables;
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return readNested(fieldName, PortableReadMethod.GENERIC_RECORD);
    }

    @Nullable
    @Override
    public InternalGenericRecord getInternalGenericRecord(@Nonnull String fieldName) {
        return readNested(fieldName, PortableReadMethod.INTERNAL_GENERIC_RECORD);
    }

    private <T> T readNested(@Nonnull String fieldName, PortableReadMethod readMethod) {
        int currentPos = in.position();
        try {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd == null) {
                throw newUnknownFieldException(fieldName);
            }
            if (fd.getType() != FieldType.PORTABLE) {
                throw new HazelcastSerializationException("Not a Portable field: " + fieldName);
            }

            int pos = readPosition(fd);
            in.position(pos);

            boolean isNull = in.readBoolean();
            int factoryId = in.readInt();
            int classId = in.readInt();

            checkFactoryAndClass(fd, factoryId, classId);

            if (!isNull) {
                switch (readMethod) {
                    case OBJECT:
                        return serializer.read(in, factoryId, classId);
                    case GENERIC_RECORD:
                        return serializer.readAsGenericRecord(in, factoryId, classId);
                    case INTERNAL_GENERIC_RECORD:
                        return serializer.readAsInternalGenericRecord(in, factoryId, classId);
                    default:
                        throw new IllegalStateException();
                }
            }
            return null;
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
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
    public Byte getInt8FromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.BYTE_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readByte(INT_SIZE_IN_BYTES + position + (index * BYTE_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @SuppressFBWarnings({"NP_BOOLEAN_RETURN_NULL"})
    @Override
    public Boolean getBooleanFromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.BOOLEAN_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readBoolean(INT_SIZE_IN_BYTES + position + (index * BOOLEAN_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Character getCharFromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.CHAR_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readChar(INT_SIZE_IN_BYTES + position + (index * CHAR_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Double getFloat64FromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.DOUBLE_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readDouble(INT_SIZE_IN_BYTES + position + (index * DOUBLE_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Float getFloat32FromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.FLOAT_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readFloat(INT_SIZE_IN_BYTES + position + (index * FLOAT_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Integer getInt32FromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.INT_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readInt(INT_SIZE_IN_BYTES + position + (index * INT_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Long getInt64FromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.LONG_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readLong(INT_SIZE_IN_BYTES + position + (index * LONG_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public Short getInt16FromArray(@Nonnull String fieldName, int index) {
        int position = readPosition(fieldName, FieldType.SHORT_ARRAY);
        if (isNullOrEmpty(position) || doesNotHaveIndex(position, index)) {
            return null;
        }
        try {
            return in.readShort(INT_SIZE_IN_BYTES + position + (index * SHORT_SIZE_IN_BYTES));
        } catch (IOException e) {
            throw newIllegalStateException(e);
        }
    }

    @Override
    public String getStringFromArray(@Nonnull String fieldName, int index) {
        int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.UTF_ARRAY);
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
        return readNestedFromArray(fieldName, index, PortableReadMethod.GENERIC_RECORD);
    }

    @Nullable
    @Override
    public InternalGenericRecord getInternalGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return readNestedFromArray(fieldName, index, PortableReadMethod.INTERNAL_GENERIC_RECORD);
    }

    @Override
    public <T> T getObjectFromArray(@Nonnull String fieldName, int index) {
        return readNestedFromArray(fieldName, index, PortableReadMethod.OBJECT);
    }

    private <T> T readNestedFromArray(@Nonnull String fieldName, int index, PortableReadMethod readMethod) {
        int currentPos = in.position();
        try {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd == null) {
                throw newUnknownFieldException(fieldName);
            }
            if (fd.getType() != FieldType.PORTABLE_ARRAY) {
                throw new HazelcastSerializationException("Not a Portable array field: " + fieldName);
            }

            int position = readPosition(fd);
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

            checkFactoryAndClass(fd, factoryId, classId);

            int offset = in.position();
            int start = in.readInt(offset + index * Bits.INT_SIZE_IN_BYTES);
            in.position(start);
            switch (readMethod) {
                case OBJECT:
                    return serializer.read(in, factoryId, classId);
                case GENERIC_RECORD:
                    return serializer.readAsGenericRecord(in, factoryId, classId);
                case INTERNAL_GENERIC_RECORD:
                    return serializer.readAsInternalGenericRecord(in, factoryId, classId);
                default:
                    throw new IllegalStateException();
            }
        } catch (IOException e) {
            throw newIllegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }


    private <T> T readObjectFromArrayField(@Nonnull String fieldName, FieldType fieldType,
                                           Reader<ObjectDataInput, T> reader, int index) {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, fieldType);
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
            return reader.read(in);
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
        return readObjectFromArrayField(fieldName, TIME_ARRAY, PortableUtil::readLocalTime, index);
    }

    @Override
    public LocalDate getDateFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, DATE_ARRAY, PortableUtil::readLocalDate, index);
    }

    @Override
    public LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, TIMESTAMP_ARRAY, PortableUtil::readLocalDateTime, index);
    }

    @Override
    public OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index) {
        return readObjectFromArrayField(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, PortableUtil::readOffsetDateTime, index);
    }

    @Override
    public <T> T[] getArrayOfObject(@Nonnull String fieldName, Class<T> componentType) {
        return readNestedArray(fieldName, length -> (T[]) Array.newInstance(componentType, length), PortableReadMethod.OBJECT);
    }

    @Override
    public Object getObject(@Nonnull String fieldName) {
        return readNested(fieldName, PortableReadMethod.OBJECT);
    }

    @Override
    protected Object getClassIdentifier() {
        return cd;
    }

    @Nullable
    @Override
    public Byte getNullableInt8FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Boolean getNullableBooleanFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Short getNullableInt16FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer getNullableInt32FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Long getNullableInt64FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Float getNullableFloat32FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Double getNullableFloat64FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

}
