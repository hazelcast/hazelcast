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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.nio.serialization.FieldType.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME_ARRAY;

/**
 * Can't be accessed concurrently.
 */
public class DefaultPortableReader implements PortableReader {

    protected final ClassDefinition cd;
    protected final PortableSerializer serializer;

    private final BufferObjectDataInput in;
    private final int finalPosition;
    private final int offset;
    private boolean raw;

    DefaultPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
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

    public ClassDefinition getClassDefinition() {
        return cd;
    }

    @Override
    public int getVersion() {
        return cd.getVersion();
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return cd.hasField(fieldName);
    }

    @Override
    @Nonnull
    public Set<String> getFieldNames() {
        return cd.getFieldNames();
    }

    @Override
    @Nonnull
    public FieldType getFieldType(@Nonnull String fieldName) {
        return cd.getFieldType(fieldName);
    }

    @Override
    public int getFieldClassId(@Nonnull String fieldName) {
        return cd.getFieldClassId(fieldName);
    }

    @Override
    @Nonnull
    public ObjectDataInput getRawDataInput() throws IOException {
        if (!raw) {
            int pos = in.readInt(offset + cd.getFieldCount() * Bits.INT_SIZE_IN_BYTES);
            in.position(pos);
        }
        raw = true;
        return in;
    }

    final void end() {
        in.position(finalPosition);
    }

    @Override
    public byte readByte(@Nonnull String fieldName) throws IOException {
        return in.readByte(readPosition(fieldName, FieldType.BYTE));
    }

    @Override
    public short readShort(@Nonnull String fieldName) throws IOException {
        return in.readShort(readPosition(fieldName, FieldType.SHORT));
    }

    protected interface Reader<T, R> {
        R read(T t) throws IOException;
    }

    @Nullable
    private <T> T readNullableField(@Nonnull String fieldName, FieldType fieldType,
                                    Reader<ObjectDataInput, T> reader) throws IOException {
        int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, fieldType);
            in.position(pos);
            boolean isNull = in.readBoolean();
            if (isNull) {
                return null;
            }
            return reader.read(in);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @Nullable
    public BigDecimal readDecimal(@Nonnull String fieldName) throws IOException {
        return readNullableField(fieldName, FieldType.DECIMAL, IOUtil::readBigDecimal);
    }

    @Override
    @Nullable
    public LocalTime readTime(@Nonnull String fieldName) throws IOException {
        return readNullableField(fieldName, FieldType.TIME, PortableUtil::readLocalTime);
    }

    @Override
    @Nullable
    public LocalDate readDate(@Nonnull String fieldName) throws IOException {
        return readNullableField(fieldName, FieldType.DATE, PortableUtil::readLocalDate);
    }

    @Override
    @Nullable
    public LocalDateTime readTimestamp(@Nonnull String fieldName) throws IOException {
        return readNullableField(fieldName, FieldType.TIMESTAMP, PortableUtil::readLocalDateTime);
    }

    @Override
    @Nullable
    public OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName) throws IOException {
        return readNullableField(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE, PortableUtil::readOffsetDateTime);
    }

    @Override
    public int readInt(@Nonnull String fieldName) throws IOException {
        return in.readInt(readPosition(fieldName, FieldType.INT));
    }

    @Override
    public long readLong(@Nonnull String fieldName) throws IOException {
        return in.readLong(readPosition(fieldName, FieldType.LONG));
    }

    @Override
    public float readFloat(@Nonnull String fieldName) throws IOException {
        return in.readFloat(readPosition(fieldName, FieldType.FLOAT));
    }

    @Override
    public double readDouble(@Nonnull String fieldName) throws IOException {
        return in.readDouble(readPosition(fieldName, FieldType.DOUBLE));
    }

    @Override
    public boolean readBoolean(@Nonnull String fieldName) throws IOException {
        return in.readBoolean(readPosition(fieldName, FieldType.BOOLEAN));
    }

    @Override
    public char readChar(@Nonnull String fieldName) throws IOException {
        return in.readChar(readPosition(fieldName, FieldType.CHAR));
    }

    @Override
    @Nullable
    public String readUTF(@Nonnull String fieldName) throws IOException {
        return readString(fieldName);
    }

    @Nullable
    @Override
    public String readString(@Nonnull String fieldName) throws IOException {
        int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.UTF);
            in.position(pos);
            return in.readString();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    public Portable readPortable(@Nonnull String fieldName) throws IOException {
        int currentPos = in.position();
        try {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd == null) {
                throw throwUnknownFieldException(fieldName);
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
                return serializer.read(in, factoryId, classId);
            }
            return null;
        } finally {
            in.position(currentPos);
        }
    }

    @Nullable
    private <T> T readPrimitiveArrayField(@Nonnull String fieldName, FieldType fieldType, Reader<ObjectDataInput, T> reader)
            throws IOException {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, fieldType);
            in.position(position);
            return reader.read(in);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @Nullable
    public byte[] readByteArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.BYTE_ARRAY, ObjectDataInput::readByteArray);
    }

    @Override
    @Nullable
    public boolean[] readBooleanArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.BOOLEAN_ARRAY, ObjectDataInput::readBooleanArray);
    }

    @Override
    @Nullable
    public char[] readCharArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.CHAR_ARRAY, ObjectDataInput::readCharArray);
    }

    @Override
    @Nullable
    public int[] readIntArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.INT_ARRAY, ObjectDataInput::readIntArray);
    }

    @Override
    @Nullable
    public long[] readLongArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.LONG_ARRAY, ObjectDataInput::readLongArray);
    }

    @Override
    @Nullable
    public double[] readDoubleArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.DOUBLE_ARRAY, ObjectDataInput::readDoubleArray);
    }

    @Override
    @Nullable
    public float[] readFloatArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.FLOAT_ARRAY, ObjectDataInput::readFloatArray);
    }

    @Override
    @Nullable
    public short[] readShortArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.SHORT_ARRAY, ObjectDataInput::readShortArray);
    }

    @Override
    @Nullable
    public String[] readUTFArray(@Nonnull String fieldName) throws IOException {
        return readStringArray(fieldName);
    }

    @Nullable
    @Override
    public String[] readStringArray(@Nonnull String fieldName) throws IOException {
        return readPrimitiveArrayField(fieldName, FieldType.UTF_ARRAY, ObjectDataInput::readStringArray);
    }

    @Override
    @Nullable
    public Portable[] readPortableArray(@Nonnull String fieldName) throws IOException {
        int currentPos = in.position();
        try {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd == null) {
                throw throwUnknownFieldException(fieldName);
            }
            if (fd.getType() != FieldType.PORTABLE_ARRAY) {
                throw new HazelcastSerializationException("Not a Portable array field: " + fieldName);
            }

            int position = readPosition(fd);
            in.position(position);
            int len = in.readInt();
            int factoryId = in.readInt();
            int classId = in.readInt();

            if (len == Bits.NULL_ARRAY_LENGTH) {
                return null;
            }

            checkFactoryAndClass(fd, factoryId, classId);

            Portable[] portables = new Portable[len];
            if (len > 0) {
                int offset = in.position();
                for (int i = 0; i < len; i++) {
                    int start = in.readInt(offset + i * Bits.INT_SIZE_IN_BYTES);
                    in.position(start);
                    portables[i] = serializer.read(in, factoryId, classId);
                }
            }
            return portables;
        } finally {
            in.position(currentPos);
        }
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

    @Nullable
    private <T> T[] readObjectArrayField(@Nonnull String fieldName, FieldType fieldType, Function<Integer, T[]> constructor,
                                         Reader<ObjectDataInput, T> reader) throws IOException {
        int currentPos = in.position();
        try {
            int position = readPosition(fieldName, fieldType);
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
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @Nullable
    public BigDecimal[] readDecimalArray(@Nonnull String fieldName) throws IOException {
        return readObjectArrayField(fieldName, DECIMAL_ARRAY, BigDecimal[]::new, IOUtil::readBigDecimal);
    }

    @Override
    @Nullable
    public LocalTime[] readTimeArray(@Nonnull String fieldName) throws IOException {
        return readObjectArrayField(fieldName, TIME_ARRAY, LocalTime[]::new, PortableUtil::readLocalTime);
    }

    @Override
    @Nullable
    public LocalDate[] readDateArray(@Nonnull String fieldName) throws IOException {
        return readObjectArrayField(fieldName, DATE_ARRAY, LocalDate[]::new, PortableUtil::readLocalDate);
    }

    @Override
    @Nullable
    public LocalDateTime[] readTimestampArray(@Nonnull String fieldName) throws IOException {
        return readObjectArrayField(fieldName, TIMESTAMP_ARRAY, LocalDateTime[]::new, PortableUtil::readLocalDateTime);
    }

    @Override
    @Nullable
    public OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName) throws IOException {
        return readObjectArrayField(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, OffsetDateTime[]::new,
                PortableUtil::readOffsetDateTime);
    }

    private int readPosition(@Nonnull String fieldName, FieldType fieldType) throws IOException {
        if (raw) {
            throw new HazelcastSerializationException("Cannot read Portable fields after getRawDataInput() is called!");
        }
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        if (fd.getType() != fieldType) {
            throw new HazelcastSerializationException("Not a '" + fieldType + "' field: " + fieldName);
        }
        return readPosition(fd);
    }

    private HazelcastSerializationException throwUnknownFieldException(@Nonnull String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    private int readPosition(FieldDefinition fd) throws IOException {
        int pos = in.readInt(offset + fd.getIndex() * Bits.INT_SIZE_IN_BYTES);
        short len = in.readShort(pos);
        // name + len + type
        return pos + Bits.SHORT_SIZE_IN_BYTES + len + 1;
    }

}
