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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.Portable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.nio.serialization.FieldType.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldType.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.BYTE;
import static com.hazelcast.nio.serialization.FieldType.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.CHAR;
import static com.hazelcast.nio.serialization.FieldType.CHAR_ARRAY;
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
import static com.hazelcast.nio.serialization.FieldType.PORTABLE;
import static com.hazelcast.nio.serialization.FieldType.PORTABLE_ARRAY;
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
 * Enables reading from a portable byte stream if the portableVersion from the classDefinition is different than
 * the portableVersion from the byte stream.
 * In this case only "compatible" changes are allowed - otherwise the read operation will fail with an
 * IncompatibleClassChangeError exception.
 */
public class MorphingPortableReader extends DefaultPortableReader {

    public MorphingPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
        super(serializer, in, cd);
    }

    @Override
    public int readInt(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0;
        }
        switch (fd.getType()) {
            case INT:
                return super.readInt(fieldName);
            case BYTE:
                return super.readByte(fieldName);
            case CHAR:
                return super.readChar(fieldName);
            case SHORT:
                return super.readShort(fieldName);
            default:
                throw createIncompatibleClassChangeError(fd, INT);
        }
    }

    @Override
    public long readLong(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0L;
        }
        switch (fd.getType()) {
            case LONG:
                return super.readLong(fieldName);
            case INT:
                return super.readInt(fieldName);
            case BYTE:
                return super.readByte(fieldName);
            case CHAR:
                return super.readChar(fieldName);
            case SHORT:
                return super.readShort(fieldName);
            default:
                throw createIncompatibleClassChangeError(fd, LONG);
        }
    }

    @Override
    @Nullable
    public String readUTF(@Nonnull String fieldName) throws IOException {
        return readString(fieldName);
    }

    @Override
    @Nullable
    public String readString(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, UTF, super::readString);
    }

    @Override
    public boolean readBoolean(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return false;
        }
        validateTypeCompatibility(fd, BOOLEAN);
        return super.readBoolean(fieldName);
    }

    @Override
    public byte readByte(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0;
        }
        validateTypeCompatibility(fd, BYTE);
        return super.readByte(fieldName);
    }

    @Override
    public char readChar(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0;
        }
        validateTypeCompatibility(fd, CHAR);
        return super.readChar(fieldName);
    }

    @Override
    public double readDouble(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0d;
        }
        switch (fd.getType()) {
            case DOUBLE:
                return super.readDouble(fieldName);
            case LONG:
                return super.readLong(fieldName);
            case FLOAT:
                return super.readFloat(fieldName);
            case INT:
                return super.readInt(fieldName);
            case BYTE:
                return super.readByte(fieldName);
            case CHAR:
                return super.readChar(fieldName);
            case SHORT:
                return super.readShort(fieldName);
            default:
                throw createIncompatibleClassChangeError(fd, DOUBLE);
        }
    }

    @Override
    public float readFloat(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0f;
        }
        switch (fd.getType()) {
            case FLOAT:
                return super.readFloat(fieldName);
            case INT:
                return super.readInt(fieldName);
            case BYTE:
                return super.readByte(fieldName);
            case CHAR:
                return super.readChar(fieldName);
            case SHORT:
                return super.readShort(fieldName);
            default:
                throw createIncompatibleClassChangeError(fd, FLOAT);
        }
    }

    @Override
    public short readShort(@Nonnull String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0;
        }
        switch (fd.getType()) {
            case SHORT:
                return super.readShort(fieldName);
            case BYTE:
                return super.readByte(fieldName);
            default:
                throw createIncompatibleClassChangeError(fd, SHORT);
        }
    }

    @Override
    @Nullable
    public BigDecimal readDecimal(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, DECIMAL, super::readDecimal);
    }

    @Override
    @Nullable
    public LocalTime readTime(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, TIME, super::readTime);
    }

    @Override
    @Nullable
    public LocalDate readDate(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, DATE, super::readDate);
    }

    @Override
    @Nullable
    public LocalDateTime readTimestamp(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, TIMESTAMP, super::readTimestamp);
    }

    @Override
    @Nullable
    public OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, TIMESTAMP_WITH_TIMEZONE, super::readTimestampWithTimezone);
    }

    @Override
    @Nullable
    public byte[] readByteArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, BYTE_ARRAY, super::readByteArray);
    }

    @Override
    @Nullable
    public boolean[] readBooleanArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, BOOLEAN_ARRAY, super::readBooleanArray);
    }

    @Override
    @Nullable
    public char[] readCharArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, CHAR_ARRAY, super::readCharArray);
    }

    @Override
    @Nullable
    public int[] readIntArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, INT_ARRAY, super::readIntArray);
    }

    @Override
    @Nullable
    public long[] readLongArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, LONG_ARRAY, super::readLongArray);
    }

    @Override
    @Nullable
    public double[] readDoubleArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, DOUBLE_ARRAY, super::readDoubleArray);
    }

    @Override
    @Nullable
    public float[] readFloatArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, FLOAT_ARRAY, super::readFloatArray);
    }

    @Override
    @Nullable
    public short[] readShortArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, SHORT_ARRAY, super::readShortArray);
    }

    @Override
    @Nullable
    public String[] readUTFArray(@Nonnull String fieldName) throws IOException {
        return readStringArray(fieldName);
    }

    @Override
    @Nullable
    public String[] readStringArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, UTF_ARRAY, super::readStringArray);
    }

    @Override
    @Nullable
    public Portable readPortable(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, PORTABLE, super::readPortable);
    }

    @Override
    @Nullable
    public Portable[] readPortableArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, PORTABLE_ARRAY, super::readPortableArray);
    }

    @Override
    @Nullable
    public BigDecimal[] readDecimalArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, DECIMAL_ARRAY, super::readDecimalArray);
    }

    @Override
    @Nullable
    public LocalTime[] readTimeArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, TIME_ARRAY, super::readTimeArray);
    }

    @Override
    @Nullable
    public LocalDate[] readDateArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, DATE_ARRAY, super::readDateArray);
    }

    @Override
    @Nullable
    public LocalDateTime[] readTimestampArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, TIMESTAMP_ARRAY, super::readTimestampArray);
    }

    @Override
    @Nullable
    public OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName) throws IOException {
        return readIncompatibleField(fieldName, TIMESTAMP_WITH_TIMEZONE_ARRAY, super::readTimestampWithTimezoneArray);
    }

    private <T> T readIncompatibleField(@Nonnull String fieldName, FieldType fieldType,
                                        Reader<String, T> reader) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, fieldType);
        return reader.read(fieldName);
    }

    private void validateTypeCompatibility(FieldDefinition fd, FieldType expectedType) {
        if (fd.getType() != expectedType) {
            throw createIncompatibleClassChangeError(fd, expectedType);
        }
    }

    private IncompatibleClassChangeError createIncompatibleClassChangeError(FieldDefinition fd, FieldType expectedType) {
        return new IncompatibleClassChangeError("Incompatible to read " + expectedType + " from " + fd.getType()
                + " while reading field: " + fd.getName() + " on " + cd);
    }
}
