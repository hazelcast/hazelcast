/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.Portable;

import java.io.IOException;

import static com.hazelcast.nio.serialization.FieldType.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldType.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.BYTE;
import static com.hazelcast.nio.serialization.FieldType.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.CHAR;
import static com.hazelcast.nio.serialization.FieldType.CHAR_ARRAY;
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
    public int readInt(String fieldName) throws IOException {
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
    public long readLong(String fieldName) throws IOException {
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
    public String readUTF(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, UTF);
        return super.readUTF(fieldName);
    }

    @Override
    public boolean readBoolean(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return false;
        }
        validateTypeCompatibility(fd, BOOLEAN);
        return super.readBoolean(fieldName);
    }

    @Override
    public byte readByte(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0;
        }
        validateTypeCompatibility(fd, BYTE);
        return super.readByte(fieldName);
    }

    @Override
    public char readChar(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return 0;
        }
        validateTypeCompatibility(fd, CHAR);
        return super.readChar(fieldName);
    }

    @Override
    public double readDouble(String fieldName) throws IOException {
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
    public float readFloat(String fieldName) throws IOException {
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
    public short readShort(String fieldName) throws IOException {
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
    public byte[] readByteArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, BYTE_ARRAY);
        return super.readByteArray(fieldName);
    }

    @Override
    public boolean[] readBooleanArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, BOOLEAN_ARRAY);
        return super.readBooleanArray(fieldName);
    }

    @Override
    public char[] readCharArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, CHAR_ARRAY);
        return super.readCharArray(fieldName);
    }

    @Override
    public int[] readIntArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, INT_ARRAY);
        return super.readIntArray(fieldName);
    }

    @Override
    public long[] readLongArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, LONG_ARRAY);
        return super.readLongArray(fieldName);
    }

    @Override
    public double[] readDoubleArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, DOUBLE_ARRAY);
        return super.readDoubleArray(fieldName);
    }

    @Override
    public float[] readFloatArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, FLOAT_ARRAY);
        return super.readFloatArray(fieldName);
    }

    @Override
    public short[] readShortArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, SHORT_ARRAY);
        return super.readShortArray(fieldName);
    }

    @Override
    public String[] readUTFArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, UTF_ARRAY);
        return super.readUTFArray(fieldName);
    }

    @Override
    public Portable readPortable(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, PORTABLE);
        return super.readPortable(fieldName);
    }

    @Override
    public Portable[] readPortableArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return null;
        }
        validateTypeCompatibility(fd, PORTABLE_ARRAY);
        return super.readPortableArray(fieldName);
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
