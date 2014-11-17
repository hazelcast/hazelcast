/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

public class DefaultPortableReader implements PortableReader {

    private static final Pattern NESTED_FIELD_PATTERN = Pattern.compile("\\.");

    protected final ClassDefinition cd;
    private final PortableSerializer serializer;
    private final BufferObjectDataInput in;
    private final int finalPosition;
    private final int offset;
    private boolean raw;

    public DefaultPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;
        try {
            // final position after portable is read
            finalPosition = in.readInt();
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        }
        this.offset = in.position();
    }

    public int getVersion() {
        return cd.getVersion();
    }

    public boolean hasField(String fieldName) {
        return cd.hasField(fieldName);
    }

    public Set<String> getFieldNames() {
        return cd.getFieldNames();
    }

    public FieldType getFieldType(String fieldName) {
        return cd.getFieldType(fieldName);
    }

    public int getFieldClassId(String fieldName) {
        return cd.getFieldClassId(fieldName);
    }

    public int readInt(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.INT);
        return in.readInt(pos);
    }

    public long readLong(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.LONG);
        return in.readLong(pos);
    }

    public String readUTF(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.UTF);
            in.position(pos);
            return in.readUTF();
        } finally {
            in.position(currentPos);
        }
    }

    public boolean readBoolean(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.BOOLEAN);
        return in.readBoolean(pos);
    }

    public byte readByte(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.BYTE);
        return in.readByte(pos);
    }

    public char readChar(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.CHAR);
        return in.readChar(pos);
    }

    public double readDouble(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.DOUBLE);
        return in.readDouble(pos);
    }

    public float readFloat(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.FLOAT);
        return in.readFloat(pos);
    }

    public short readShort(String fieldName) throws IOException {
        int pos = readPosition(fieldName, FieldType.SHORT);
        return in.readShort(pos);
    }

    public byte[] readByteArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.BYTE_ARRAY);
            in.position(pos);
            return in.readByteArray();
        } finally {
            in.position(currentPos);
        }
    }

    public char[] readCharArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.CHAR_ARRAY);
            in.position(pos);
            return in.readCharArray();
        } finally {
            in.position(currentPos);
        }
    }

    public int[] readIntArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.INT_ARRAY);
            in.position(pos);
            return in.readIntArray();
        } finally {
            in.position(currentPos);
        }
    }

    public long[] readLongArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.LONG_ARRAY);
            in.position(pos);
            return in.readLongArray();
        } finally {
            in.position(currentPos);
        }
    }

    public double[] readDoubleArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.DOUBLE_ARRAY);
            in.position(pos);
            return in.readDoubleArray();
        } finally {
            in.position(currentPos);
        }
    }

    public float[] readFloatArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.FLOAT_ARRAY);
            in.position(pos);
            return in.readFloatArray();
        } finally {
            in.position(currentPos);
        }
    }

    public short[] readShortArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = readPosition(fieldName, FieldType.SHORT_ARRAY);
            in.position(pos);
            return in.readShortArray();
        } finally {
            in.position(currentPos);
        }
    }

    public Portable readPortable(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        if (fd.getType() != FieldType.PORTABLE) {
            throw new HazelcastSerializationException("Not a Portable field: " + fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = readPosition(fd);
            in.position(pos);
            final boolean isNull = in.readBoolean();
            if (!isNull) {
                return serializer.readAndInitialize(in);
            }
            return null;
        } finally {
            in.position(currentPos);
        }
    }

    private HazelcastSerializationException throwUnknownFieldException(String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    public Portable[] readPortableArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        if (fd.getType() != FieldType.PORTABLE_ARRAY) {
            throw new HazelcastSerializationException("Not a Portable array field: " + fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = readPosition(fd);
            in.position(pos);
            final int len = in.readInt();
            final Portable[] portables = new Portable[len];
            if (len > 0) {
                final int offset = in.position();
                for (int i = 0; i < len; i++) {
                    final int start = in.readInt(offset + i * 4);
                    in.position(start);
                    portables[i] = serializer.readAndInitialize(in);
                }
            }
            return portables;
        } finally {
            in.position(currentPos);
        }
    }

    private int readPosition(String fieldName, FieldType type) throws IOException {
        if (raw) {
            throw new HazelcastSerializationException("Cannot read Portable fields after getRawDataInput() is called!");
        }
        FieldDefinition fd = cd.getField(fieldName);
        if (fd == null) {
            return readNestedPosition(fieldName, type);
        }
        if (fd.getType() != type) {
            throw new HazelcastSerializationException("Not a '" + type + "' field: " + fieldName);
        }
        return readPosition(fd);
    }

    private int readNestedPosition(String fieldName, FieldType type) throws IOException {
        String[] fieldNames = NESTED_FIELD_PATTERN.split(fieldName);
        if (fieldNames.length > 1) {
            FieldDefinition fd = null;
            DefaultPortableReader reader = this;

            for (int i = 0; i < fieldNames.length; i++) {
                fd = reader.cd.getField(fieldNames[i]);
                if (fd == null) {
                    break;
                }
                if (i == fieldNames.length - 1) {
                    break;
                }

                int pos = reader.readPosition(fd);
                in.position(pos);
                boolean isNull = in.readBoolean();
                if (isNull) {
                    throw new NullPointerException("Parent field is null: " + fieldNames[i]);
                }
                reader = serializer.createReader(in);
            }
            if (fd == null) {
                throw throwUnknownFieldException(fieldName);
            }
            if (fd.getType() != type) {
                throw new HazelcastSerializationException("Not a '" + type + "' field: " + fieldName);
            }
            return reader.readPosition(fd);
        }
        throw throwUnknownFieldException(fieldName);
    }

    private int readPosition(FieldDefinition fd) throws IOException {
        return in.readInt(offset + fd.getIndex() * 4);
    }

    public ObjectDataInput getRawDataInput() throws IOException {
        if (!raw) {
            int pos = in.readInt(offset + cd.getFieldCount() * 4);
            in.position(pos);
        }
        raw = true;
        return in;
    }

    void end() throws IOException {
        in.position(finalPosition);
    }

}
