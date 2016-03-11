/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;

import java.io.IOException;
import java.util.Set;

/**
 * Can't be accessed concurrently
 */
public class DefaultPortableReader implements PortableReader {

    private final BufferObjectDataInput in;
    protected final ClassDefinition cd;
    protected final PortableSerializer serializer;

    private final PortablePathNavigator navigator;
    private final int finalPosition;
    private final int offset;
    private boolean raw;

    public DefaultPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;

        this.navigator = new PortablePathNavigator();
        this.navigator.init(in, cd, serializer);

        this.finalPosition = navigator.finalPosition;
        this.offset = navigator.offset;
    }

    @Override
    public int getVersion() {
        return cd.getVersion();
    }

    @Override
    public boolean hasField(String fieldName) {
        return cd.hasField(fieldName);
    }

    @Override
    public Set<String> getFieldNames() {
        return cd.getFieldNames();
    }

    @Override
    public FieldType getFieldType(String fieldName) {
        return cd.getFieldType(fieldName);
    }

    @Override
    public int getFieldClassId(String fieldName) {
        return cd.getFieldClassId(fieldName);
    }

    @Override
    public byte readByte(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.BYTE).position;
        return in.readByte(pos);
    }

    @Override
    public short readShort(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.SHORT).position;
        return in.readShort(pos);
    }

    @Override
    public int readInt(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.INT).position;
        return in.readInt(pos);
    }

    @Override
    public long readLong(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.LONG).position;
        return in.readLong(pos);
    }

    @Override
    public float readFloat(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.FLOAT).position;
        return in.readFloat(pos);
    }

    @Override
    public double readDouble(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.DOUBLE).position;
        return in.readDouble(pos);
    }

    @Override
    public boolean readBoolean(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.BOOLEAN).position;
        return in.readBoolean(pos);
    }

    @Override
    public char readChar(String path) throws IOException {
        validateQuantifier(path);
        int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.CHAR).position;
        return in.readChar(pos);
    }

    @Override
    public String readUTF(String path) throws IOException {
        validateQuantifier(path);
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveObject(path, FieldType.UTF).position;
            in.position(pos);
            return in.readUTF();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Portable readPortable(String path) throws IOException {
        validateQuantifier(path);
        final int currentPos = in.position();
        try {
            PortablePathNavigator.Position pos = navigator.findPositionOfPortableObject(path);
            in.position(pos.position);
            if (!pos.isNull) {
                return serializer.readAndInitialize(in, pos.factoryId, pos.classId);
            }
            return null;
        } finally {
            in.position(currentPos);
        }
    }

    private void validateQuantifier(String path) {
        if (path.contains("[any]")) {
            throw new IllegalArgumentException("Invalid method for [any] quantifier. Use the readArray method family.");
        }
    }

    @Override
    public byte[] readByteArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.BYTE_ARRAY).position;
            in.position(pos);
            return in.readByteArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public boolean[] readBooleanArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.BOOLEAN_ARRAY).position;
            in.position(pos);
            return in.readBooleanArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public char[] readCharArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.CHAR_ARRAY).position;
            in.position(pos);
            return in.readCharArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public int[] readIntArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.INT_ARRAY).position;
            in.position(pos);
            return in.readIntArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public long[] readLongArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.LONG_ARRAY).position;
            in.position(pos);
            return in.readLongArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public double[] readDoubleArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.DOUBLE_ARRAY).position;
            in.position(pos);
            return in.readDoubleArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public float[] readFloatArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.FLOAT_ARRAY).position;
            in.position(pos);
            return in.readFloatArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public short[] readShortArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.SHORT_ARRAY).position;
            in.position(pos);
            return in.readShortArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public String[] readUTFArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = navigator.findPositionOfPrimitiveArray(path, FieldType.UTF_ARRAY).position;
            in.position(pos);
            return in.readUTFArray();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public Portable[] readPortableArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePathNavigator.Position pos = navigator.findPositionOfPortableArray(fieldName);
            in.position(pos.position);
            if (pos.len == Bits.NULL_ARRAY_LENGTH) {
                return null;
            }
            return readPortableArray(pos);
        } finally {
            in.position(currentPos);
        }
    }

    private Portable[] readPortableArray(PortablePathNavigator.Position pos) throws IOException {
        final Portable[] portables = new Portable[pos.len];
        for (int i = 0; i < pos.len; i++) {
            final int start = in.readInt(pos.position + i * Bits.INT_SIZE_IN_BYTES);
            in.position(start);
            portables[i] = serializer.readAndInitialize(in, pos.factoryId, pos.classId);
        }
        return portables;
    }

    @Override
    public ObjectDataInput getRawDataInput() throws IOException {
        if (!raw) {
            int pos = in.readInt(offset + cd.getFieldCount() * Bits.INT_SIZE_IN_BYTES);
            in.position(pos);
        }
        raw = true;
        return in;
    }

    final void end() throws IOException {
        in.position(finalPosition);
    }

}
