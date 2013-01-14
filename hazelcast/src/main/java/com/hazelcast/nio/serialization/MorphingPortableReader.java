/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;

/**
 * @mdogan 1/7/13
 */
public class MorphingPortableReader extends DefaultPortableReader {


    public MorphingPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinitionImpl cd) {
        super(serializer, in, cd);
    }

    @Override
    public int readInt(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return 0;
        }
        switch (fd.getType()) {
            case FieldDefinition.TYPE_INT:
                return super.readInt(fieldName);
            case FieldDefinition.TYPE_BYTE:
                return super.readByte(fieldName);
            case FieldDefinition.TYPE_CHAR:
                return super.readChar(fieldName);
            case FieldDefinition.TYPE_SHORT:
                return super.readShort(fieldName);
            default:
                throw new IncompatibleClassChangeError();
        }
    }

    @Override
    public long readLong(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return 0L;
        }
        switch (fd.getType()) {
            case FieldDefinition.TYPE_LONG:
                return super.readLong(fieldName);
            case FieldDefinition.TYPE_INT:
                return super.readInt(fieldName);
            case FieldDefinition.TYPE_BYTE:
                return super.readByte(fieldName);
            case FieldDefinition.TYPE_CHAR:
                return super.readChar(fieldName);
            case FieldDefinition.TYPE_SHORT:
                return super.readShort(fieldName);
            default:
                throw new IncompatibleClassChangeError();
        }
    }

    @Override
    public String readUTF(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return null;
        }
        if (fd.getType() != FieldDefinition.TYPE_UTF) {
            throw new IncompatibleClassChangeError();
        }
        return super.readUTF(fieldName);
    }

    @Override
    public boolean readBoolean(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return false;
        }
        if (fd.getType() != FieldDefinition.TYPE_BOOLEAN) {
            throw new IncompatibleClassChangeError();
        }
        return super.readBoolean(fieldName);
    }

    @Override
    public byte readByte(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return 0;
        }
        if (fd.getType() != FieldDefinition.TYPE_BYTE) {
            throw new IncompatibleClassChangeError();
        }
        return super.readByte(fieldName);
    }

    @Override
    public char readChar(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return 0;
        }
        if (fd.getType() != FieldDefinition.TYPE_CHAR) {
            throw new IncompatibleClassChangeError();
        }
        return super.readChar(fieldName);
    }

    @Override
    public double readDouble(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return 0d;
        }
        switch (fd.getType()) {
            case FieldDefinition.TYPE_DOUBLE:
                return super.readDouble(fieldName);
            case FieldDefinition.TYPE_LONG:
                return super.readLong(fieldName);
            case FieldDefinition.TYPE_FLOAT:
                return super.readFloat(fieldName);
            case FieldDefinition.TYPE_INT:
                return super.readInt(fieldName);
            case FieldDefinition.TYPE_BYTE:
                return super.readByte(fieldName);
            case FieldDefinition.TYPE_CHAR:
                return super.readChar(fieldName);
            case FieldDefinition.TYPE_SHORT:
                return super.readShort(fieldName);
            default:
                throw new IncompatibleClassChangeError();
        }
    }

    @Override
    public float readFloat(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return 0f;
        }
        switch (fd.getType()) {
            case FieldDefinition.TYPE_FLOAT:
                return super.readFloat(fieldName);
            case FieldDefinition.TYPE_INT:
                return super.readInt(fieldName);
            case FieldDefinition.TYPE_BYTE:
                return super.readByte(fieldName);
            case FieldDefinition.TYPE_CHAR:
                return super.readChar(fieldName);
            case FieldDefinition.TYPE_SHORT:
                return super.readShort(fieldName);
            default:
                throw new IncompatibleClassChangeError();
        }
    }

    @Override
    public short readShort(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return 0;
        }
        switch (fd.getType()) {
            case FieldDefinition.TYPE_SHORT:
                return super.readShort(fieldName);
            case FieldDefinition.TYPE_BYTE:
                return super.readByte(fieldName);
            default:
                throw new IncompatibleClassChangeError();
        }
    }

    @Override
    public byte[] readByteArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new byte[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_BYTE_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readByteArray(fieldName);
    }

    @Override
    public char[] readCharArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new char[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_CHAR_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readCharArray(fieldName);
    }

    @Override
    public int[] readIntArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new int[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_INT_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readIntArray(fieldName);
    }

    @Override
    public long[] readLongArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new long[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_LONG_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readLongArray(fieldName);
    }

    @Override
    public double[] readDoubleArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new double[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_DOUBLE_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readDoubleArray(fieldName);
    }

    @Override
    public float[] readFloatArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new float[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_FLOAT_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readFloatArray(fieldName);
    }

    @Override
    public short[] readShortArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new short[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_SHORT_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readShortArray(fieldName);
    }

    @Override
    public Portable readPortable(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return null;
        }
        if (fd.getType() != FieldDefinition.TYPE_PORTABLE) {
            throw new IncompatibleClassChangeError();
        }
        return super.readPortable(fieldName);
    }

    @Override
    public Portable[] readPortableArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            return new Portable[0];
        }
        if (fd.getType() != FieldDefinition.TYPE_PORTABLE_ARRAY) {
            throw new IncompatibleClassChangeError();
        }
        return super.readPortableArray(fieldName);
    }
}
