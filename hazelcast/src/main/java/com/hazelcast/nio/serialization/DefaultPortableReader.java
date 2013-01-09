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

import com.hazelcast.nio.IOUtil;

import java.io.IOException;

/**
 * @mdogan 12/28/12
 */
public class DefaultPortableReader implements PortableReader {

    final PortableSerializer serializer;
    final ClassDefinitionImpl cd;
    final IndexedObjectDataInput in;
    final int offset;

    public DefaultPortableReader(PortableSerializer serializer, IndexedObjectDataInput in, ClassDefinitionImpl cd) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;
        this.offset = in.position();
    }

    public int readInt(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readInt(pos);
    }

    public long readLong(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readLong(pos);
    }

    public String readUTF(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        return IOUtil.readNullableString(in);
    }

    public boolean readBoolean(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readBoolean(pos);
    }

    public byte readByte(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readByte(pos);
    }

    public char readChar(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readChar(pos);
    }

    public double readDouble(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readDouble(pos);
    }

    public float readFloat(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readFloat(pos);
    }

    public short readShort(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readShort(pos);
    }

    public byte[] readByteArray(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        final int len = in.readInt();
        final byte[] values = new byte[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readByte();
        }
        return values;
    }

    public char[] readCharArray(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        final int len = in.readInt();
        final char[] values = new char[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readChar();
        }
        return values;
    }

    public int[] readIntArray(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        final int len = in.readInt();
        final int [] values = new int[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readInt();
        }
        return values;
    }

    public long[] readLongArray(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        final int len = in.readInt();
        final long[] values = new long[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readLong();
        }
        return values;
    }

    public double[] readDoubleArray(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        final int len = in.readInt();
        final double [] values = new double[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readDouble();
        }
        return values;
    }

    public float[] readFloatArray(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        final int len = in.readInt();
        final float [] values = new float[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readFloat();
        }
        return values;
    }

    public short[] readShortArray(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        in.position(pos);
        final int len = in.readInt();
        final short [] values = new short[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readShort();
        }
        return values;
    }

    public Portable readPortable(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        int pos = getPosition(fd);
        in.position(pos);
        final boolean NULL = in.readBoolean();
        if (!NULL) {
            final ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
            try {
                ctxIn.setDataClassId(fd.getClassId());
                return serializer.read(in);
            } finally {
                ctxIn.setDataClassId(cd.classId);
            }
        }
        return null;
    }

    private HazelcastSerializationException throwUnknownFieldException(String fieldName) {
        return new HazelcastSerializationException("Invalid field name: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    public Portable[] readPortableArray(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        int pos = getPosition(fd);
        in.position(pos);
        final int len = in.readInt();
        final Portable[] portables = new Portable[len];
        final ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
        try {
            ctxIn.setDataClassId(fd.getClassId());
            for (int i = 0; i < len; i++) {
                portables[i] = serializer.read(in);
            }
        } finally {
            ctxIn.setDataClassId(cd.classId);
        }
        return portables;
    }

//    public Map<Integer, Portable> readIntMap(String fieldName) throws IOException {
//        FieldDefinition fd = cd.get(fieldName);
//        int pos = getPosition(fd);
//        in.position(pos);
//        final int len = in.readInt();
//        final Map<Integer, Portable> portables = new HashMap<Integer, Portable>(len);
//        final ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
//        try {
//            ctxIn.setDataClassId(fd.getDataClassId());
//            for (int i = 0; i < len; i++) {
//                int key = in.readInt();
//                Portable p = serializer.read(in);
//                portables.put(key, p);
//            }
//        } finally {
//            ctxIn.setDataClassId(cd.classId);
//        }
//        return portables;
//    }
//
//    public Map<String, Portable> readStringMap(String fieldName) throws IOException {
//        FieldDefinition fd = cd.get(fieldName);
//        int pos = getPosition(fd);
//        in.position(pos);
//        final int len = in.readInt();
//        final Map<String, Portable> portables = new HashMap<String, Portable>(len);
//        final ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
//        try {
//            ctxIn.setDataClassId(fd.getDataClassId());
//            for (int i = 0; i < len; i++) {
//                String key = in.readUTF();
//                Portable p = serializer.read(in);
//                portables.put(key, p);
//            }
//        } finally {
//            ctxIn.setDataClassId(cd.classId);
//        }
//        return portables;
//    }

    protected int getPosition(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        return getPosition(fd);
    }

    protected int getPosition(FieldDefinition fd) throws IOException {
        return in.readInt(offset + fd.getIndex() * 4);
    }
}
