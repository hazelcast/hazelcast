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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author mdogan 12/28/12
 */
public class DefaultPortableReader implements PortableReader {

    protected final ClassDefinition cd;
    private final PortableSerializer serializer;
    private final BufferObjectDataInput in;
    private final int finalPosition;
    private final int offset;
    private boolean raw = false;

    public DefaultPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;
        try {
            finalPosition = in.readInt();  // final position after portable is read
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
        int pos = getPosition(fieldName);
        return in.readInt(pos);
    }

    public long readLong(String fieldName) throws IOException {
        int pos = getPosition(fieldName);
        return in.readLong(pos);
    }

    public String readUTF(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return in.readUTF();
        } finally {
            in.position(currentPos);
        }
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
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return IOUtil.readByteArray(in);
        } finally {
            in.position(currentPos);
        }
    }

    public char[] readCharArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return in.readCharArray();
        } finally {
            in.position(currentPos);
        }
    }

    public int[] readIntArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return in.readIntArray();
        } finally {
            in.position(currentPos);
        }
    }

    public long[] readLongArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return in.readLongArray();
        } finally {
            in.position(currentPos);
        }
    }

    public double[] readDoubleArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return in.readDoubleArray();
        } finally {
            in.position(currentPos);
        }
    }

    public float[] readFloatArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return in.readFloatArray();
        } finally {
            in.position(currentPos);
        }
    }

    public short[] readShortArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            int pos = getPosition(fieldName);
            in.position(pos);
            return in.readShortArray();
        } finally {
            in.position(currentPos);
        }
    }

    public Portable readPortable(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = getPosition(fd);
            in.position(pos);
            final boolean NULL = in.readBoolean();
            if (!NULL) {
                final PortableContextAwareInputStream ctxIn = (PortableContextAwareInputStream) in;
                try {
                    ctxIn.setFactoryId(fd.getFactoryId());
                    ctxIn.setClassId(fd.getClassId());
                    return serializer.readAndInitialize(in);
                } finally {
                    ctxIn.setFactoryId(cd.getFactoryId());
                    ctxIn.setClassId(cd.getClassId());
                }
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
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = getPosition(fd);
            in.position(pos);
            final int len = in.readInt();
            final Portable[] portables = new Portable[len];
            if (len > 0) {
                final int offset = in.position();
                final PortableContextAwareInputStream ctxIn = (PortableContextAwareInputStream) in;
                try {
                    ctxIn.setFactoryId(fd.getFactoryId());
                    ctxIn.setClassId(fd.getClassId());
                    for (int i = 0; i < len; i++) {
                        final int start = in.readInt(offset + i * 4);
                        in.position(start);
                        portables[i] = serializer.readAndInitialize(in);
                    }
                } finally {
                    ctxIn.setFactoryId(cd.getFactoryId());
                    ctxIn.setClassId(cd.getClassId());
                }
            }
            return portables;
        } finally {
            in.position(currentPos);
        }
    }

    protected int getPosition(String fieldName) throws IOException {
        if (raw) {
            throw new HazelcastSerializationException("Cannot read Portable fields after getRawDataInput() is called!");
        }
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        return getPosition(fd);
    }

    protected int getPosition(FieldDefinition fd) throws IOException {
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

    @Override
    public <K, V> void readMap(final String fieldName, final Map<K, V> map) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = getPosition(fd);
            in.position(pos);
            final int len = in.readInt();
            if (len > 0) {
                final int offset = in.position();
                for (int i = 0; i < len; i++) {
                    final int startKey = in.readInt(offset + i * 8);
                    in.position(startKey);
                    final K key = in.readObject();
                    final int startValue = in.readInt(offset + 4 + i * 8);
                    in.position(startValue);
                    final V value = in.readObject();
                    map.put(key, value);
                }
            }
        } finally {
            in.position(currentPos);
        }

    }

    @Override
    public <T> void readCollection(final String fieldName, final Collection<T> collection) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = getPosition(fd);
            in.position(pos);
            final int len = in.readInt();
            if (len > 0) {
                final int offset = in.position();
                for (int i = 0; i < len; i++) {
                    final int start = in.readInt(offset + i * 4);
                    in.position(start);
                    final T value = in.readObject();
                    collection.add(value);
                }
            }
        } finally {
            in.position(currentPos);
        }

    }

    @Override
    public <T> T[] readObjectArray(final String fieldName, final Class<T[]> clazz) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = getPosition(fd);
            in.position(pos);
            final int len = in.readInt();
            if (len > 0) {
                T[] result = clazz.cast(Array.newInstance(clazz.getComponentType(), len));
                final int offset = in.position();
                for (int i = 0; i < len; i++) {
                    final int start = in.readInt(offset + i * 4);
                    in.position(start);
                    final T value = in.readObject();
                    result[i] = value;
                }
                return result;
            }
            return null;
        } finally {
            in.position(currentPos);
        }
    }


    @Override
    public <T> T readObject(final String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        final int currentPos = in.position();
        try {
            int pos = getPosition(fd);
            in.position(pos);
            return in.readObject();
        } finally {
            in.position(currentPos);
        }
    }



    void end() throws IOException {
        in.position(finalPosition);
    }
}
