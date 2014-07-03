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

import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author mdogan 12/26/12
 */
public class DefaultPortableWriter implements PortableWriter {

    private final PortableSerializer serializer;
    private final ClassDefinition cd;
    private final BufferObjectDataOutput out;
    private final int begin;
    private final int offset;
    private final Set<String> writtenFields;
    private boolean raw = false;

    public DefaultPortableWriter(PortableSerializer serializer, BufferObjectDataOutput out, ClassDefinition cd)throws IOException {
        this.serializer = serializer;
        this.out = out;
        this.cd = cd;
        this.writtenFields = new HashSet<String>(cd.getFieldCount());
        this.begin = out.position();

        out.writeZeroBytes(4);  // room for final offset

        this.offset = out.position();
        final int fieldIndexesLength = (cd.getFieldCount() + 1) * 4; // one additional for raw data
        out.writeZeroBytes(fieldIndexesLength);
    }

    public int getVersion() {
        return cd.getVersion();
    }

    public void writeInt(String fieldName, int value) throws IOException {
        setPosition(fieldName);
        out.writeInt(value);
    }

    public void writeLong(String fieldName, long value) throws IOException {
        setPosition(fieldName);
        out.writeLong(value);
    }

    public void writeUTF(String fieldName, String str) throws IOException {
        setPosition(fieldName);
        out.writeUTF(str);
    }

    public void writeBoolean(String fieldName, boolean value) throws IOException {
        setPosition(fieldName);
        out.writeBoolean(value);
    }

    public void writeByte(String fieldName, byte value) throws IOException {
        setPosition(fieldName);
        out.writeByte(value);
    }

    public void writeChar(String fieldName, int value) throws IOException {
        setPosition(fieldName);
        out.writeChar(value);
    }

    public void writeDouble(String fieldName, double value) throws IOException {
        setPosition(fieldName);
        out.writeDouble(value);
    }

    public void writeFloat(String fieldName, float value) throws IOException {
        setPosition(fieldName);
        out.writeFloat(value);
    }

    public void writeShort(String fieldName, short value) throws IOException {
        setPosition(fieldName);
        out.writeShort(value);
    }

    public void writePortable(String fieldName, Portable portable) throws IOException {
        setPosition(fieldName);
        final boolean NULL = portable == null;
        out.writeBoolean(NULL);
        if (!NULL) {
            serializer.write(out, portable);
        }
    }

    public void writeNullPortable(String fieldName, int factoryId, int classId) throws IOException {
        setPosition(fieldName);
        final boolean NULL = true;
        out.writeBoolean(NULL);
    }

    public void writeByteArray(String fieldName, byte[] values) throws IOException {
        setPosition(fieldName);
        IOUtil.writeByteArray(out, values);
    }

    public void writeCharArray(String fieldName, char[] values) throws IOException {
        setPosition(fieldName);
        out.writeCharArray(values);
    }

    public void writeIntArray(String fieldName, int[] values) throws IOException {
        setPosition(fieldName);
        out.writeIntArray(values);
    }

    public void writeLongArray(String fieldName, long[] values) throws IOException {
        setPosition(fieldName);
        out.writeLongArray(values);
    }

    public void writeDoubleArray(String fieldName, double[] values) throws IOException {
        setPosition(fieldName);
        out.writeDoubleArray(values);
    }

    public void writeFloatArray(String fieldName, float[] values) throws IOException {
        setPosition(fieldName);
        out.writeFloatArray(values);
    }

    public void writeShortArray(String fieldName, short[] values) throws IOException {
        setPosition(fieldName);
        out.writeShortArray(values);
    }

    public void writePortableArray(String fieldName, Portable[] portables) throws IOException {
        setPosition(fieldName);
        final int len = portables == null ? 0 : portables.length;
        out.writeInt(len);
        if (len > 0) {
            final int offset = out.position();
            out.writeZeroBytes(len * 4);
            for (int i = 0; i < portables.length; i++) {
                out.writeInt(offset + i * 4, out.position());
                final Portable portable = portables[i];
                serializer.write(out, portable);
            }
        }
    }

    private void setPosition(String fieldName) throws IOException {
        if (raw) {
            throw new HazelcastSerializationException("Cannot write Portable fields after getRawDataOutput() is called!");
        }
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName
                    + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
        }
        if (writtenFields.add(fieldName)) {
            int pos = out.position();
            int index = fd.getIndex();
            out.writeInt(offset + index * 4, pos);
        } else {
            throw new HazelcastSerializationException("Field '" + fieldName + "' has already been written!");
        }
    }

    public ObjectDataOutput getRawDataOutput() throws IOException {
        if (!raw) {
            int pos = out.position();
            int index = cd.getFieldCount(); // last index
            out.writeInt(offset + index * 4, pos);
        }
        raw = true;
        return out;
    }

    @Override
    public <K, V> void writeMap(final String fieldName, final Map<K,V> map) throws IOException {
            setPosition(fieldName);
            final int len = map == null ? 0 : map.size();
            out.writeInt(len);
            if (len > 0) {
                final int offset = out.position();
                out.writeZeroBytes(len * 4 * 2); //Keys and values
                int i = 0;
                for (final Map.Entry<K,V> entry: map.entrySet()) {
                    out.writeInt(offset + i * 8, out.position());
                    out.writeObject(entry.getKey());
                    out.writeInt(offset + 4 + i * 8, out.position());
                    out.writeObject(entry.getValue());
                    i++;
                }
            }
        }

    @Override
    public <T> void writeCollection(final String fieldName, final Collection<T> collection) throws IOException {
        setPosition(fieldName);
        final int len = collection == null ? 0 : collection.size();
        out.writeInt(len);
        if (len > 0) {
            final int offset = out.position();
            out.writeZeroBytes(len * 4);
            int i = 0;
            for (final T val: collection) {
                out.writeInt(offset + i * 4, out.position());
                out.writeObject(val);
                i++;
            }
        }
    }

    @Override
    public <T> void writeObjectArray(final String fieldName, final T[] objectArray) throws IOException {
        setPosition(fieldName);
        final int len = objectArray == null ? 0 : objectArray.length;
        out.writeInt(len);
        if (len > 0) {
            final int offset = out.position();
            out.writeZeroBytes(len * 4);
            int i = 0;
            for (final T val: objectArray) {
                out.writeInt(offset + i * 4, out.position());
                out.writeObject(val);
                i++;
            }
        }
    }

    @Override
    public <T> void writeObject(final String fieldName, final T object) throws IOException {
            setPosition(fieldName);
            out.writeObject(object);
    }

    void end() throws IOException {
        out.writeInt(begin, out.position()); // write final offset
    }
}
