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

import java.io.IOException;

/**
 * @mdogan 12/26/12
 */
class DefaultPortableWriter implements PortableWriter {

    final PortableSerializer serializer;
    final ClassDefinitionImpl cd;
    final BufferObjectDataOutput out;
    final int offset;
//    int fieldIndex = 0;

    DefaultPortableWriter(PortableSerializer serializer, BufferObjectDataOutput out, ClassDefinitionImpl cd) {
        this.serializer = serializer;
        this.out = out;
        this.offset = out.position();
        this.cd = cd;
        this.out.position(offset + cd.getFieldCount() * 4);
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
        IOUtil.writeNullableString(out, str);
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

    public void writeByteArray(String fieldName, byte[] values) throws IOException {
        setPosition(fieldName);
        final int len = values == null ? 0 : values.length;
        out.writeInt(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                out.writeByte(values[i]);
            }
        }
    }

    public void writeCharArray(String fieldName, char[] values) throws IOException {
        setPosition(fieldName);
        final int len = values == null ? 0 : values.length;
        out.writeInt(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                out.writeChar(values[i]);
            }
        }
    }

    public void writeIntArray(String fieldName, int[] values) throws IOException {
        setPosition(fieldName);
        final int len = values == null ? 0 : values.length;
        out.writeInt(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                out.writeInt(values[i]);
            }
        }
    }

    public void writeLongArray(String fieldName, long[] values) throws IOException {
        setPosition(fieldName);
        final int len = values == null ? 0 : values.length;
        out.writeInt(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                out.writeLong(values[i]);
            }
        }
    }

    public void writeDoubleArray(String fieldName, double[] values) throws IOException {
        setPosition(fieldName);
        final int len = values == null ? 0 : values.length;
        out.writeInt(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                out.writeDouble(values[i]);
            }
        }
    }

    public void writeFloatArray(String fieldName, float[] values) throws IOException {
        setPosition(fieldName);
        final int len = values == null ? 0 : values.length;
        out.writeInt(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                out.writeFloat(values[i]);
            }
        }
    }

    public void writeShortArray(String fieldName, short[] values) throws IOException {
        setPosition(fieldName);
        final int len = values == null ? 0 : values.length;
        out.writeInt(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                out.writeShort(values[i]);
            }
        }
    }

    public void writePortableArray(String fieldName, Portable[] portables) throws IOException {
        setPosition(fieldName);
        final int len = portables == null ? 0 : portables.length;
        out.writeInt(len);
        if (len > 0) {
            for (Portable portable : portables) {
                serializer.write(out, portable);
            }
        }
    }

//    public void writeIntMap(String fieldName, Map<Integer, Portable> portables) throws IOException {
//        setPosition(fieldName);
//        final int len = portables == null ? 0 : portables.size();
//        out.writeInt(len);
//        if (len > 0) {
//            for (Map.Entry<Integer, Portable> entry : portables.entrySet()) {
//                out.writeInt(entry.getKey());
//                serializer.write(out, entry.getValue());
//            }
//        }
//    }
//
//    public void writeStringMap(String fieldName, Map<String, Portable> portables) throws IOException {
//        setPosition(fieldName);
//        final int len = portables == null ? 0 : portables.size();
//        out.writeInt(len);
//        if (len > 0) {
//            for (Map.Entry<String, Portable> entry : portables.entrySet()) {
//                out.writeUTF(entry.getKey());
//                serializer.write(out, entry.getValue());
//            }
//        }
//    }

    private void setPosition(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName
                    + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
        }
        int pos = out.position();
        int index = fd.getIndex();
        // index = fieldIndex++; // if class versions are the same.
        out.writeInt(offset + index * 4, pos);
    }
}
