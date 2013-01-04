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
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * @mdogan 12/26/12
 */
class PortableWriterImpl implements PortableWriter {

    final PortableSerializer serializer;
    final ClassDefinitionImpl cd;
    final ObjectDataOutput out;
    final int offset;
//    int fieldIndex = 0;

    PortableWriterImpl(PortableSerializer serializer, ObjectDataOutput out, ClassDefinitionImpl cd) {
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

    public void writeByte(String fieldName, int value) throws IOException {
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

    public void writeShort(String fieldName, int value) throws IOException {
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

    private void setPosition(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        int pos = out.position();
        int index = fd.getIndex();
        // index = fieldIndex++; // if class versions are the same.
        out.writeInt(offset + index * 4, pos);
    }
}
