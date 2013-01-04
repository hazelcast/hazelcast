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
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;

/**
 * @mdogan 12/28/12
 */
public class PortableReaderImpl implements PortableReader {

    final PortableSerializer serializer;
    final ClassDefinitionImpl cd;
    final ObjectDataInput in;
    final int offset;

    public PortableReaderImpl(PortableSerializer serializer, ObjectDataInput in, ClassDefinitionImpl cd) {
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

    public Portable readPortable(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        int pos = getPosition(fd);
        in.position(pos);
        final boolean NULL = in.readBoolean();
        if (!NULL) {
            final ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
            try {
                ctxIn.setClassId(fd.getClassId());
                return serializer.read(in);
            } finally {
                ctxIn.setClassId(cd.classId);
            }
        }
        return null;
    }

    private int getPosition(String fieldName) throws IOException {
        FieldDefinition fd = cd.get(fieldName);
        return getPosition(fd);
    }

    private int getPosition(FieldDefinition fd) throws IOException {
        return in.readInt(offset + fd.getIndex() * 4);
    }
}
