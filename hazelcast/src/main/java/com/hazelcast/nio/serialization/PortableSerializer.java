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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

class PortableSerializer implements TypeSerializer<Portable> {

    private final SerializationContext context;

    public PortableSerializer(SerializationContext context) {
        this.context = context;
    }

    public int getTypeId() {
        return SerializationConstants.SERIALIZER_TYPE_PORTABLE;
    }

    public void write(ObjectDataOutput out, Portable p) throws IOException {
        ClassDefinitionImpl cd = getClassDefinition(p);
        PortableWriterImpl writer = new PortableWriterImpl(this, out, cd);
        p.writePortable(writer);
    }

    public ClassDefinitionImpl getClassDefinition(Portable p) throws IOException {
        final int classId = p.getClassId();
        ClassDefinitionImpl cd = context.lookup(classId);
        if (cd == null) {
            ClassDefinitionWriter classDefinitionWriter = new ClassDefinitionWriter(classId);
            p.writePortable(classDefinitionWriter);
            cd = classDefinitionWriter.cd;
            context.registerClassDefinition(classId, cd);
        }
        return cd;
    }

    public Portable read(ObjectDataInput in) throws IOException {
        ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
        final ClassDefinitionImpl cd = context.lookup(ctxIn.getClassId());
        PortableReaderImpl reader = new PortableReaderImpl(this, in, cd);
        Portable p = context.createPortable(cd.classId);
        p.readPortable(reader);
        return p;
    }

    public SerializationContext getContext() {
        return context;
    }

    public int getVersion() {
        return context.getVersion();
    }

    public void destroy() {

    }

    private class ClassDefinitionWriter implements PortableWriter {

        final ClassDefinitionImpl cd = new ClassDefinitionImpl();
        int index = 0;

        public ClassDefinitionWriter(int classId) {
            cd.classId = classId;
            cd.version = getVersion();
        }

        public void writeInt(String fieldName, int value) {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_INT));
        }

        public void writeLong(String fieldName, long value) {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_LONG));
        }

        public void writeUTF(String fieldName, String str) {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_UTF));
        }

        public void writeBoolean(String fieldName, boolean value) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_BOOLEAN));
        }

        public void writeByte(String fieldName, int value) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_BYTE));
        }

        public void writeChar(String fieldName, int value) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_CHAR));
        }

        public void writeDouble(String fieldName, double value) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_DOUBLE));
        }

        public void writeFloat(String fieldName, float value) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_FLOAT));
        }

        public void writeShort(String fieldName, int value) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_SHORT));
        }

        public void writePortable(String fieldName, Portable portable) throws IOException {
            final FieldDefinitionImpl fd = new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_PORTABLE, portable.getClassId());
            cd.add(fd);
            ClassDefinitionImpl nestedCd = getClassDefinition(portable);
            cd.add(nestedCd);
        }
    }


}

