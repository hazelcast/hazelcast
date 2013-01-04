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
            context.registerClassDefinition(cd);
        }
        return cd;
    }

    public Portable read(ObjectDataInput in) throws IOException {
        ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
        ClassDefinitionImpl cd = context.lookup(ctxIn.getLocalClassId(), ctxIn.getLocalVersionId());
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

        public void writeByte(String fieldName, byte value) throws IOException {
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

        public void writeShort(String fieldName, short value) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_SHORT));
        }

        public void writePortable(String fieldName, Portable portable) throws IOException {
            final FieldDefinitionImpl fd = new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_PORTABLE, portable.getClassId());
            addNestedField(portable, fd);
        }

        public void writeByteArray(String fieldName, byte[] bytes) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_BYTE_ARRAY));
        }

        public void writeCharArray(String fieldName, char[] chars) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_CHAR_ARRAY));
        }

        public void writeIntArray(String fieldName, int[] ints) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_INT_ARRAY));
        }

        public void writeLongArray(String fieldName, long[] longs) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_LONG_ARRAY));
        }

        public void writeDoubleArray(String fieldName, double[] values) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_DOUBLE_ARRAY));
        }

        public void writeFloatArray(String fieldName, float[] values) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_FLOAT_ARRAY));
        }

        public void writeShortArray(String fieldName, short[] values) throws IOException {
            cd.add(new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_SHORT_ARRAY));
        }

        public void writePortableArray(String fieldName, Portable[] portables) throws IOException {
            if (portables == null || portables.length == 0) {
                throw new IllegalArgumentException();
            }
            final Portable p = portables[0];
            final int classId = p.getClassId();
            for (int i = 1; i < portables.length; i++) {
                if (portables[i].getClassId() != classId) {
                    throw new IllegalArgumentException();
                }
            }
            final FieldDefinitionImpl fd = new FieldDefinitionImpl(index++, fieldName,
                    FieldDefinitionImpl.TYPE_PORTABLE_ARRAY, classId);
            addNestedField(p, fd);
        }

        private void addNestedField(Portable p, FieldDefinitionImpl fd) throws IOException {
            cd.add(fd);
            ClassDefinitionImpl nestedCd = getClassDefinition(p);
            cd.add(nestedCd);
        }
    }


}

