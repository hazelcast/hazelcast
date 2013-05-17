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
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PortableSerializer implements TypeSerializer<Portable> {

    private final SerializationContext context;
    private final Map<Integer, PortableFactory> factories = new HashMap<Integer, PortableFactory>();

    public PortableSerializer(SerializationContext context, Map<Integer, ? extends PortableFactory> portableFactories) {
        this.context = context;
        factories.putAll(portableFactories);
    }

    public int getTypeId() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE;
    }

    public void write(ObjectDataOutput out, Portable p) throws IOException {
        if (p.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }
        write(out, p, p.getClassId());
    }

    void write(ObjectDataOutput out, Portable p, int classId) throws IOException {
        if (!(out instanceof BufferObjectDataOutput)) {
            throw new IllegalArgumentException("ObjectDataOutput must be instance of BufferObjectDataOutput!");
        }
        if (classId == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }
        ClassDefinition cd = getClassDefinition(p, classId);
        PortableWriter writer = new DefaultPortableWriter(this, (BufferObjectDataOutput) out, cd);
        p.writePortable(writer);
    }

    private ClassDefinition getClassDefinition(Portable p, int classId) throws IOException {
        ClassDefinition cd = context.lookup(p.getFactoryId(), classId);
        if (cd == null) {
            ClassDefinitionWriter classDefinitionWriter = new ClassDefinitionWriter(p.getFactoryId(), classId);
            p.writePortable(classDefinitionWriter);
            cd = classDefinitionWriter.registerAndGet();
        }
        return cd;
    }

    public Portable read(ObjectDataInput in) throws IOException {
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of BufferObjectDataInput!");
        }
        final ContextAwareDataInput ctxIn = (ContextAwareDataInput) in;
        final int factoryId = ctxIn.getFactoryId();
        final int dataClassId = ctxIn.getDataClassId();
        final int dataVersion = ctxIn.getDataVersion();
        final PortableFactory portableFactory = factories.get(factoryId);
        if (portableFactory == null) {
            throw new HazelcastSerializationException("Could not find PortableFactory for factoryId: " + factoryId);
        }
        final Portable portable = portableFactory.create(dataClassId);
        if (portable == null) {
            throw new HazelcastSerializationException("Could not create Portable for class-id: " + dataClassId);
        }
        final PortableReader reader;
        final ClassDefinition cd;
        if (context.getVersion() == dataVersion) {
            cd = context.lookup(factoryId, dataClassId); // using context.version
            reader = new DefaultPortableReader(this, (BufferObjectDataInput) in, cd);
        } else {
            cd = context.lookup(factoryId, dataClassId, dataVersion); // registered during read
            reader = new MorphingPortableReader(this, (BufferObjectDataInput) in, cd);
        }
        portable.readPortable(reader);
        return portable;
    }

    public void destroy() {
        factories.clear();
    }

    private class ClassDefinitionWriter implements PortableWriter {

        final ClassDefinitionBuilder builder;

        ClassDefinitionWriter(int factoryId, int classId) {
            builder = new ClassDefinitionBuilder(factoryId, classId);
        }

        private ClassDefinitionWriter(ClassDefinitionBuilder builder) {
            this.builder = builder;
        }

        public int getVersion() {
            return context.getVersion();
        }

        public void writeInt(String fieldName, int value) {
            builder.addIntField(fieldName);
        }

        public void writeLong(String fieldName, long value) {
            builder.addLongField(fieldName);
        }

        public void writeUTF(String fieldName, String str) {
            builder.addUTFField(fieldName);
        }

        public void writeBoolean(String fieldName, boolean value) throws IOException {
            builder.addBooleanField(fieldName);
        }

        public void writeByte(String fieldName, byte value) throws IOException {
            builder.addByteField(fieldName);
        }

        public void writeChar(String fieldName, int value) throws IOException {
            builder.addCharField(fieldName);
        }

        public void writeDouble(String fieldName, double value) throws IOException {
            builder.addDoubleField(fieldName);
        }

        public void writeFloat(String fieldName, float value) throws IOException {
            builder.addFloatField(fieldName);
        }

        public void writeShort(String fieldName, short value) throws IOException {
            builder.addShortField(fieldName);
        }

        public void writeByteArray(String fieldName, byte[] bytes) throws IOException {
            builder.addByteArrayField(fieldName);
        }

        public void writeCharArray(String fieldName, char[] chars) throws IOException {
            builder.addCharArrayField(fieldName);
        }

        public void writeIntArray(String fieldName, int[] ints) throws IOException {
            builder.addIntArrayField(fieldName);
        }

        public void writeLongArray(String fieldName, long[] longs) throws IOException {
            builder.addLongArrayField(fieldName);
        }

        public void writeDoubleArray(String fieldName, double[] values) throws IOException {
            builder.addDoubleArrayField(fieldName);
        }

        public void writeFloatArray(String fieldName, float[] values) throws IOException {
            builder.addFloatArrayField(fieldName);
        }

        public void writeShortArray(String fieldName, short[] values) throws IOException {
            builder.addShortArrayField(fieldName);
        }

        public void writePortable(String fieldName, Portable portable) throws IOException {
            if (portable == null) {
                throw new HazelcastSerializationException("Cannot write null portable without explicitly " +
                        "registering class definition!");
            }
            writePortable(fieldName, portable.getFactoryId(), portable.getClassId(), portable);
        }

        private void writePortable(String fieldName, int factoryId, int classId, Portable portable) throws IOException {
            final ClassDefinition nestedClassDef;
            if (portable != null) {
                nestedClassDef = createNestedClassDef(portable, new ClassDefinitionBuilder(factoryId, classId));
            } else {
                nestedClassDef = context.lookup(factoryId, classId);
                if (nestedClassDef == null) {
                    throw new HazelcastSerializationException("Cannot write null portable without explicitly " +
                            "registering class definition!");
                }
            }
            builder.addPortableField(fieldName, nestedClassDef);
        }

        public void writeNullPortable(String fieldName, int factoryId, int classId) throws IOException {
            writePortable(fieldName, factoryId, classId, null);
        }

        public void writePortableArray(String fieldName, Portable[] portables) throws IOException {
            if (portables == null || portables.length == 0) {
                throw new HazelcastSerializationException("Cannot write null portable array without explicitly " +
                        "registering class definition!");
            }
            final Portable p = portables[0];
            final int classId = p.getClassId();
            for (int i = 1; i < portables.length; i++) {
                if (portables[i].getClassId() != classId) {
                    throw new IllegalArgumentException("Detected different class-ids in portable array!");
                }
            }
            final ClassDefinition nestedClassDef = createNestedClassDef(p, new ClassDefinitionBuilder(p.getFactoryId(), classId));
            builder.addPortableArrayField(fieldName, nestedClassDef);
        }

        public ObjectDataOutput getRawDataOutput() {
            return new EmptyObjectDataOutput();
        }

        private ClassDefinition createNestedClassDef(Portable portable, ClassDefinitionBuilder nestedBuilder) throws IOException {
            ClassDefinitionWriter nestedWriter = new ClassDefinitionWriter(nestedBuilder);
            portable.writePortable(nestedWriter);
            return context.registerClassDefinition(nestedBuilder.build());
        }

        ClassDefinition registerAndGet() {
            final ClassDefinition cd = builder.build();
            return context.registerClassDefinition(cd);
        }
    }
}

