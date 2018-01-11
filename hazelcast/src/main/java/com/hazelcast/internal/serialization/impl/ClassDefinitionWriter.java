/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

final class ClassDefinitionWriter implements PortableWriter {

    private final PortableContext context;
    private final ClassDefinitionBuilder builder;

    ClassDefinitionWriter(PortableContext context, int factoryId, int classId, int version) {
        this.context = context;
        builder = new ClassDefinitionBuilder(factoryId, classId, version);
    }

    ClassDefinitionWriter(PortableContext context, ClassDefinitionBuilder builder) {
        this.context = context;
        this.builder = builder;
    }

    @Override
    public void writeInt(String fieldName, int value) {
        builder.addIntField(fieldName);
    }

    @Override
    public void writeLong(String fieldName, long value) {
        builder.addLongField(fieldName);
    }

    @Override
    public void writeUTF(String fieldName, String str) {
        builder.addUTFField(fieldName);
    }

    @Override
    public void writeBoolean(String fieldName, boolean value) throws IOException {
        builder.addBooleanField(fieldName);
    }

    @Override
    public void writeByte(String fieldName, byte value) throws IOException {
        builder.addByteField(fieldName);
    }

    @Override
    public void writeChar(String fieldName, int value) throws IOException {
        builder.addCharField(fieldName);
    }

    @Override
    public void writeDouble(String fieldName, double value) throws IOException {
        builder.addDoubleField(fieldName);
    }

    @Override
    public void writeFloat(String fieldName, float value) throws IOException {
        builder.addFloatField(fieldName);
    }

    @Override
    public void writeShort(String fieldName, short value) throws IOException {
        builder.addShortField(fieldName);
    }

    @Override
    public void writeByteArray(String fieldName, byte[] bytes) throws IOException {
        builder.addByteArrayField(fieldName);
    }

    @Override
    public void writeBooleanArray(String fieldName, boolean[] booleans)
            throws IOException {
        builder.addBooleanArrayField(fieldName);
    }

    @Override
    public void writeCharArray(String fieldName, char[] chars) throws IOException {
        builder.addCharArrayField(fieldName);
    }

    @Override
    public void writeIntArray(String fieldName, int[] ints) throws IOException {
        builder.addIntArrayField(fieldName);
    }

    @Override
    public void writeLongArray(String fieldName, long[] longs) throws IOException {
        builder.addLongArrayField(fieldName);
    }

    @Override
    public void writeDoubleArray(String fieldName, double[] values) throws IOException {
        builder.addDoubleArrayField(fieldName);
    }

    @Override
    public void writeFloatArray(String fieldName, float[] values) throws IOException {
        builder.addFloatArrayField(fieldName);
    }

    @Override
    public void writeShortArray(String fieldName, short[] values) throws IOException {
        builder.addShortArrayField(fieldName);
    }

    @Override
    public void writeUTFArray(String fieldName, String[] values)
            throws IOException {
        builder.addUTFArrayField(fieldName);
    }

    @Override
    public void writePortable(String fieldName, Portable portable) throws IOException {
        if (portable == null) {
            throw new HazelcastSerializationException("Cannot write null portable without explicitly "
                    + "registering class definition!");
        }
        int version = SerializationUtil.getPortableVersion(portable, context.getVersion());
        ClassDefinition nestedClassDef = createNestedClassDef(portable,
                new ClassDefinitionBuilder(portable.getFactoryId(), portable.getClassId(), version));
        builder.addPortableField(fieldName, nestedClassDef);
    }

    @Override
    public void writeNullPortable(String fieldName, int factoryId, int classId) throws IOException {
        final ClassDefinition nestedClassDef = context.lookupClassDefinition(factoryId, classId, context.getVersion());
        if (nestedClassDef == null) {
            throw new HazelcastSerializationException("Cannot write null portable without explicitly "
                    + "registering class definition!");
        }
        builder.addPortableField(fieldName, nestedClassDef);
    }

    @Override
    public void writePortableArray(String fieldName, Portable[] portables) throws IOException {
        if (portables == null || portables.length == 0) {
            throw new HazelcastSerializationException("Cannot write null portable array without explicitly "
                    + "registering class definition!");
        }
        Portable p = portables[0];
        int classId = p.getClassId();
        for (int i = 1; i < portables.length; i++) {
            if (portables[i].getClassId() != classId) {
                throw new IllegalArgumentException("Detected different class-ids in portable array!");
            }
        }
        int version = SerializationUtil.getPortableVersion(p, context.getVersion());
        ClassDefinition nestedClassDef = createNestedClassDef(p,
                new ClassDefinitionBuilder(p.getFactoryId(), classId, version));
        builder.addPortableArrayField(fieldName, nestedClassDef);
    }

    @Override
    public ObjectDataOutput getRawDataOutput() {
        return new EmptyObjectDataOutput();
    }

    private ClassDefinition createNestedClassDef(Portable portable, ClassDefinitionBuilder nestedBuilder)
            throws IOException {
        ClassDefinitionWriter writer = new ClassDefinitionWriter(context, nestedBuilder);
        portable.writePortable(writer);
        return context.registerClassDefinition(nestedBuilder.build());
    }

    ClassDefinition registerAndGet() {
        final ClassDefinition cd = builder.build();
        return context.registerClassDefinition(cd);
    }
}
