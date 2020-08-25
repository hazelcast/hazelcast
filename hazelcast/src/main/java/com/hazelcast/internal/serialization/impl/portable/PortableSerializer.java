/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class PortableSerializer implements StreamSerializer<Object> {

    private final PortableContextImpl context;
    private final Map<Integer, PortableFactory> factories = new HashMap<>();

    public PortableSerializer(PortableContextImpl context, Map<Integer, ? extends PortableFactory> portableFactories) {
        this.context = context;
        factories.putAll(portableFactories);
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE;
    }

    @Override
    public void write(ObjectDataOutput out, Object o) throws IOException {
        if (o instanceof Portable) {
            Portable p = (Portable) o;
            if (!(out instanceof BufferObjectDataOutput)) {
                throw new IllegalArgumentException("ObjectDataOutput must be instance of BufferObjectDataOutput!");
            }
            if (p.getClassId() == 0) {
                throw new IllegalArgumentException("Portable class ID cannot be zero!");
            }

            out.writeInt(p.getFactoryId());
            out.writeInt(p.getClassId());
            writeInternal((BufferObjectDataOutput) out, p);
            return;
        }
        if (o instanceof PortableGenericRecord) {
            writePortableGenericRecord(out, (PortableGenericRecord) o);
            return;
        }
        throw new IllegalArgumentException("PortableSerializer can only write Portable and PortableGenericRecord");
    }


    void writeInternal(BufferObjectDataOutput out, Portable p) throws IOException {
        ClassDefinition cd = context.lookupOrRegisterClassDefinition(p);
        out.writeInt(cd.getVersion());

        DefaultPortableWriter writer = new DefaultPortableWriter(this, out, cd);
        p.writePortable(writer);
        writer.end();
    }

    @Override
    public Object read(ObjectDataInput in) throws IOException {
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of BufferObjectDataInput!");
        }

        int factoryId = in.readInt();
        int classId = in.readInt();

        BufferObjectDataInput input = (BufferObjectDataInput) in;
        Portable portable = createNewPortableInstance(factoryId, classId);
        if (portable != null) {
            return readPortable(input, factoryId, classId, portable);
        }
        GenericRecord genericRecord = readPortableGenericRecord(input, factoryId, classId);
        assert genericRecord instanceof PortableGenericRecord;
        return genericRecord;
    }


    private Portable readPortable(BufferObjectDataInput in, int factoryId, int classId, Portable portable) throws IOException {
        int writeVersion = in.readInt();
        int readVersion = findPortableVersion(factoryId, classId, portable);
        DefaultPortableReader reader = createReader(in, factoryId, classId, writeVersion, readVersion);
        portable.readPortable(reader);
        reader.end();
        return portable;
    }

    private int findPortableVersion(int factoryId, int classId, Portable portable) {
        int currentVersion = context.getClassVersion(factoryId, classId);
        if (currentVersion < 0) {
            currentVersion = SerializationUtil.getPortableVersion(portable, context.getVersion());
            if (currentVersion > 0) {
                context.setClassVersion(factoryId, classId, currentVersion);
            }
        }
        return currentVersion;
    }

    private Portable createNewPortableInstance(int factoryId, int classId) {
        final PortableFactory portableFactory = factories.get(factoryId);
        if (portableFactory == null) {
            return null;
        }
        return portableFactory.create(classId);
    }

    public InternalGenericRecord readAsInternalGenericRecord(ObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();

        BufferObjectDataInput input = (BufferObjectDataInput) in;
        ClassDefinition cd = setupPositionAndDefinition(input, factoryId, classId, version);
        return new PortableInternalGenericRecord(this, input, cd, true);
    }

    DefaultPortableReader createMorphingReader(BufferObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();

        Portable portable = createNewPortableInstance(factoryId, classId);
        int portableVersion = findPortableVersion(factoryId, classId, portable);

        return createReader(in, factoryId, classId, version, portableVersion);
    }

    public ClassDefinition setupPositionAndDefinition(BufferObjectDataInput in, int factoryId, int classId, int version)
            throws IOException {

        int effectiveVersion = version;
        if (effectiveVersion < 0) {
            effectiveVersion = context.getVersion();
        }

        ClassDefinition cd = context.lookupClassDefinition(factoryId, classId, effectiveVersion);
        if (cd == null) {
            int begin = in.position();
            cd = context.readClassDefinition(in, factoryId, classId, effectiveVersion);
            in.position(begin);
        }

        return cd;
    }

    public DefaultPortableReader createReader(BufferObjectDataInput in, int factoryId, int classId, int version,
                                              int portableVersion) throws IOException {

        ClassDefinition cd = setupPositionAndDefinition(in, factoryId, classId, version);
        DefaultPortableReader reader;
        if (portableVersion == cd.getVersion()) {
            reader = new DefaultPortableReader(this, in, cd);
        } else {
            reader = new MorphingPortableReader(this, in, cd);
        }
        return reader;
    }

    @Override
    public void destroy() {
        factories.clear();
    }

    //Portable Generic Record

    private void writePortableGenericRecord(ObjectDataOutput out, PortableGenericRecord record) throws IOException {
        ClassDefinition cd = record.getClassDefinition();
        if (context.shouldCheckClassDefinitionErrors()) {
            ClassDefinition existingCd = context.lookupClassDefinition(cd.getFactoryId(), cd.getClassId(), cd.getVersion());
            if (existingCd != null && !existingCd.equals(cd)) {
                throw new HazelcastSerializationException("Inconsistent class definition found. New class definition : " + cd
                        + ", Existing class definition " + existingCd);
            }
        }
        context.registerClassDefinition(cd);
        out.writeInt(cd.getFactoryId());
        out.writeInt(cd.getClassId());
        writePortableGenericRecordInternal(out, record);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    void writePortableGenericRecordInternal(ObjectDataOutput out, PortableGenericRecord record) throws IOException {
        ClassDefinition cd = record.getClassDefinition();
        out.writeInt(cd.getVersion());

        BufferObjectDataOutput output = (BufferObjectDataOutput) out;
        DefaultPortableWriter writer = new DefaultPortableWriter(this, output, cd);
        Set<String> fieldNames = cd.getFieldNames();
        for (String fieldName : fieldNames) {
            switch (cd.getFieldType(fieldName)) {
                case PORTABLE:
                    writer.writeGenericRecord(fieldName, record.readGenericRecord(fieldName));
                    break;
                case BYTE:
                    writer.writeByte(fieldName, record.readByte(fieldName));
                    break;
                case BOOLEAN:
                    writer.writeBoolean(fieldName, record.readBoolean(fieldName));
                    break;
                case CHAR:
                    writer.writeChar(fieldName, record.readChar(fieldName));
                    break;
                case SHORT:
                    writer.writeShort(fieldName, record.readShort(fieldName));
                    break;
                case INT:
                    writer.writeInt(fieldName, record.readInt(fieldName));
                    break;
                case LONG:
                    writer.writeLong(fieldName, record.readLong(fieldName));
                    break;
                case FLOAT:
                    writer.writeFloat(fieldName, record.readFloat(fieldName));
                    break;
                case DOUBLE:
                    writer.writeDouble(fieldName, record.readDouble(fieldName));
                    break;
                case UTF:
                    writer.writeUTF(fieldName, record.readUTF(fieldName));
                    break;
                case PORTABLE_ARRAY:
                    writer.writeGenericRecordArray(fieldName, record.readGenericRecordArray(fieldName));
                    break;
                case BYTE_ARRAY:
                    writer.writeByteArray(fieldName, record.readByteArray(fieldName));
                    break;
                case BOOLEAN_ARRAY:
                    writer.writeBooleanArray(fieldName, record.readBooleanArray(fieldName));
                    break;
                case CHAR_ARRAY:
                    writer.writeCharArray(fieldName, record.readCharArray(fieldName));
                    break;
                case SHORT_ARRAY:
                    writer.writeShortArray(fieldName, record.readShortArray(fieldName));
                    break;
                case INT_ARRAY:
                    writer.writeIntArray(fieldName, record.readIntArray(fieldName));
                    break;
                case LONG_ARRAY:
                    writer.writeLongArray(fieldName, record.readLongArray(fieldName));
                    break;
                case FLOAT_ARRAY:
                    writer.writeFloatArray(fieldName, record.readFloatArray(fieldName));
                    break;
                case DOUBLE_ARRAY:
                    writer.writeDoubleArray(fieldName, record.readDoubleArray(fieldName));
                    break;
                case UTF_ARRAY:
                    writer.writeUTFArray(fieldName, record.readUTFArray(fieldName));
                    break;
                default:
                    throw new IllegalStateException("Unexpected field type: " + cd.getFieldType(fieldName));
            }
        }
        writer.end();
    }

    <T> T readAsObject(BufferObjectDataInput in, int factoryId, int classId) throws IOException {
        Portable portable = createNewPortableInstance(factoryId, classId);
        if (portable == null) {
            throw new HazelcastSerializationException("Could not find PortableFactory for factory-id: " + factoryId
                    + ", class-id:" + classId);
        }
        readPortable(in, factoryId, classId, portable);
        final ManagedContext managedContext = context.getManagedContext();
        return managedContext != null ? (T) managedContext.initialize(portable) : (T) portable;
    }

    <T> T readAndInitialize(BufferObjectDataInput in, int factoryId, int classId,
                            boolean readGenericLazy) throws IOException {
        if (readGenericLazy) {
            int version = in.readInt();
            ClassDefinition cd = setupPositionAndDefinition(in, factoryId, classId, version);
            PortableInternalGenericRecord reader = new PortableInternalGenericRecord(this, in, cd, true);
            return (T) reader;
        }
        return readPortableGenericRecord(in, factoryId, classId);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    private <T> T readPortableGenericRecord(BufferObjectDataInput in, int factoryId, int classId) throws IOException {
        int version = in.readInt();
        ClassDefinition cd = setupPositionAndDefinition(in, factoryId, classId, version);
        GenericRecord reader = new PortableInternalGenericRecord(this, in, cd, false);
        GenericRecord.Builder genericRecordBuilder = GenericRecord.Builder.portable(cd);
        for (String fieldName : cd.getFieldNames()) {
            switch (cd.getFieldType(fieldName)) {
                case PORTABLE:
                    genericRecordBuilder.writeGenericRecord(fieldName, reader.readGenericRecord(fieldName));
                    break;
                case BYTE:
                    genericRecordBuilder.writeByte(fieldName, reader.readByte(fieldName));
                    break;
                case BOOLEAN:
                    genericRecordBuilder.writeBoolean(fieldName, reader.readBoolean(fieldName));
                    break;
                case CHAR:
                    genericRecordBuilder.writeChar(fieldName, reader.readChar(fieldName));
                    break;
                case SHORT:
                    genericRecordBuilder.writeShort(fieldName, reader.readShort(fieldName));
                    break;
                case INT:
                    genericRecordBuilder.writeInt(fieldName, reader.readInt(fieldName));
                    break;
                case LONG:
                    genericRecordBuilder.writeLong(fieldName, reader.readLong(fieldName));
                    break;
                case FLOAT:
                    genericRecordBuilder.writeFloat(fieldName, reader.readFloat(fieldName));
                    break;
                case DOUBLE:
                    genericRecordBuilder.writeDouble(fieldName, reader.readDouble(fieldName));
                    break;
                case UTF:
                    genericRecordBuilder.writeUTF(fieldName, reader.readUTF(fieldName));
                    break;
                case PORTABLE_ARRAY:
                    genericRecordBuilder.writeGenericRecordArray(fieldName, reader.readGenericRecordArray(fieldName));
                    break;
                case BYTE_ARRAY:
                    genericRecordBuilder.writeByteArray(fieldName, reader.readByteArray(fieldName));
                    break;
                case BOOLEAN_ARRAY:
                    genericRecordBuilder.writeBooleanArray(fieldName, reader.readBooleanArray(fieldName));
                    break;
                case CHAR_ARRAY:
                    genericRecordBuilder.writeCharArray(fieldName, reader.readCharArray(fieldName));
                    break;
                case SHORT_ARRAY:
                    genericRecordBuilder.writeShortArray(fieldName, reader.readShortArray(fieldName));
                    break;
                case INT_ARRAY:
                    genericRecordBuilder.writeIntArray(fieldName, reader.readIntArray(fieldName));
                    break;
                case LONG_ARRAY:
                    genericRecordBuilder.writeLongArray(fieldName, reader.readLongArray(fieldName));
                    break;
                case FLOAT_ARRAY:
                    genericRecordBuilder.writeFloatArray(fieldName, reader.readFloatArray(fieldName));
                    break;
                case DOUBLE_ARRAY:
                    genericRecordBuilder.writeDoubleArray(fieldName, reader.readDoubleArray(fieldName));
                    break;
                case UTF_ARRAY:
                    genericRecordBuilder.writeUTFArray(fieldName, reader.readUTFArray(fieldName));
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + cd.getFieldType(fieldName));
            }
        }
        return (T) genericRecordBuilder.build();
    }
}

