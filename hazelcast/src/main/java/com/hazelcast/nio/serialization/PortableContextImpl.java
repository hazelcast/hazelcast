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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.hazelcast.nio.Bits.combineToLong;

final class PortableContextImpl implements PortableContext {

    private static final Pattern NESTED_FIELD_PATTERN = Pattern.compile("\\.");
    private static final int COMPRESSION_BUFFER_LENGTH = 1024;

    private final int version;
    private final ConcurrentHashMap<Integer, ClassDefinitionContext> classDefContextMap =
            new ConcurrentHashMap<Integer, ClassDefinitionContext>();

    private final SerializationService serializationService;

    private final ConstructorFunction<Integer, ClassDefinitionContext> constructorFunction =
            new ConstructorFunction<Integer, ClassDefinitionContext>() {
                public ClassDefinitionContext createNew(Integer arg) {
                    return new ClassDefinitionContext(arg);
                }
            };

    PortableContextImpl(SerializationService serializationService, int version) {
        this.serializationService = serializationService;
        this.version = version;
    }

    @Override
    public int getClassVersion(int factoryId, int classId) {
        return getClassDefContext(factoryId).getClassVersion(classId);
    }

    @Override
    public void setClassVersion(int factoryId, int classId, int version) {
        getClassDefContext(factoryId).setClassVersion(classId, version);
    }

    @Override
    public ClassDefinition lookupClassDefinition(int factoryId, int classId, int version) {
        return getClassDefContext(factoryId).lookup(classId, version);
    }

    public ClassDefinition lookupClassDefinition(Data data) {
        if (!data.isPortable()) {
            throw new IllegalArgumentException("Data is not Portable!");
        }

        ByteOrder byteOrder = serializationService.getByteOrder();
        return readClassDefinition(data, 0, byteOrder);
    }

    private ClassDefinition readClassDefinition(Data data, int start, ByteOrder order) {
        int factoryId =  data.readIntHeader(start + HEADER_FACTORY_OFFSET, order);
        int classId = data.readIntHeader(start + HEADER_CLASS_OFFSET, order);
        int version = data.readIntHeader(start + HEADER_VERSION_OFFSET, order);
        return lookupClassDefinition(factoryId, classId, version);
    }

    @Override
    public boolean hasClassDefinition(Data data) {
        if (data.isPortable()) {
            return true;
        }
        return data.headerSize() > 0;
    }

    @Override
    public ClassDefinition[] getClassDefinitions(Data data) {
        if (data.headerSize() == 0) {
            return null;
        }

        int len =  data.headerSize();
        if (len % HEADER_ENTRY_LENGTH != 0) {
            throw new AssertionError("Header length should be factor of " + HEADER_ENTRY_LENGTH);
        }
        int k = len / HEADER_ENTRY_LENGTH;

        ByteOrder byteOrder = serializationService.getByteOrder();
        ClassDefinition[] definitions = new ClassDefinition[k];
        for (int i = 0; i < k; i++) {
            definitions[i] = readClassDefinition(data, i * HEADER_ENTRY_LENGTH, byteOrder);
        }
        return definitions;
    }

    @Override
    public ClassDefinition createClassDefinition(int factoryId, final byte[] compressedBinary) throws IOException {
        return getClassDefContext(factoryId).create(compressedBinary);
    }

    @Override
    public ClassDefinition registerClassDefinition(final ClassDefinition cd) {
        return getClassDefContext(cd.getFactoryId()).register(cd);
    }

    @Override
    public ClassDefinition lookupOrRegisterClassDefinition(Portable p) throws IOException {
        int portableVersion = PortableVersionHelper.getVersion(p, version);
        ClassDefinition cd = lookupClassDefinition(p.getFactoryId(), p.getClassId(), portableVersion);
        if (cd == null) {
            ClassDefinitionWriter writer = new ClassDefinitionWriter(this, p.getFactoryId(),
                    p.getClassId(), portableVersion);
            p.writePortable(writer);
            cd = writer.registerAndGet();
        }
        return cd;
    }

    @Override
    public FieldDefinition getFieldDefinition(ClassDefinition classDef, String name) {
        FieldDefinition fd = classDef.getField(name);
        if (fd == null) {
            String[] fieldNames = NESTED_FIELD_PATTERN.split(name);
            if (fieldNames.length > 1) {
                ClassDefinition currentClassDef = classDef;
                for (int i = 0; i < fieldNames.length; i++) {
                    name = fieldNames[i];
                    fd = currentClassDef.getField(name);
                    if (i == fieldNames.length - 1) {
                        break;
                    }
                    if (fd == null) {
                        throw new IllegalArgumentException("Unknown field: " + name);
                    }
                    currentClassDef = lookupClassDefinition(fd.getFactoryId(), fd.getClassId(),
                            currentClassDef.getVersion());
                    if (currentClassDef == null) {
                        throw new IllegalArgumentException("Not a registered Portable field: " + fd);
                    }
                }
            }
        }
        return fd;
    }

    private ClassDefinitionContext getClassDefContext(int factoryId) {
        return ConcurrencyUtil.getOrPutIfAbsent(classDefContextMap, factoryId, constructorFunction);
    }

    public int getVersion() {
        return version;
    }

    public ManagedContext getManagedContext() {
        return serializationService.getManagedContext();
    }

    @Override
    public ByteOrder getByteOrder() {
        return serializationService.getByteOrder();
    }

    private final class ClassDefinitionContext {

        final int factoryId;
        final ConcurrentMap<Long, ClassDefinition> versionedDefinitions = new ConcurrentHashMap<Long, ClassDefinition>();
        final ConcurrentMap<Integer, Integer> currentClassVersions = new ConcurrentHashMap<Integer, Integer>();

        private ClassDefinitionContext(int factoryId) {
            this.factoryId = factoryId;
        }

        int getClassVersion(int classId) {
            Integer version = currentClassVersions.get(classId);
            return version != null ? version : -1;
        }

        void setClassVersion(int classId, int version) {
            Integer current = currentClassVersions.putIfAbsent(classId, version);
            if (current != null && current != version) {
                throw new IllegalArgumentException("Class-id: " + classId + " is already registered!");
            }
        }

        ClassDefinition lookup(int classId, int version) {
            long versionedClassId = combineToLong(classId, version);
            ClassDefinition cd = versionedDefinitions.get(versionedClassId);
            if (cd instanceof BinaryClassDefinitionProxy) {
                try {
                    cd = create(((BinaryClassDefinitionProxy) cd).getBinary());
                } catch (IOException e) {
                    throw new HazelcastSerializationException(e);
                }
            }
            return cd;
        }

        ClassDefinition create(byte[] compressedBinary) throws IOException {
            ClassDefinition cd = toClassDefinition(compressedBinary);
            return register(cd);
        }

        ClassDefinition register(ClassDefinition cd) {
            if (cd == null) {
                return null;
            }
            if (cd.getFactoryId() != factoryId) {
                throw new HazelcastSerializationException("Invalid factory-id! " + factoryId + " -> " + cd);
            }
            if (cd instanceof ClassDefinitionImpl) {
                final ClassDefinitionImpl cdImpl = (ClassDefinitionImpl) cd;
                cdImpl.setVersionIfNotSet(getVersion());
                setClassDefBinary(cdImpl);
            }
            final long versionedClassId = combineToLong(cd.getClassId(), cd.getVersion());
            final ClassDefinition currentCd = versionedDefinitions.putIfAbsent(versionedClassId, cd);
            if (currentCd == null) {
                return cd;
            }
            if (currentCd instanceof ClassDefinitionImpl) {
                if (!currentCd.equals(cd)) {
                    throw new HazelcastSerializationException("Incompatible class-definitions with same class-id: "
                            + cd + " VS " + currentCd);
                }
                return currentCd;
            }
            versionedDefinitions.put(versionedClassId, cd);
            return cd;
        }

        private void setClassDefBinary(ClassDefinitionImpl cd) {
            if (cd.getBinary() == null) {
                try {
                    byte[] binary = toClassDefinitionBinary(cd);
                    cd.setBinary(binary);
                } catch (IOException e) {
                    throw new HazelcastSerializationException(e);
                }
            }
        }

        private byte[] toClassDefinitionBinary(ClassDefinition cd) throws IOException {
            BufferObjectDataOutput out = serializationService.pop();
            try {
                writeClassDefinition(cd, out);
                byte[] binary = out.toByteArray();
                out.clear();
                compress(binary, out);
                return out.toByteArray();
            } finally {
                serializationService.push(out);
            }
        }

        private ClassDefinition toClassDefinition(byte[] compressedBinary) throws IOException {

            if (compressedBinary == null || compressedBinary.length == 0) {
                throw new IOException("Illegal class-definition binary! ");
            }

            BufferObjectDataOutput out = serializationService.pop();
            byte[] binary;
            try {
                decompress(compressedBinary, out);
                binary = out.toByteArray();
            } finally {
                serializationService.push(out);
            }

            ClassDefinitionImpl cd = readClassDefinition(serializationService.createObjectDataInput(binary));
            if (cd.getVersion() < 0) {
                throw new IOException("ClassDefinition version cannot be negative! -> " + cd);
            }
            cd.setBinary(compressedBinary);
            return cd;
        }
    }

    /**
     * Writes a ClassDefinition to a stream.
     *
     * @param classDefinition ClassDefinition
     * @param out             stream to write ClassDefinition
     */
    private static void writeClassDefinition(ClassDefinition classDefinition, ObjectDataOutput out) throws IOException {
        ClassDefinitionImpl cd = (ClassDefinitionImpl) classDefinition;

        out.writeInt(cd.getFactoryId());
        out.writeInt(cd.getClassId());
        out.writeInt(cd.getVersion());

        Collection<FieldDefinition> fieldDefinitions = cd.getFieldDefinitions();
        out.writeShort(fieldDefinitions.size());

        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            writeFieldDefinition((FieldDefinitionImpl) fieldDefinition, out);
        }
    }

    /**
     * Reads a ClassDefinition from a stream.
     *
     * @param in stream to write ClassDefinition
     * @return ClassDefinition
     */
    private static ClassDefinitionImpl readClassDefinition(ObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();

        if (classId == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }

        ClassDefinitionImpl cd = new ClassDefinitionImpl(factoryId, classId, version);
        int len = in.readShort();

        for (int i = 0; i < len; i++) {
            FieldDefinitionImpl fd = readFieldDefinition(in);
            cd.addFieldDef(fd);
        }

        return cd;
    }

    /**
     * Writes a FieldDefinition to a stream.
     *
     * @param fd FieldDefinition
     * @param out             stream to write FieldDefinition
     */
    private static void writeFieldDefinition(FieldDefinitionImpl fd, ObjectDataOutput out) throws IOException {
        out.writeInt(fd.index);
        out.writeUTF(fd.fieldName);
        out.writeByte(fd.type.getId());
        out.writeInt(fd.factoryId);
        out.writeInt(fd.classId);
    }

    /**
     * Reads a FieldDefinition from a stream.
     *
     * @param in stream to write FieldDefinition
     * @return FieldDefinition
     */
    private static FieldDefinitionImpl readFieldDefinition(ObjectDataInput in) throws IOException {
        int index = in.readInt();
        String name = in.readUTF();
        byte typeId = in.readByte();
        int factoryId = in.readInt();
        int classId = in.readInt();

        return new FieldDefinitionImpl(index, name, FieldType.get(typeId), factoryId, classId);
    }

    private static void compress(byte[] input, DataOutput out) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setLevel(Deflater.DEFAULT_COMPRESSION);
        deflater.setStrategy(Deflater.FILTERED);
        deflater.setInput(input);
        deflater.finish();
        byte[] buf = new byte[COMPRESSION_BUFFER_LENGTH];
        while (!deflater.finished()) {
            int count = deflater.deflate(buf);
            out.write(buf, 0, count);
        }
        deflater.end();
    }

    private static void decompress(byte[] compressedData, DataOutput out) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedData);
        byte[] buf = new byte[COMPRESSION_BUFFER_LENGTH];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buf);
                out.write(buf, 0, count);
            } catch (DataFormatException e) {
                throw new IOException(e);
            }
        }
        inflater.end();
    }

}
