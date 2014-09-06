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
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.hazelcast.util.ByteUtil.combineToLong;

final class PortableContextImpl implements PortableContext {

    private static final int COMPRESSION_BUFFER_LENGTH = 1024;
    private static final Pattern NESTED_FIELD_PATTERN = Pattern.compile("\\.");

    private final int version;
    private final ConcurrentHashMap<Integer, ClassDefinitionContext> classDefContextMap =
            new ConcurrentHashMap<Integer, ClassDefinitionContext>();
    private final SerializationServiceImpl serializationService;
    private final ConstructorFunction<Integer, ClassDefinitionContext> constructorFunction =
            new ConstructorFunction<Integer, ClassDefinitionContext>() {
                public ClassDefinitionContext createNew(Integer arg) {
                    return new ClassDefinitionContext();
                }
            };

    PortableContextImpl(SerializationServiceImpl serializationService, Collection<Integer> portableFactories,
            int version) {
        this.serializationService = serializationService;
        this.version = version;

        for (int factoryId : portableFactories) {
            classDefContextMap.put(factoryId, new ClassDefinitionContext());
        }
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
    public ClassDefinition lookup(int factoryId, int classId, int version) {
        return getClassDefContext(factoryId).lookup(classId, version);
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
        ClassDefinition cd = lookup(p.getFactoryId(), p.getClassId(), portableVersion);
        if (cd == null) {
            ClassDefinitionWriter classDefinitionWriter = new ClassDefinitionWriter(this, p.getFactoryId(),
                    p.getClassId(), portableVersion);
            p.writePortable(classDefinitionWriter);
            cd = classDefinitionWriter.registerAndGet();
        }
        return cd;
    }

    private void registerNestedDefinitions(ClassDefinitionImpl cd) {
        Collection<ClassDefinition> nestedDefinitions = cd.getNestedClassDefinitions();
        for (ClassDefinition classDefinition : nestedDefinitions) {
            final ClassDefinitionImpl nestedCD = (ClassDefinitionImpl) classDefinition;
            registerClassDefinition(nestedCD);
        }
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
                    currentClassDef = lookup(fd.getFactoryId(), fd.getClassId(), fd.getVersion());
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

    private class ClassDefinitionContext {

        final ConcurrentMap<Long, ClassDefinition> versionedDefinitions = new ConcurrentHashMap<Long, ClassDefinition>();
        final ConcurrentMap<Integer, Integer> currentClassVersions = new ConcurrentHashMap<Integer, Integer>();

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
            if (compressedBinary == null || compressedBinary.length == 0) {
                throw new IOException("Illegal class-definition binary! ");
            }
            final byte[] binary = getClassDefBinary(compressedBinary);
            final ClassDefinitionImpl cd = new ClassDefinitionImpl();
            cd.readData(serializationService.createObjectDataInput(binary));
            if (cd.getVersion() < 0) {
                throw new IOException("ClassDefinition version cannot be negative! -> " + cd);
            }
            cd.setBinary(compressedBinary);
            return register(cd);
        }

        private byte[] getClassDefBinary(byte[] compressedBinary) throws IOException {
            final BufferObjectDataOutput out = serializationService.pop();
            final byte[] binary;
            try {
                decompress(compressedBinary, out);
                binary = out.toByteArray();
            } finally {
                serializationService.push(out);
            }
            return binary;
        }

        ClassDefinition register(ClassDefinition cd) {
            if (cd == null) {
                return null;
            }
            if (cd instanceof ClassDefinitionImpl) {
                final ClassDefinitionImpl cdImpl = (ClassDefinitionImpl) cd;
                cdImpl.setVersionIfNotSet(getVersion());
                setClassDefBinary(cdImpl);
                registerNestedDefinitions(cdImpl);
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
                final BufferObjectDataOutput out = serializationService.pop();
                try {
                    cd.writeData(out);
                    final byte[] binary = out.toByteArray();
                    out.clear();
                    compress(binary, out);
                    cd.setBinary(out.toByteArray());
                } catch (IOException e) {
                    throw new HazelcastSerializationException(e);
                } finally {
                    serializationService.push(out);
                }
            }
        }
    }

    static void compress(byte[] input, DataOutput out) throws IOException {
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

    static void decompress(byte[] compressedData, DataOutput out) throws IOException {
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
