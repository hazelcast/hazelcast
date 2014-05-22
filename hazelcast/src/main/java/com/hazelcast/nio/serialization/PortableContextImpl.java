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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

final class PortableContextImpl implements PortableContext {

    final int version;
    final ConcurrentHashMap<Integer, ClassDefinitionContext> classDefContextMap = new ConcurrentHashMap<Integer, ClassDefinitionContext>();
    final SerializationServiceImpl serializationService;
    final ConstructorFunction<Integer, ClassDefinitionContext> constructorFunction =
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
    public Integer getClassVersion(int factoryId, int classId) {
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

        Integer getClassVersion(int classId) {
            return currentClassVersions.get(classId);
        }

        void setClassVersion(int classId, int version) {
            Integer current;
            if ((current = currentClassVersions.putIfAbsent(classId, version)) != null) {
                if (current != version) {
                    throw new IllegalArgumentException("Class-id: " + classId + " is already registered!");
                }
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
            final BufferObjectDataOutput out = serializationService.pop();
            final byte[] binary;
            try {
                decompress(compressedBinary, out);
                binary = out.toByteArray();
            } finally {
                serializationService.push(out);
            }
            final ClassDefinitionImpl cd = new ClassDefinitionImpl();
            cd.readData(serializationService.createObjectDataInput(binary));
            if (cd.getVersion() < 0) {
                throw new IOException("ClassDefinition version cannot be negative! -> " + cd);
            }
            cd.setBinary(compressedBinary);
            return register(cd);
        }

        ClassDefinition register(ClassDefinition cd) {
            if (cd == null) {
                return null;
            }
            if (cd instanceof ClassDefinitionImpl) {
                final ClassDefinitionImpl cdImpl = (ClassDefinitionImpl) cd;
                if (cdImpl.getVersion() < 0) {
                    cdImpl.version = getVersion();
                }
                if (cdImpl.getBinary() == null) {
                    final BufferObjectDataOutput out = serializationService.pop();
                    try {
                        cdImpl.writeData(out);
                        final byte[] binary = out.toByteArray();
                        out.clear();
                        compress(binary, out);
                        cdImpl.setBinary(out.toByteArray());
                    } catch (IOException e) {
                        throw new HazelcastSerializationException(e);
                    } finally {
                        serializationService.push(out);
                    }
                }
                registerNestedDefinitions(cdImpl);
            }
            final long versionedClassId = combineToLong(cd.getClassId(), cd.getVersion());
            final ClassDefinition currentCd = versionedDefinitions.putIfAbsent(versionedClassId, cd);
            if (currentCd == null) {
                return cd;
            }
            if (currentCd instanceof ClassDefinitionImpl) {
                return currentCd;
            }
            versionedDefinitions.put(versionedClassId, cd);
            return cd;
        }
    }

    static void compress(byte[] input, DataOutput out) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setLevel(Deflater.DEFAULT_COMPRESSION);
        deflater.setStrategy(Deflater.FILTERED);
        deflater.setInput(input);
        deflater.finish();
        byte[] buf = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buf);
            out.write(buf, 0, count);
        }
        deflater.end();
    }

    static void decompress(byte[] compressedData, DataOutput out) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedData);
        byte[] buf = new byte[1024];
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

    static long combineToLong(int x, int y) {
        return ((long) x << 32) | ((long) y & 0xFFFFFFFL);
    }

    static int extractInt(long value, boolean lowerBits) {
        return (lowerBits) ? (int) value : (int) (value >> 32);
    }
}
