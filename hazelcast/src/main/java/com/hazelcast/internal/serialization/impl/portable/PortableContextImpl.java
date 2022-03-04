/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import static com.hazelcast.internal.nio.Bits.combineToLong;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;

public final class PortableContextImpl implements PortableContext {

    private static final Pattern NESTED_FIELD_PATTERN = Pattern.compile("\\.");

    private final int version;
    private final ConcurrentHashMap<Integer, ClassDefinitionContext> classDefContextMap = new ConcurrentHashMap<>();
    private final InternalSerializationService serializationService;
    private final boolean checkClassDefErrors;

    public PortableContextImpl(InternalSerializationService serializationService, int version, boolean checkClassDefErrors) {
        this.serializationService = serializationService;
        this.version = version;
        this.checkClassDefErrors = checkClassDefErrors;
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

    @Override
    public ClassDefinition lookupClassDefinition(Data data) throws IOException {
        if (!data.isPortable()) {
            throw new IllegalArgumentException("Data is not Portable!");
        }
        BufferObjectDataInput in = serializationService.createObjectDataInput(data);
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();

        ClassDefinition classDefinition = lookupClassDefinition(factoryId, classId, version);
        if (classDefinition == null) {
            classDefinition = readClassDefinition(in, factoryId, classId, version);
        }
        return classDefinition;
    }

    ClassDefinition readClassDefinition(BufferObjectDataInput in, int factoryId, int classId, int version)
            throws IOException {
        boolean register = true;
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(factoryId, classId, version);

        // final position after portable is read
        in.readInt();

        // field count
        int fieldCount = in.readInt();
        int offset = in.position();
        for (int i = 0; i < fieldCount; i++) {
            int pos = in.readInt(offset + i * Bits.INT_SIZE_IN_BYTES);
            in.position(pos);

            short len = in.readShort();
            char[] chars = new char[len];
            for (int k = 0; k < len; k++) {
                chars[k] = (char) in.readUnsignedByte();
            }

            FieldType type = FieldType.get(in.readByte());
            String name = new String(chars);
            int fieldFactoryId = 0;
            int fieldClassId = 0;
            int fieldVersion = version;
            if (type == FieldType.PORTABLE) {
                // is null
                if (in.readBoolean()) {
                    register = false;
                }
                fieldFactoryId = in.readInt();
                fieldClassId = in.readInt();

                // TODO: what there's a null inner Portable field
                if (register) {
                    fieldVersion = in.readInt();
                    readClassDefinition(in, fieldFactoryId, fieldClassId, fieldVersion);
                }
            } else if (type == FieldType.PORTABLE_ARRAY) {
                int k = in.readInt();
                fieldFactoryId = in.readInt();
                fieldClassId = in.readInt();

                // TODO: what there's a null inner Portable field
                if (k > 0) {
                    int p = in.readInt();
                    in.position(p);

                    fieldVersion = in.readInt();
                    readClassDefinition(in, fieldFactoryId, fieldClassId, fieldVersion);
                } else {
                    register = false;
                }

            }
            builder.addField(new FieldDefinitionImpl(i, name, type, fieldFactoryId, fieldClassId, fieldVersion));
        }
        ClassDefinition classDefinition = builder.build();
        if (register) {
            classDefinition = registerClassDefinition(classDefinition);
        }
        return classDefinition;
    }

    @Override
    public ClassDefinition registerClassDefinition(final ClassDefinition cd) {
        return getClassDefContext(cd.getFactoryId()).register(cd, true);
    }

    @Override
    public ClassDefinition registerClassDefinition(ClassDefinition cd, boolean throwOnIncompatibleClassDefinitions) {
        return getClassDefContext(cd.getFactoryId()).register(cd, throwOnIncompatibleClassDefinitions);
    }

    @Override
    public ClassDefinition lookupOrRegisterClassDefinition(Portable p) throws IOException {
        int portableVersion = SerializationUtil.getPortableVersion(p, version);
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
    // TODO cache the result
    public FieldDefinition getFieldDefinition(ClassDefinition classDef, String name) {
        FieldDefinition fd = classDef.getField(name);
        if (fd == null) {
            if (name.contains(".")) {
                String[] fieldNames = NESTED_FIELD_PATTERN.split(name);
                if (fieldNames.length <= 1) {
                    return fd;
                }
                ClassDefinition currentClassDef = classDef;
                for (int i = 0; i < fieldNames.length; i++) {
                    fd = currentClassDef.getField(fieldNames[i]);
                    if (fd == null) {
                        fd = currentClassDef.getField(extractAttributeNameNameWithoutArguments(fieldNames[i]));
                    }
                    // This is not enough to fully implement issue: https://github.com/hazelcast/hazelcast/issues/3927
                    if (i == fieldNames.length - 1) {
                        break;
                    }
                    if (fd == null) {
                        throw new IllegalArgumentException("Unknown field: " + name);
                    }
                    currentClassDef = lookupClassDefinition(fd.getFactoryId(), fd.getClassId(),
                            fd.getVersion());
                    if (currentClassDef == null) {
                        throw new IllegalArgumentException("Not a registered Portable field: " + fd);
                    }
                }
            } else {
                fd = classDef.getField(extractAttributeNameNameWithoutArguments(name));
            }
        }
        return fd;
    }

    private ClassDefinitionContext getClassDefContext(int factoryId) {
        return ConcurrencyUtil.getOrPutIfAbsent(classDefContextMap, factoryId, ClassDefinitionContext::new);
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public ManagedContext getManagedContext() {
        return serializationService.getManagedContext();
    }

    @Override
    public ByteOrder getByteOrder() {
        return serializationService.getByteOrder();
    }

    public boolean shouldCheckClassDefinitionErrors() {
        return checkClassDefErrors;
    }

    private final class ClassDefinitionContext {

        final int factoryId;
        final ConcurrentMap<Long, ClassDefinition> versionedDefinitions = new ConcurrentHashMap<>();
        final ConcurrentMap<Integer, Integer> currentClassVersions = new ConcurrentHashMap<>();

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
            return versionedDefinitions.get(versionedClassId);
        }

        ClassDefinition register(ClassDefinition cd, boolean throwOnIncompatibleClassDefinitions) {
            if (cd == null) {
                return null;
            }
            if (cd instanceof ClassDefinitionImpl) {
                final ClassDefinitionImpl cdImpl = (ClassDefinitionImpl) cd;
                cdImpl.setVersionIfNotSet(getVersion());
            }
            final long versionedClassId = combineToLong(cd.getClassId(), cd.getVersion());
            final ClassDefinition existingCd = versionedDefinitions.putIfAbsent(versionedClassId, cd);
            if (existingCd != null && throwOnIncompatibleClassDefinitions && !existingCd.equals(cd)) {
                throw new HazelcastSerializationException(
                        "Incompatible class definitions are found. New class definition: " + cd
                                + ", existing class definition " + existingCd);
            }
            return cd;
        }

    }

}
