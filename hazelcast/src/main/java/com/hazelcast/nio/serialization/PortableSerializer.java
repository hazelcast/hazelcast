/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

final class PortableSerializer implements StreamSerializer<Portable> {

    private final PortableContextImpl context;
    private final Map<Integer, PortableFactory> factories = new HashMap<Integer, PortableFactory>();

    PortableSerializer(PortableContextImpl context, Map<Integer, ? extends PortableFactory> portableFactories) {
        this.context = context;
        factories.putAll(portableFactories);
    }

    public int getTypeId() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE;
    }

    public void write(ObjectDataOutput out, Portable p) throws IOException {
        if (!(out instanceof BufferObjectDataOutput)) {
            throw new IllegalArgumentException("ObjectDataOutput must be instance of BufferObjectDataOutput!");
        }
        if (p.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }

        out.writeInt(p.getFactoryId());
        out.writeInt(p.getClassId());
        writeInternal((BufferObjectDataOutput) out, p);
    }

    void writeInternal(BufferObjectDataOutput out, Portable p) throws IOException {

        ClassDefinition cd = context.lookupOrRegisterClassDefinition(p);
        out.writeInt(cd.getVersion());

        DefaultPortableWriter writer = new DefaultPortableWriter(this, out, cd);
        p.writePortable(writer);
        writer.end();
    }

    public Portable read(ObjectDataInput in) throws IOException {
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of BufferObjectDataInput!");
        }

        int factoryId = in.readInt();
        int classId = in.readInt();
        return read((BufferObjectDataInput) in, factoryId, classId);
    }

    private Portable read(BufferObjectDataInput in, int factoryId, int classId) throws IOException {
        int version = in.readInt();

        Portable portable = createNewPortableInstance(factoryId, classId);
        int portableVersion = findPortableVersion(factoryId, classId, portable);

        DefaultPortableReader reader = createReader(in, factoryId, classId,
                version, portableVersion);
        portable.readPortable(reader);
        reader.end();
        return portable;
    }

    private int findPortableVersion(int factoryId, int classId, Portable portable) {
        int currentVersion = context.getClassVersion(factoryId, classId);
        if (currentVersion < 0) {
            currentVersion = PortableVersionHelper.getVersion(portable, context.getVersion());
            if (currentVersion > 0) {
                context.setClassVersion(factoryId, classId, currentVersion);
            }
        }
        return currentVersion;
    }

    private Portable createNewPortableInstance(int factoryId, int classId) {
        final PortableFactory portableFactory = factories.get(factoryId);
        if (portableFactory == null) {
            throw new HazelcastSerializationException("Could not find PortableFactory for factory-id: " + factoryId);
        }
        final Portable portable = portableFactory.create(classId);
        if (portable == null) {
            throw new HazelcastSerializationException("Could not create Portable for class-id: " + classId);
        }
        return portable;
    }

    Portable readAndInitialize(BufferObjectDataInput in, int factoryId, int classId) throws IOException {
        Portable p = read(in, factoryId, classId);
        final ManagedContext managedContext = context.getManagedContext();
        return managedContext != null ? (Portable) managedContext.initialize(p) : p;
    }

    DefaultPortableReader createReader(BufferObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();

        return createReader(in, factoryId, classId, version, version);
    }

    private DefaultPortableReader createReader(BufferObjectDataInput in, int factoryId, int classId, int version,
            int portableVersion) throws IOException {

        int effectiveVersion = version;
        if (version < 0) {
            effectiveVersion = context.getVersion();
        }

        ClassDefinition cd = context.lookupClassDefinition(factoryId, classId, effectiveVersion);
        if (cd == null) {
            int begin = in.position();
            cd = context.readClassDefinition(in, factoryId, classId, effectiveVersion);
            in.position(begin);
        }

        DefaultPortableReader reader;
        if (portableVersion == effectiveVersion) {
            reader = new DefaultPortableReader(this, in, cd);
        } else {
            reader = new MorphingPortableReader(this, in, cd);
        }
        return reader;
    }

    public void destroy() {
        factories.clear();
    }
}

