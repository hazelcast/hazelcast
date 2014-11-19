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
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.DynamicByteBuffer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

final class PortableSerializer implements StreamSerializer<Portable> {

    private final PortableContext context;
    private final Map<Integer, PortableFactory> factories = new HashMap<Integer, PortableFactory>();

    PortableSerializer(PortableContext context, Map<Integer, ? extends PortableFactory> portableFactories) {
        this.context = context;
        factories.putAll(portableFactories);
    }

    public int getTypeId() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE;
    }

    public void write(ObjectDataOutput out, Portable p) throws IOException {
        if (!(out instanceof PortableDataOutput)) {
            throw new IllegalArgumentException("ObjectDataOutput must be instance of PortableDataOutput!");
        }
        if (p.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }

        ClassDefinition cd = context.lookupOrRegisterClassDefinition(p);

        PortableDataOutput output = (PortableDataOutput) out;
        DynamicByteBuffer headerBuffer = output.getHeaderBuffer();

        int pos = headerBuffer.position();
        out.writeInt(pos);

        headerBuffer.putInt(cd.getFactoryId());
        headerBuffer.putInt(cd.getClassId());
        headerBuffer.putInt(cd.getVersion());

        DefaultPortableWriter writer = new DefaultPortableWriter(this, output, cd);
        p.writePortable(writer);
        writer.end();
    }

    public Portable read(ObjectDataInput in) throws IOException {
        if (!(in instanceof PortableDataInput)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of PortableDataInput!");
        }

        PortableDataInput input = (PortableDataInput) in;
        ByteBuffer headerBuffer = input.getHeaderBuffer();

        int pos = input.readInt();
        headerBuffer.position(pos);

        int factoryId = headerBuffer.getInt();
        int classId = headerBuffer.getInt();
        int version = headerBuffer.getInt();

        Portable portable = createNewPortableInstance(factoryId, classId);
        int portableVersion = findPortableVersion(factoryId, classId, portable);

        DefaultPortableReader reader = createReader(input, factoryId, classId, version, portableVersion);
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

    Portable readAndInitialize(BufferObjectDataInput in) throws IOException {
        Portable p = read(in);
        final ManagedContext managedContext = context.getManagedContext();
        return managedContext != null ? (Portable) managedContext.initialize(p) : p;
    }

    DefaultPortableReader createReader(BufferObjectDataInput in) throws IOException {
        ByteBuffer headerBuffer = ((PortableDataInput) in).getHeaderBuffer();

        int pos = in.readInt();
        headerBuffer.position(pos);

        int factoryId = headerBuffer.getInt();
        int classId = headerBuffer.getInt();
        int version = headerBuffer.getInt();

        return createReader(in, factoryId, classId, version, version);
    }

    private DefaultPortableReader createReader(BufferObjectDataInput in, int factoryId, int classId, int version,
            int portableVersion) {

        int effectiveVersion = version;
        if (version < 0) {
            effectiveVersion = context.getVersion();
        }

        ClassDefinition cd = context.lookupClassDefinition(factoryId, classId, effectiveVersion);
        if (cd == null) {
            throw new HazelcastSerializationException("Could not find class-definition for "
                    + "factory-id: " + factoryId + ", class-id: " + classId + ", version: " + effectiveVersion);
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

