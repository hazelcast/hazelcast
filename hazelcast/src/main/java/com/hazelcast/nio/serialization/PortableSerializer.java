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
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

final class PortableSerializer implements StreamSerializer<Portable> {

    private final SerializationContext context;
    private final Map<Integer, PortableFactory> factories = new HashMap<Integer, PortableFactory>();

    PortableSerializer(SerializationContext context, Map<Integer, ? extends PortableFactory> portableFactories) {
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
        if (!(out instanceof BufferObjectDataOutput)) {
            throw new IllegalArgumentException("ObjectDataOutput must be instance of BufferObjectDataOutput!");
        }
        ClassDefinition cd = context.lookupOrRegisterClassDefinition(p);

        BufferObjectDataOutput bufferedOut = (BufferObjectDataOutput) out;
        DefaultPortableWriter writer = new DefaultPortableWriter(this, bufferedOut, cd);
        p.writePortable(writer);
        writer.end();
    }

    public Portable read(ObjectDataInput in) throws IOException {
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of BufferObjectDataInput!");
        }
        if (!(in instanceof PortableContextAwareInputStream)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of PortableContextAwareInputStream!");
        }
        final PortableContextAwareInputStream ctxIn = (PortableContextAwareInputStream) in;
        final int factoryId = ctxIn.getFactoryId();
        final int dataClassId = ctxIn.getClassId();
        final int dataVersion = ctxIn.getVersion();

        final PortableFactory portableFactory = factories.get(factoryId);
        if (portableFactory == null) {
            throw new HazelcastSerializationException("Could not find PortableFactory for factory-id: " + factoryId);
        }
        final Portable portable = portableFactory.create(dataClassId);
        if (portable == null) {
            throw new HazelcastSerializationException("Could not create Portable for class-id: " + dataClassId);
        }
        final DefaultPortableReader reader;
        final ClassDefinition cd;
        final BufferObjectDataInput bufferedIn = (BufferObjectDataInput) in;
        if (context.getVersion() == dataVersion) {
            cd = context.lookup(factoryId, dataClassId);
            // using context.version
            if (cd == null) {
                throw new HazelcastSerializationException("Could not find class-definition for "
                        + "factory-id: " + factoryId + ", class-id: " + dataClassId + ", version: " + dataVersion);
            }
            reader = new DefaultPortableReader(this, bufferedIn, cd);
        } else {
            cd = context.lookup(factoryId, dataClassId, dataVersion);
            // registered during read
            if (cd == null) {
                throw new HazelcastSerializationException("Could not find class-definition for "
                        + "factory-id: " + factoryId + ", class-id: " + dataClassId + ", version: " + dataVersion);
            }
            reader = new MorphingPortableReader(this, bufferedIn, cd);
        }
        portable.readPortable(reader);
        reader.end();
        return portable;
    }

    Portable readAndInitialize(BufferObjectDataInput in) throws IOException {
        Portable p = read(in);
        final ManagedContext managedContext = context.getManagedContext();
        return managedContext != null ? (Portable) managedContext.initialize(p) : p;
    }

    public void destroy() {
        factories.clear();
    }

}

