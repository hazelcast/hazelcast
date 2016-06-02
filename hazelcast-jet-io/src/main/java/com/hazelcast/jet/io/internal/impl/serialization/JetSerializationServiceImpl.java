/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.io.internal.impl.serialization;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.jet.io.api.serialization.JetInputFactory;
import com.hazelcast.jet.io.api.serialization.JetOutputFactory;
import com.hazelcast.jet.io.api.serialization.JetDataInput;
import com.hazelcast.jet.io.api.serialization.JetDataOutput;
import com.hazelcast.jet.io.api.serialization.JetSerializationService;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;

import java.io.IOException;
import java.nio.ByteOrder;

public final class JetSerializationServiceImpl implements JetSerializationService {
    private final JetInputFactory inputFactory = new JetInputOutputFactory();
    private final JetOutputFactory outputFactory = new JetInputOutputFactory();
    private final InternalSerializationService hazelcastSerialization = new DefaultSerializationServiceBuilder().build();

    @Override
    public JetDataInput createObjectDataInput(MemoryManager memoryManager, boolean useBigEndian) {
        return inputFactory.createInput(memoryManager, this, useBigEndian);
    }

    @Override
    public JetDataOutput createObjectDataOutput(MemoryManager memoryManager, boolean useBigEndian) {
        return outputFactory.createOutput(memoryManager, this, useBigEndian);
    }

    @Override
    public <B extends Data> B toData(Object obj) {
        return hazelcastSerialization.toData(obj);
    }

    @Override
    public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
        return hazelcastSerialization.toData(obj, strategy);
    }

    @Override
    public byte[] toBytes(Object obj) {
        return hazelcastSerialization.toBytes(obj);
    }

    @Override
    public byte[] toBytes(Object obj, PartitioningStrategy strategy) {
        return hazelcastSerialization.toBytes(obj, strategy);
    }

    @Override
    public <T> T toObject(Object data) {
        return hazelcastSerialization.toObject(data);
    }

    @Override
    public void writeObject(ObjectDataOutput out, Object obj) {
        hazelcastSerialization.writeObject(out, obj);
    }

    @Override
    public <T> T readObject(ObjectDataInput in) {
        return hazelcastSerialization.readObject(in);
    }

    @Override
    public void disposeData(Data data) {
        hazelcastSerialization.disposeData(data);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(byte[] data) {
        return hazelcastSerialization.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(Data data) {
        return hazelcastSerialization.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput(int size) {
        return hazelcastSerialization.createObjectDataOutput(size);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput() {
        return hazelcastSerialization.createObjectDataOutput();
    }

    @Override
    public PortableReader createPortableReader(Data data) throws IOException {
        return hazelcastSerialization.createPortableReader(data);
    }

    @Override
    public PortableContext getPortableContext() {
        return hazelcastSerialization.getPortableContext();
    }

    @Override
    public ClassLoader getClassLoader() {
        return hazelcastSerialization.getClassLoader();
    }

    @Override
    public ManagedContext getManagedContext() {
        return hazelcastSerialization.getManagedContext();
    }

    @Override
    public ByteOrder getByteOrder() {
        return hazelcastSerialization.getByteOrder();
    }

    @Override
    public byte getVersion() {
        return hazelcastSerialization.getVersion();
    }

    @Override
    public void dispose() {
        hazelcastSerialization.dispose();
    }
}
