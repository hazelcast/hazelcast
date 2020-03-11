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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.internal.usercodedeployment.impl.ClassLocator;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.partition.PartitioningStrategy;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createSerializerAdapter;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleSerializeException;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.isNullData;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.Thread.currentThread;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.stream.Collectors.toMap;

// refactor after https://github.com/hazelcast/hazelcast/pull/16722 is released
@Deprecated
public class JetSerializationService implements InternalSerializationService {

    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 4096;

    private final Map<Class<?>, SerializerAdapter> serializersByClass;
    private final Map<Integer, SerializerAdapter> serializersById;

    private final AbstractSerializationService delegate;

    private volatile boolean active;

    public JetSerializationService(Map<Class<?>, ? extends Serializer> serializers,
                                   AbstractSerializationService delegate) {
        Map<Class<?>, SerializerAdapter> serializersByClass = new HashMap<>();
        Map<Integer, SerializerAdapter> serializersById = new HashMap<>();
        serializers.forEach((clazz, serializer) -> {
            if (serializersById.containsKey(serializer.getTypeId())) {
                Serializer registered = serializersById.get(serializer.getTypeId()).getImpl();
                throw new IllegalStateException("Cannot register Serializer[" + serializer.getClass().getName() + "] - " +
                        registered.getClass().getName() + " has been already registered for type ID: " +
                        serializer.getTypeId());
            }

            SerializerAdapter serializerAdapter = createSerializerAdapter(serializer, this);
            serializersByClass.put(clazz, serializerAdapter);
            serializersById.put(serializerAdapter.getImpl().getTypeId(), serializerAdapter);
        });
        this.serializersByClass = serializersByClass;
        this.serializersById = serializersById;

        this.delegate = delegate;

        this.active = true;
    }

    @Override
    public <B extends Data> B toData(Object object, DataType type) {
        checkTrue(type != DataType.NATIVE, "Native data type is not supported");
        return toData(object);
    }

    @Override
    public <B extends Data> B toData(Object object, DataType type, PartitioningStrategy strategy) {
        checkTrue(type != DataType.NATIVE, "Native data type is not supported");
        return toData(object, strategy);
    }

    @Override
    public <B extends Data> B toData(Object object) {
        return toData(object, delegate.globalPartitioningStrategy);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <B extends Data> B toData(Object object, PartitioningStrategy strategy) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (B) object;
        }

        byte[] bytes = toBytes(object, strategy);
        return (B) new HeapData(bytes);
    }

    @Override
    public byte[] toBytes(Object object) {
        return toBytes(object, delegate.globalPartitioningStrategy);
    }

    @Override
    public byte[] toBytes(Object obj, int leftPadding, boolean insertPartitionHash) {
        return toBytes(obj, leftPadding, insertPartitionHash, delegate.globalPartitioningStrategy, getByteOrder());
    }

    private byte[] toBytes(Object obj, PartitioningStrategy<?> strategy) {
        return toBytes(obj, 0, true, strategy, BIG_ENDIAN);
    }

    private byte[] toBytes(Object object,
                           int leftPadding,
                           boolean writeHash,
                           PartitioningStrategy<?> strategy,
                           ByteOrder serializerTypeIdByteOrder) {
        checkNotNull(object);
        checkNotNull(serializerTypeIdByteOrder);

        BufferPool pool = delegate.bufferPoolThreadLocal.get();
        BufferObjectDataOutput out = pool.takeOutputBuffer();
        try {
            out.position(leftPadding);
            SerializerAdapter serializer = serializerFor(object);
            if (writeHash) {
                int partitionHash = delegate.calculatePartitionHash(object, strategy);
                out.writeInt(partitionHash, BIG_ENDIAN);
            }
            out.writeInt(serializer.getTypeId(), serializerTypeIdByteOrder);
            serializer.write(out, object);
            return out.toByteArray();
        } catch (Throwable e) {
            throw handleSerializeException(object, e);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <B extends Data> B convertData(Data data, DataType type) {
        checkTrue(type != DataType.NATIVE, "Native data type is not supported");
        return (B) data;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T toObject(Object object) {
        if (!(object instanceof Data)) {
            return (T) object;
        }

        Data data = (Data) object;
        if (isNullData(data)) {
            return null;
        }

        BufferPool pool = delegate.bufferPoolThreadLocal.get();
        BufferObjectDataInput in = pool.takeInputBuffer(data);
        try {
            ClassLocator.onStartDeserialization();
            int typeId = data.getType();
            SerializerAdapter serializer = serializerFor(typeId);
            Object obj = serializer.read(in);
            if (getManagedContext() != null) {
                obj = getManagedContext().initialize(obj);
            }
            return (T) obj;
        } catch (Throwable e) {
            throw handleException(e);
        } finally {
            ClassLocator.onFinishDeserialization();
            pool.returnInputBuffer(in);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T toObject(Object object, Class clazz) {
        if (!(object instanceof Data)) {
            return (T) object;
        }

        Data data = (Data) object;
        if (isNullData(data)) {
            return null;
        }

        BufferPool pool = delegate.bufferPoolThreadLocal.get();
        BufferObjectDataInput in = pool.takeInputBuffer(data);
        try {
            ClassLocator.onStartDeserialization();
            int typeId = data.getType();
            SerializerAdapter serializer = serializerFor(typeId);
            Object obj = serializer.read(in, clazz);
            if (getManagedContext() != null) {
                obj = getManagedContext().initialize(obj);
            }
            return (T) obj;
        } catch (Throwable e) {
            throw handleException(e);
        } finally {
            ClassLocator.onFinishDeserialization();
            pool.returnInputBuffer(in);
        }
    }

    @Override
    public void writeObject(ObjectDataOutput out, Object object) {
        if (object instanceof Data) {
            throw new HazelcastSerializationException("Cannot write a Data instance, use writeData() instead");
        }
        try {
            SerializerAdapter serializer = serializerFor(object);
            out.writeInt(serializer.getTypeId());
            serializer.write(out, object);
        } catch (Throwable e) {
            throw handleSerializeException(object, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readObject(ObjectDataInput in) {
        try {
            int typeId = in.readInt();
            SerializerAdapter serializer = serializerFor(typeId);
            Object object = serializer.read(in);
            if (getManagedContext() != null) {
                object = getManagedContext().initialize(object);
            }
            return (T) object;
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readObject(ObjectDataInput in, Class clazz) {
        try {
            int typeId = in.readInt();
            SerializerAdapter serializer = serializerFor(typeId);
            Object object = serializer.read(in, clazz);
            if (getManagedContext() != null) {
                object = getManagedContext().initialize(object);
            }
            return (T) object;
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    private SerializerAdapter serializerFor(Object object) {
        Class<?> clazz = object.getClass();
        SerializerAdapter serializer = serializersByClass.get(clazz);
        if (serializer == null) {
            serializer = delegate.serializerFor(object);
            if (serializer == null) {
                throw active ?
                        new HazelcastSerializationException("There is no suitable serializer for " + clazz) :
                        new HazelcastInstanceNotActiveException();
            }
        }
        return serializer;
    }

    private SerializerAdapter serializerFor(int typeId) {
        SerializerAdapter serializer = serializersById.get(typeId);
        if (serializer == null) {
            serializer = delegate.serializerFor(typeId);
            if (serializer == null) {
                throw active ?
                        newHazelcastSerializationException(typeId) :
                        new HazelcastInstanceNotActiveException();
            }
        }
        return serializer;
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(byte[] data) {
        return delegate.inputOutputFactory.createInput(data, this);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(byte[] data, int offset) {
        return delegate.inputOutputFactory.createInput(data, offset, this);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(Data data) {
        return delegate.inputOutputFactory.createInput(data, this);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput(int size) {
        return delegate.inputOutputFactory.createOutput(size, this);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput() {
        return createObjectDataOutput(DEFAULT_OUTPUT_BUFFER_SIZE);
    }

    @Override
    public PortableReader createPortableReader(Data data) throws IOException {
        return delegate.createPortableReader(data);
    }

    @Override
    public PortableContext getPortableContext() {
        return delegate.getPortableContext();
    }

    @Override
    public byte getVersion() {
        return delegate.getVersion();
    }

    @Override
    public ByteOrder getByteOrder() {
        return delegate.getByteOrder();
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public ManagedContext getManagedContext() {
        return delegate.getManagedContext();
    }

    @Override
    public void disposeData(Data data) {
        delegate.disposeData(data);
    }

    @Override
    public void dispose() {
        active = false;
        for (SerializerAdapter serializer : serializersByClass.values()) {
            serializer.destroy();
        }
    }

    public static InternalSerializationService from(SerializationService serializationService,
                                                    Map<String, String> serializerConfigs) {
        ClassLoader classLoader = currentThread().getContextClassLoader();
        Map<Class<?>, ? extends Serializer> serializers =
                serializerConfigs.entrySet()
                                 .stream()
                                 .collect(toMap(
                                         entry -> ReflectionUtils.loadClass(classLoader, entry.getKey()),
                                         entry -> ReflectionUtils.newInstance(classLoader, entry.getValue())
                                 ));
        return new JetSerializationService(serializers, (AbstractSerializationService) serializationService);
    }

    private static HazelcastSerializationException newHazelcastSerializationException(int typeId) {
        return new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId + ". "
                + "This exception is likely caused by differences in the serialization configuration between members "
                + "or between clients and members.");
    }
}
