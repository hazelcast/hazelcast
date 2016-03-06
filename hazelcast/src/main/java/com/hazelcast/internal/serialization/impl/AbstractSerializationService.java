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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.ByteSerializer;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.DoubleSerializer;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.FloatSerializer;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.IntegerSerializer;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.LongSerializer;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolFactory;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolThreadLocal;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.Serializer;

import java.io.Externalizable;
import java.io.Serializable;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.serialization.impl.AbstractSerializationService.Null.NULL;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_SERIALIZERS_LENGTH;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.EMPTY_PARTITIONING_STRATEGY;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.getInterfaces;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.indexForDefaultType;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.isNullData;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.nio.ByteOrder.BIG_ENDIAN;

public abstract class AbstractSerializationService implements SerializationService {

    protected final ManagedContext managedContext;
    protected final InputOutputFactory inputOutputFactory;
    protected final PartitioningStrategy globalPartitioningStrategy;
    protected final BufferPoolThreadLocal bufferPoolThreadLocal;

    protected SerializerAdapter dataSerializerAdapter;
    protected SerializerAdapter portableSerializerAdapter;
    protected final SerializerAdapter nullSerializerAdapter;
    protected SerializerAdapter javaSerializerAdapter;
    protected SerializerAdapter javaExternalizableAdapter;

    private final IdentityHashMap<Class, SerializerAdapter> constantSerializersByType
            = new IdentityHashMap<Class, SerializerAdapter>(CONSTANT_SERIALIZERS_LENGTH);
    private final SerializerAdapter[] constantTypeIds = new SerializerAdapter[CONSTANT_SERIALIZERS_LENGTH];
    private final ConcurrentMap<Class, SerializerAdapter> serializersByType = new ConcurrentHashMap<Class, SerializerAdapter>(1000);
    private final ConcurrentMap<Integer, SerializerAdapter> serializersById = new ConcurrentHashMap<Integer, SerializerAdapter>();

    private final AtomicReference<SerializerAdapter> global = new AtomicReference<SerializerAdapter>();

    private final AtomicReferenceArray<SerializerAdapter> serializerArray = new AtomicReferenceArray<SerializerAdapter>(
            Short.MAX_VALUE * 2);

    //Global serializer may override Java Serialization or not
    private boolean overrideJavaSerialization;

    private final ClassLoader classLoader;
    private final int outputBufferSize;
    private volatile boolean active = true;
    private final byte version;

    private ILogger logger = Logger.getLogger(SerializationService.class);

    AbstractSerializationService(InputOutputFactory inputOutputFactory, byte version, ClassLoader classLoader,
                                 ManagedContext managedContext, PartitioningStrategy globalPartitionStrategy,
                                 int initialOutputBufferSize,
                                 BufferPoolFactory bufferPoolFactory) {
        this.inputOutputFactory = inputOutputFactory;
        this.version = version;
        this.classLoader = classLoader;
        this.managedContext = managedContext;
        this.globalPartitioningStrategy = globalPartitionStrategy;
        this.outputBufferSize = initialOutputBufferSize;
        this.bufferPoolThreadLocal = new BufferPoolThreadLocal(this, bufferPoolFactory);
        this.nullSerializerAdapter = new SerializerAdapter(new ConstantSerializers.NullSerializer());
    }

    //region Serialization Service
    @Override
    public final Data toData(Object obj) {
        return toData(obj, globalPartitioningStrategy);
    }

    @Override
    public final Data toData(Object obj, PartitioningStrategy strategy) {
//        if (obj == null || obj.getClass() == HeapData.class) {
//            return (HeapData) obj;
//        }

        byte[] bytes = toBytes(obj, strategy);
        return new HeapData(bytes);
    }

    @Override
    public byte[] toBytes(Object obj) {
        return toBytes(obj, globalPartitioningStrategy);
    }

    @Override
    public byte[] toBytes(Object obj, PartitioningStrategy strategy) {
        checkNotNull(obj);

        BufferPool pool = bufferPoolThreadLocal.get();
        BufferObjectDataOutput out = pool.takeOutputBuffer();
        try {
            SerializerAdapter serializer = serializerFor(obj);
            int partitionHash = calculatePartitionHash(obj, strategy);
            out.writeInt(partitionHash, BIG_ENDIAN);
            out.writeInt(serializer.getTypeId(), BIG_ENDIAN);
            serializer.write(out, obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw handleException(e);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    public final <T> T toObject(final Object object) {
        if (object.getClass() != HeapData.class && object.getClass() != Packet.class) {
            return (T) object;
        }

        return toObject(object);
    }

    @Override
    public <T> T toObject(Data data) {
        if (isNullData(data)) {
            return null;
        }

        BufferPool pool = bufferPoolThreadLocal.get();
        BufferObjectDataInput in = pool.takeInputBuffer(data);
        try {
            int typeId = data.getType();
            SerializerAdapter serializer = serializerFor(typeId);
            if (serializer == null) {
                if (active) {
                    throw newHazelcastSerializationException(typeId);
                }
                throw new HazelcastInstanceNotActiveException();
            }

            Object obj = serializer.read(in);
            if (managedContext != null) {
                obj = managedContext.initialize(obj);
            }
            return (T) obj;
        } catch (Throwable e) {
            throw handleException(e);
        } finally {
            pool.returnInputBuffer(in);
        }
    }

    private HazelcastSerializationException newHazelcastSerializationException(int typeId) {
        return new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId + ". "
                + "This exception is likely to be caused by differences in the serialization configuration between members "
                + "or between clients and members.");
    }

    @Override
    public final void writeObject(final ObjectDataOutput out, final Object obj) {
        if (obj instanceof Data) {
            throw new HazelcastSerializationException(
                    "Cannot write a Data instance! " + "Use #writeData(ObjectDataOutput out, Data data) instead.");
        }
        try {
            SerializerAdapter serializer = serializerFor(obj);
            out.writeInt(serializer.getTypeId());
            serializer.write(out, obj);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    @Override
    public final <T> T readObject(final ObjectDataInput in) {
        try {
            final int typeId = in.readInt();
            final SerializerAdapter serializer = serializerFor(typeId);
            if (serializer == null) {
                if (active) {
                    throw newHazelcastSerializationException(typeId);
                }
                throw new HazelcastInstanceNotActiveException();
            }
            Object obj = serializer.read(in);
            if (managedContext != null) {
                obj = managedContext.initialize(obj);
            }
            return (T) obj;
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    @Override
    public void disposeData(Data data) {
    }

    @Override
    public final BufferObjectDataInput createObjectDataInput(byte[] data) {
        return inputOutputFactory.createInput(data, this);
    }

    @Override
    public final BufferObjectDataInput createObjectDataInput(Data data) {
        return inputOutputFactory.createInput(data, this);
    }

    @Override
    public final BufferObjectDataOutput createObjectDataOutput(int size) {
        return inputOutputFactory.createOutput(size, this);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput() {
        return inputOutputFactory.createOutput(outputBufferSize, this);
    }

    public final ClassLoader getClassLoader() {
        return classLoader;
    }

    public final ManagedContext getManagedContext() {
        return managedContext;
    }

    @Override
    public ByteOrder getByteOrder() {
        return inputOutputFactory.getByteOrder();
    }

    @Override
    public byte getVersion() {
        return version;
    }

    public void destroy() {
        active = false;
        for (SerializerAdapter serializer : serializersByType.values()) {
            serializer.destroy();
        }
        serializersByType.clear();

        for (SerializerAdapter serializer : constantSerializersByType.values()) {
            serializer.destroy();
        }
        constantSerializersByType.clear();

        for (int k = 0; k < serializerArray.length(); k++) {
            serializerArray.set(k, null);
        }

        serializersById.clear();
        global.set(null);
        bufferPoolThreadLocal.clear();
    }
    //endregion Serialization Service

    public final void register(Class type, Serializer serializer) {
        if (type == null) {
            throw new IllegalArgumentException("Class type information is required!");
        }
        if (serializer.getTypeId() <= 0) {
            throw new IllegalArgumentException(
                    "Type id must be positive! Current: " + serializer.getTypeId() + ", Serializer: " + serializer);
        }
        safeRegister(type, new SerializerAdapter(serializer));
    }

    public final void registerGlobal(final Serializer serializer) {
        registerGlobal(serializer, false);
    }

    public final void registerGlobal(final Serializer serializer, boolean overrideJavaSerialization) {
        SerializerAdapter adapter = new SerializerAdapter(serializer);
        if (!global.compareAndSet(null, adapter)) {
            throw new IllegalStateException("Global serializer is already registered!");
        }
        this.overrideJavaSerialization = overrideJavaSerialization;
        SerializerAdapter current = serializersById.putIfAbsent(serializer.getTypeId(), adapter);
        if (current != null && current.getImpl().getClass() != adapter.getImpl().getClass()) {
            global.compareAndSet(adapter, null);
            this.overrideJavaSerialization = false;
            throw new IllegalStateException(
                    "Serializer [" + current.getImpl() + "] has been already registered for type-id: " + serializer.getTypeId());
        }
    }

    protected final int calculatePartitionHash(Object obj, PartitioningStrategy strategy) {
        int partitionHash = 0;
        PartitioningStrategy partitioningStrategy = strategy == null ? globalPartitioningStrategy : strategy;
        if (partitioningStrategy != null) {
            Object pk = partitioningStrategy.getPartitionKey(obj);
            if (pk != null && pk != obj) {
                final Data partitionKey = toData(pk, EMPTY_PARTITIONING_STRATEGY);
                partitionHash = partitionKey == null ? 0 : partitionKey.getPartitionHash();
            }
        }
        return partitionHash;
    }

    protected final boolean safeRegister(final Class type, final Serializer serializer) {
        return safeRegister(type, new SerializerAdapter(serializer));
    }

    protected final boolean safeRegister(final Class type, final SerializerAdapter serializer) {
        if (constantSerializersByType.containsKey(type)) {
            throw new IllegalArgumentException("[" + type + "] serializer cannot be overridden!");
        }
        SerializerAdapter current = serializersByType.putIfAbsent(type, serializer);
        if (current != null && current.getImpl().getClass() != serializer.getImpl().getClass()) {
            throw new IllegalStateException(
                    "Serializer[" + current.getImpl() + "] has been already registered for type: " + type);
        }
        current = serializersById.putIfAbsent(serializer.getTypeId(), serializer);
        if (current != null && current.getImpl().getClass() != serializer.getImpl().getClass()) {
            throw new IllegalStateException(
                    "Serializer [" + current.getImpl() + "] has been already registered for type-id: " + serializer.getTypeId());
        }
        return current == null;
    }

    protected final void registerConstant(Class type, Serializer serializer) {
        SerializerAdapter streamSerializer = new SerializerAdapter(serializer);
        registerConstant(type, streamSerializer);

        serializerArray.set(serializer.getTypeId() + Short.MAX_VALUE, streamSerializer);
    }

    protected final void registerConstant(Class type, SerializerAdapter serializer) {
        constantSerializersByType.put(type, serializer);
        constantTypeIds[indexForDefaultType(serializer.getTypeId())] = serializer;
    }

    private SerializerAdapter registerFromSuperType(final Class type, final Class superType) {
        final SerializerAdapter serializer = serializersByType.get(superType);
        if (serializer != null) {
            safeRegister(type, serializer);
        }
        return serializer;
    }

    private SerializerAdapter serializerFor(int typeId) {
        int index = typeId + Short.MAX_VALUE;

        if (index >= 0 && index < serializerArray.length()) {
            SerializerAdapter serializer = serializerArray.get(index);
            if (serializer != null) {
                return serializer;
            }

            serializer = lookupSerializerById(typeId);
            serializerArray.compareAndSet(index, null, serializer);
            return serializerArray.get(index);
        } else {
            return lookupSerializerById(typeId);
        }
    }

    private SerializerAdapter lookupSerializerById(int typeId) {
        if (typeId <= 0) {
            final int index = indexForDefaultType(typeId);
            if (index < CONSTANT_SERIALIZERS_LENGTH) {
                return constantTypeIds[index];
            }
        }
        return serializersById.get(typeId);
    }

    SerializerAdapter longSerializerAdapter = new SerializerAdapter(new LongSerializer());
    SerializerAdapter intSerializerAdapter = new SerializerAdapter(new IntegerSerializer());
    SerializerAdapter doubleSerializerAdapter = new SerializerAdapter(new DoubleSerializer());
    SerializerAdapter floatSerializerAdapter = new SerializerAdapter(new FloatSerializer());
    SerializerAdapter byteSerializerAdapter = new SerializerAdapter(new ByteSerializer());

    protected final SerializerAdapter serializerFor(Object object) {
        object = object == null ? NULL : object;

        Class<?> type = object.getClass();
//        if (type == Long.class) {
//            return longSerializerAdapter;
//        } else if (type == Integer.class) {
//            return intSerializerAdapter;
//        } else if (type == Double.class) {
//            return doubleSerializerAdapter;
//        } else if (type == Float.class) {
//            return floatSerializerAdapter;
//        } else if (type == Byte.class) {
//            return byteSerializerAdapter;
//        }
//
        SerializerAdapter serializer = serializersByType.get(type);
        if (serializer == null) {
            serializer = lookupSerializer(type);
            SerializerAdapter found = serializersByType.putIfAbsent(type, serializer);
            if (found != null) {
                serializer = found;
            }
        }
        return serializer;
    }

    private SerializerAdapter lookupSerializer(Class type) {
    /*
        Searches for a serializer for the provided object
        Serializers will be  searched in this order;

        1-NULL serializer
        2-Default serializers, like primitives, arrays, String and some Java types
        3-Custom registered types by user
        4-JDK serialization ( Serializable and Externalizable ) if a global serializer with Java serialization not registered
        5-Global serializer if registered by user
     */

        //1-NULL serializer
        if (type == Null.class) {
            return nullSerializerAdapter;
        }

        //2-Default serializers, Dataserializable, Portable, primitives, arrays, String and some helper Java types(BigInteger etc)
        SerializerAdapter serializer = lookupDefaultSerializer(type);

        //3-Custom registered types by user
        if (serializer == null) {
            serializer = lookupCustomSerializer(type);
        }

        //4-JDK serialization ( Serializable and Externalizable )
        if (serializer == null && !overrideJavaSerialization) {
            serializer = lookupJavaSerializer(type);
        }

        //5-Global serializer if registered by user
        if (serializer == null) {
            serializer = lookupGlobalSerializer(type);
        }

        if (serializer == null) {
            if (active) {
                throw new HazelcastSerializationException("There is no suitable serializer for " + type);
            }
            throw new HazelcastInstanceNotActiveException();
        }
        return serializer;
    }

    private SerializerAdapter lookupDefaultSerializer(Class type) {
        if (DataSerializable.class.isAssignableFrom(type)) {
            return dataSerializerAdapter;
        }
        if (Portable.class.isAssignableFrom(type)) {
            return portableSerializerAdapter;
        }
        return constantSerializersByType.get(type);
    }

    private SerializerAdapter lookupCustomSerializer(Class type) {
        SerializerAdapter serializer = serializersByType.get(type);
        if (serializer != null) {
            return serializer;
        }
        // look for super classes
        Class typeSuperclass = type.getSuperclass();
        final Set<Class> interfaces = new LinkedHashSet<Class>(5);
        getInterfaces(type, interfaces);
        while (typeSuperclass != null) {
            serializer = registerFromSuperType(type, typeSuperclass);
            if (serializer != null) {
                break;
            }
            getInterfaces(typeSuperclass, interfaces);
            typeSuperclass = typeSuperclass.getSuperclass();
        }
        if (serializer == null) {
            //remove ignore Interfaces:
            interfaces.remove(Serializable.class);
            interfaces.remove(Externalizable.class);
            // look for interfaces
            for (Class typeInterface : interfaces) {
                serializer = registerFromSuperType(type, typeInterface);
                if (serializer != null) {
                    break;
                }
            }
        }
        return serializer;
    }

    private SerializerAdapter lookupGlobalSerializer(Class type) {
        SerializerAdapter serializer = global.get();
        if (serializer != null) {
            logger.fine("Registering global serializer for : " + type.getName());
            safeRegister(type, serializer);
        }
        return serializer;
    }

    private SerializerAdapter lookupJavaSerializer(Class type) {
        if (Externalizable.class.isAssignableFrom(type)) {
            if (safeRegister(type, javaExternalizableAdapter) && !Throwable.class.isAssignableFrom(type)) {
                logger.info("Performance Hint: Serialization service will use java.io.Externalizable for : " + type.getName()
                        + " . Please consider using a faster serialization option such as DataSerializable. ");
            }
            return javaExternalizableAdapter;
        }

        if (Serializable.class.isAssignableFrom(type)) {
            if (safeRegister(type, javaSerializerAdapter) && !Throwable.class.isAssignableFrom(type)) {
                logger.info("Performance Hint: Serialization service will use java.io.Serializable for : " + type.getName()
                        + " . Please consider using a faster serialization option such as DataSerializable. ");
            }
            return javaSerializerAdapter;
        }
        return null;
    }

    static class Null {
        static final Null NULL = new Null();
    }
}
