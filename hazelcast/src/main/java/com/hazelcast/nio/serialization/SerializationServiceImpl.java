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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.DynamicByteBuffer;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ConstantSerializers.BooleanSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.ByteSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.CharArraySerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.CharSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.DoubleArraySerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.DoubleSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.FloatArraySerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.FloatSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.IntegerArraySerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.IntegerSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.LongArraySerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.LongSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.ShortArraySerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.ShortSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.StringSerializer;
import com.hazelcast.nio.serialization.ConstantSerializers.TheByteArraySerializer;
import com.hazelcast.nio.serialization.DefaultSerializers.BigDecimalSerializer;
import com.hazelcast.nio.serialization.DefaultSerializers.BigIntegerSerializer;
import com.hazelcast.nio.serialization.DefaultSerializers.ClassSerializer;
import com.hazelcast.nio.serialization.DefaultSerializers.DateSerializer;
import com.hazelcast.nio.serialization.DefaultSerializers.EnumSerializer;
import com.hazelcast.nio.serialization.DefaultSerializers.Externalizer;
import com.hazelcast.nio.serialization.DefaultSerializers.ObjectSerializer;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class SerializationServiceImpl implements SerializationService {

    protected static final PartitioningStrategy EMPTY_PARTITIONING_STRATEGY = new PartitioningStrategy() {
        public Object getPartitionKey(Object key) {
            return null;
        }
    };

    private static final int CONSTANT_SERIALIZERS_SIZE = SerializationConstants.CONSTANT_SERIALIZERS_LENGTH;

    protected final ManagedContext managedContext;
    protected final PortableContext portableContext;
    protected final InputOutputFactory inputOutputFactory;
    protected final PartitioningStrategy globalPartitioningStrategy;

    private final IdentityHashMap<Class, SerializerAdapter> constantTypesMap
            = new IdentityHashMap<Class, SerializerAdapter>(CONSTANT_SERIALIZERS_SIZE);
    private final SerializerAdapter[] constantTypeIds = new SerializerAdapter[CONSTANT_SERIALIZERS_SIZE];

    private final ConcurrentMap<Class, SerializerAdapter> typeMap = new ConcurrentHashMap<Class, SerializerAdapter>();
    private final ConcurrentMap<Integer, SerializerAdapter> idMap = new ConcurrentHashMap<Integer, SerializerAdapter>();
    private final AtomicReference<SerializerAdapter> global = new AtomicReference<SerializerAdapter>();
    private final ThreadLocalOutputCache dataOutputQueue;
    private final PortableSerializer portableSerializer;
    private final SerializerAdapter dataSerializerAdapter;
    private final SerializerAdapter portableSerializerAdapter;
    private final ClassLoader classLoader;
    private final int outputBufferSize;

    private volatile boolean active = true;

    SerializationServiceImpl(InputOutputFactory inputOutputFactory, int version, ClassLoader classLoader,
                             Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                             Map<Integer, ? extends PortableFactory> portableFactories,
                             Collection<ClassDefinition> classDefinitions, boolean checkClassDefErrors,
                             ManagedContext managedContext, PartitioningStrategy partitionStrategy,
                             int initialOutputBufferSize, boolean enableCompression, boolean enableSharedObject) {

        this.inputOutputFactory = inputOutputFactory;
        this.classLoader = classLoader;
        this.managedContext = managedContext;
        this.globalPartitioningStrategy = partitionStrategy;
        this.outputBufferSize = initialOutputBufferSize;

        dataOutputQueue = new ThreadLocalOutputCache(this);

        PortableHookLoader loader = new PortableHookLoader(portableFactories, classLoader);
        portableContext = new PortableContextImpl(this, version);
        for (ClassDefinition cd : loader.getDefinitions()) {
            portableContext.registerClassDefinition(cd);
        }

        dataSerializerAdapter = createSerializerAdapter(new DataSerializer(dataSerializableFactories, classLoader));
        portableSerializer = new PortableSerializer(portableContext, loader.getFactories());
        portableSerializerAdapter = createSerializerAdapter(portableSerializer);

        registerConstantSerializers();
        registerJvmTypeSerializers(enableCompression, enableSharedObject);
        registerClassDefinitions(classDefinitions, checkClassDefErrors);
    }

    private void registerJvmTypeSerializers(boolean enableCompression, boolean enableSharedObject) {
        safeRegister(Date.class, new DateSerializer());
        safeRegister(BigInteger.class, new BigIntegerSerializer());
        safeRegister(BigDecimal.class, new BigDecimalSerializer());
        safeRegister(Externalizable.class, new Externalizer(enableCompression));
        safeRegister(Serializable.class, new ObjectSerializer(enableSharedObject, enableCompression));
        safeRegister(Class.class, new ClassSerializer());
        safeRegister(Enum.class, new EnumSerializer());
    }

    private void registerConstantSerializers() {
        registerConstant(DataSerializable.class, dataSerializerAdapter);
        registerConstant(Portable.class, portableSerializerAdapter);
        registerConstant(Byte.class, new ByteSerializer());
        registerConstant(Boolean.class, new BooleanSerializer());
        registerConstant(Character.class, new CharSerializer());
        registerConstant(Short.class, new ShortSerializer());
        registerConstant(Integer.class, new IntegerSerializer());
        registerConstant(Long.class, new LongSerializer());
        registerConstant(Float.class, new FloatSerializer());
        registerConstant(Double.class, new DoubleSerializer());
        registerConstant(byte[].class, new TheByteArraySerializer());
        registerConstant(char[].class, new CharArraySerializer());
        registerConstant(short[].class, new ShortArraySerializer());
        registerConstant(int[].class, new IntegerArraySerializer());
        registerConstant(long[].class, new LongArraySerializer());
        registerConstant(float[].class, new FloatArraySerializer());
        registerConstant(double[].class, new DoubleArraySerializer());
        registerConstant(String.class, new StringSerializer());
    }

    private void registerClassDefinitions(Collection<ClassDefinition> classDefinitions, boolean checkClassDefErrors) {
        final Map<Integer, ClassDefinition> classDefMap = new HashMap<Integer, ClassDefinition>(classDefinitions.size());
        for (ClassDefinition cd : classDefinitions) {
            if (classDefMap.containsKey(cd.getClassId())) {
                throw new HazelcastSerializationException("Duplicate registration found for class-id["
                        + cd.getClassId() + "]!");
            }
            classDefMap.put(cd.getClassId(), cd);
        }
        for (ClassDefinition classDefinition : classDefinitions) {
            registerClassDefinition(classDefinition, classDefMap, checkClassDefErrors);
        }
    }

    private void registerClassDefinition(ClassDefinition cd, Map<Integer, ClassDefinition> classDefMap,
            boolean checkClassDefErrors) {
        for (int i = 0; i < cd.getFieldCount(); i++) {
            FieldDefinition fd = cd.getField(i);
            if (fd.getType() == FieldType.PORTABLE || fd.getType() == FieldType.PORTABLE_ARRAY) {
                int classId = fd.getClassId();
                ClassDefinition nestedCd = classDefMap.get(classId);
                if (nestedCd != null) {
                    registerClassDefinition(nestedCd, classDefMap, checkClassDefErrors);
                    portableContext.registerClassDefinition(nestedCd);
                } else if (checkClassDefErrors) {
                    throw new HazelcastSerializationException("Could not find registered ClassDefinition for class-id: "
                            + classId);
                }
            }
        }
        portableContext.registerClassDefinition(cd);
    }

    public final Data toData(final Object obj) {
        return toData(obj, globalPartitioningStrategy);
    }

    public final Data toData(Object obj, PartitioningStrategy strategy) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return (Data) obj;
        }
        int partitionHash = calculatePartitionHash(obj, strategy);
        try {
            final SerializerAdapter serializer = serializerFor(obj.getClass());
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable serializer for " + obj.getClass());
                }
                throw new HazelcastInstanceNotActiveException();
            }
            return serializer.toData(obj, partitionHash);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    @SuppressWarnings("unchecked")
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

    public final <T> T toObject(final Object object) {
        if (!(object instanceof Data)) {
            return (T) object;
        }

        Data data = (Data) object;
        if (data.dataSize() == 0 && data.getType() == SerializationConstants.CONSTANT_TYPE_NULL) {
            return null;
        }
        try {
            final int typeId = data.getType();
            final SerializerAdapter serializer = serializerFor(typeId);
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId);
                }
                throw new HazelcastInstanceNotActiveException();
            }
            Object obj = serializer.toObject(data);
            if (managedContext != null) {
                obj = managedContext.initialize(obj);
            }
            return (T) obj;
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    public final void writeObject(final ObjectDataOutput out, final Object obj) {
        if (obj instanceof Data) {
            throw new HazelcastSerializationException("Cannot write a Data instance! "
                    + "Use #writeData(ObjectDataOutput out, Data data) instead.");
        }
        final boolean isNull = obj == null;
        try {
            out.writeBoolean(isNull);
            if (isNull) {
                return;
            }
            final SerializerAdapter serializer = serializerFor(obj.getClass());
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable serializer for " + obj.getClass());
                }
                throw new HazelcastInstanceNotActiveException();
            }
            out.writeInt(serializer.getTypeId());
            serializer.write(out, obj);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    public final <T> T readObject(final ObjectDataInput in) {
        try {
            final boolean isNull = in.readBoolean();
            if (isNull) {
                return null;
            }
            final int typeId = in.readInt();
            final SerializerAdapter serializer = serializerFor(typeId);
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId);
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
    public final void writeData(ObjectDataOutput out, Data data) {
        try {
            boolean isNull = data == null;
            out.writeBoolean(isNull);
            if (isNull) {
                return;
            }
            out.writeInt(data.getType());
            out.writeInt(data.hasPartitionHash() ? data.getPartitionHash() : 0);
            writePortableHeader(out, data);

            int size = data.dataSize();
            out.writeInt(size);
            if (size > 0) {
                writeDataInternal(out, data);
            }
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    protected final void writePortableHeader(ObjectDataOutput out, Data data) throws IOException {
        if (data.headerSize() == 0) {
            out.writeInt(0);
        } else {
            if (!(out instanceof PortableDataOutput)) {
                throw new HazelcastSerializationException("PortableDataOutput is required to be able "
                        + "to write Portable header.");
            }

            byte[] header = data.getHeader();
            PortableDataOutput output = (PortableDataOutput) out;
            DynamicByteBuffer headerBuffer = output.getHeaderBuffer();
            out.writeInt(header.length);
            out.writeInt(headerBuffer.position());
            headerBuffer.put(header);
        }
    }

    protected void writeDataInternal(ObjectDataOutput out, Data data) throws IOException {
        out.write(data.getData());
    }

    @Override
    public final Data readData(ObjectDataInput in) {
        try {
            boolean isNull = in.readBoolean();
            if (isNull) {
                return null;
            }

            int typeId = in.readInt();
            int partitionHash = in.readInt();
            byte[] header = readPortableHeader(in);

            int dataSize = in.readInt();
            byte[] data = null;
            if (dataSize > 0) {
                data = new byte[dataSize];
                in.readFully(data);
            }
            return new DefaultData(typeId, data, partitionHash, header);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    protected final byte[] readPortableHeader(ObjectDataInput in) throws IOException {
        byte[] header = null;
        int len = in.readInt();
        if (len > 0) {
            if (!(in instanceof PortableDataInput)) {
                throw new HazelcastSerializationException("PortableDataInput is required to be able "
                        + "to read Portable header.");
            }
            PortableDataInput input = (PortableDataInput) in;
            ByteBuffer headerBuffer = input.getHeaderBuffer();
            int pos = in.readInt();
            headerBuffer.position(pos);
            header = new byte[len];
            headerBuffer.get(header);
        }
        return header;
    }

    public void disposeData(Data data) {
    }

    protected RuntimeException handleException(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            throw (Error) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException(e);
    }


    public final BufferObjectDataOutput pop() {
        return dataOutputQueue.pop();
    }

    public final void push(BufferObjectDataOutput out) {
        dataOutputQueue.push(out);
    }

    public final BufferObjectDataInput createObjectDataInput(byte[] data) {
        return inputOutputFactory.createInput(data, this);
    }

    public final BufferObjectDataInput createObjectDataInput(Data data) {
        return inputOutputFactory.createInput(data, this);
    }

    public final BufferObjectDataOutput createObjectDataOutput(int size) {
        return inputOutputFactory.createOutput(size, this);
    }

    public final ObjectDataOutputStream createObjectDataOutputStream(OutputStream out) {
        return new ObjectDataOutputStream(out, this);
    }

    public final ObjectDataInputStream createObjectDataInputStream(InputStream in) {
        return new ObjectDataInputStream(in, this);
    }

    public final void register(Class type, Serializer serializer) {
        if (type == null) {
            throw new IllegalArgumentException("Class type information is required!");
        }
        if (serializer.getTypeId() <= 0) {
            throw new IllegalArgumentException("Type id must be positive! Current: "
                    + serializer.getTypeId() + ", Serializer: " + serializer);
        }
        safeRegister(type, createSerializerAdapter(serializer));
    }

    public final void registerGlobal(final Serializer serializer) {
        SerializerAdapter adapter = createSerializerAdapter(serializer);
        if (!global.compareAndSet(null, adapter)) {
            throw new IllegalStateException("Global serializer is already registered!");
        }
        SerializerAdapter current = idMap.putIfAbsent(serializer.getTypeId(), adapter);
        if (current != null && current.getImpl().getClass() != adapter.getImpl().getClass()) {
            global.compareAndSet(adapter, null);
            throw new IllegalStateException("Serializer [" + current.getImpl()
                    + "] has been already registered for type-id: " + serializer.getTypeId());
        }
    }

    protected SerializerAdapter createSerializerAdapter(Serializer serializer) {
        final SerializerAdapter s;
        if (serializer instanceof StreamSerializer) {
            s = new StreamSerializerAdapter(this, (StreamSerializer) serializer);
        } else if (serializer instanceof ByteArraySerializer) {
            s = new ByteArraySerializerAdapter((ByteArraySerializer) serializer);
        } else {
            throw new IllegalArgumentException("Serializer must be instance of either "
                    + "StreamSerializer or ByteArraySerializer!");
        }
        return s;
    }

    protected final SerializerAdapter serializerFor(final Class type) {
        if (DataSerializable.class.isAssignableFrom(type)) {
            return dataSerializerAdapter;
        } else if (Portable.class.isAssignableFrom(type)) {
            return portableSerializerAdapter;
        } else {
            final SerializerAdapter serializer = constantTypesMap.get(type);
            if (serializer != null) {
                return serializer;
            }
        }
        SerializerAdapter serializer = lookupSerializer(type);
        return serializer;
    }

    protected final SerializerAdapter lookupSerializer(Class type) {
        SerializerAdapter serializer = typeMap.get(type);
        if (serializer == null) {
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
                // look for interfaces
                for (Class typeInterface : interfaces) {
                    serializer = registerFromSuperType(type, typeInterface);
                    if (serializer != null) {
                        break;
                    }
                }
            }

            if (serializer == null) {
                serializer = global.get();
                if (serializer != null) {
                    safeRegister(type, serializer);
                }
            }
        }
        return serializer;
    }

    private static void getInterfaces(Class clazz, Set<Class> interfaces) {
        final Class[] classes = clazz.getInterfaces();
        if (classes.length > 0) {
            Collections.addAll(interfaces, classes);
            for (Class cl : classes) {
                getInterfaces(cl, interfaces);
            }
        }
    }

    private SerializerAdapter registerFromSuperType(final Class type, final Class superType) {
        final SerializerAdapter serializer = typeMap.get(superType);
        if (serializer != null) {
            safeRegister(type, serializer);
        }
        return serializer;
    }

    private void registerConstant(Class type, Serializer serializer) {
        registerConstant(type, createSerializerAdapter(serializer));
    }

    private void registerConstant(Class type, SerializerAdapter serializer) {
        constantTypesMap.put(type, serializer);
        constantTypeIds[indexForDefaultType(serializer.getTypeId())] = serializer;
    }

    void safeRegister(final Class type, final Serializer serializer) {
        safeRegister(type, createSerializerAdapter(serializer));
    }

    private void safeRegister(final Class type, final SerializerAdapter serializer) {
        if (constantTypesMap.containsKey(type)) {
            throw new IllegalArgumentException("[" + type + "] serializer cannot be overridden!");
        }
        SerializerAdapter current = typeMap.putIfAbsent(type, serializer);
        if (current != null && current.getImpl().getClass() != serializer.getImpl().getClass()) {
            throw new IllegalStateException("Serializer[" + current.getImpl()
                    + "] has been already registered for type: " + type);
        }
        current = idMap.putIfAbsent(serializer.getTypeId(), serializer);
        if (current != null && current.getImpl().getClass() != serializer.getImpl().getClass()) {
            throw new IllegalStateException("Serializer [" + current.getImpl() + "] has been already registered for type-id: "
                    + serializer.getTypeId());
        }
    }

    protected final SerializerAdapter serializerFor(final int typeId) {
        if (typeId < 0) {
            final int index = indexForDefaultType(typeId);
            if (index < CONSTANT_SERIALIZERS_SIZE) {
                return constantTypeIds[index];
            }
        }
        return idMap.get(typeId);
    }

    private int indexForDefaultType(final int typeId) {
        return -typeId - 1;
    }

    public PortableContext getPortableContext() {
        return portableContext;
    }

    public final PortableReader createPortableReader(Data data) throws IOException {
        if (!data.isPortable()) {
            throw new IllegalArgumentException();
        }
        BufferObjectDataInput in = createObjectDataInput(data);
        return portableSerializer.createReader(in);

    }

    public void destroy() {
        active = false;
        for (SerializerAdapter serializer : typeMap.values()) {
            serializer.destroy();
        }
        typeMap.clear();
        idMap.clear();
        global.set(null);
        constantTypesMap.clear();
        dataOutputQueue.clear();
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

    public boolean isActive() {
        return active;
    }

    private static final class ThreadLocalOutputCache {
        static final float LOAD_FACTOR = 0.91f;
        final ConcurrentMap<Thread, Queue<BufferObjectDataOutput>> map;
        final SerializationServiceImpl serializationService;
        final int bufferSize;

        private ThreadLocalOutputCache(SerializationServiceImpl serializationService) {
            this.serializationService = serializationService;
            bufferSize = serializationService.outputBufferSize;
            int initialCapacity = Runtime.getRuntime().availableProcessors();
            map = new ConcurrentReferenceHashMap<Thread, Queue<BufferObjectDataOutput>>(initialCapacity, LOAD_FACTOR, 1);
        }

        BufferObjectDataOutput pop() {
            Thread t = Thread.currentThread();
            Queue<BufferObjectDataOutput> outputQueue = map.get(t);
            if (outputQueue == null) {
                outputQueue = new ArrayDeque<BufferObjectDataOutput>(3);
                map.put(t, outputQueue);
            }
            BufferObjectDataOutput out = outputQueue.poll();
            if (out == null) {
                out = serializationService.createObjectDataOutput(bufferSize);
            }
            return out;
        }

        void push(BufferObjectDataOutput out) {
            if (out != null) {
                out.clear();
                Queue<BufferObjectDataOutput> outputQueue = map.get(Thread.currentThread());
                if (outputQueue == null || !outputQueue.offer(out)) {
                    IOUtil.closeResource(out);
                }
            }
        }

        void clear() {
            map.clear();
        }
    }
}
