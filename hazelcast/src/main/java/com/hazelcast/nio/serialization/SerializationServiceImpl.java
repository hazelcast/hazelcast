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

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public final class SerializationServiceImpl implements SerializationService {

    private static final int CONSTANT_SERIALIZERS_SIZE = SerializationConstants.CONSTANT_SERIALIZERS_LENGTH;

    private static final PartitioningStrategy EMPTY_PARTITIONING_STRATEGY = new PartitioningStrategy() {
        public Object getPartitionKey(Object key) {
            return null;
        }
    };

    private final IdentityHashMap<Class, SerializerAdapter> constantTypesMap
            = new IdentityHashMap<Class, SerializerAdapter>(CONSTANT_SERIALIZERS_SIZE);
    private final SerializerAdapter[] constantTypeIds = new SerializerAdapter[CONSTANT_SERIALIZERS_SIZE];

    private final ConcurrentMap<Class, SerializerAdapter> typeMap = new ConcurrentHashMap<Class, SerializerAdapter>();
    private final ConcurrentMap<Integer, SerializerAdapter> idMap = new ConcurrentHashMap<Integer, SerializerAdapter>();
    private final AtomicReference<SerializerAdapter> global = new AtomicReference<SerializerAdapter>();

    private final InputOutputFactory inputOutputFactory;
    private final Queue<BufferObjectDataOutput> outputPool = new ConcurrentLinkedQueue<BufferObjectDataOutput>();
    private final PortableSerializer portableSerializer;
    private final SerializerAdapter dataSerializerAdapter;
    private final SerializerAdapter portableSerializerAdapter;
    private final ClassLoader classLoader;
    private final ManagedContext managedContext;
    private final PortableContextImpl serializationContext;
    private final PartitioningStrategy globalPartitioningStrategy;
    private final int outputBufferSize;

    private volatile boolean active = true;

    SerializationServiceImpl(InputOutputFactory inputOutputFactory, int version, ClassLoader classLoader,
                             Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                             Map<Integer, ? extends PortableFactory> portableFactories,
                             Collection<ClassDefinition> classDefinitions, boolean checkClassDefErrors,
                             ManagedContext managedContext, PartitioningStrategy partitionStrategy,
                             int initialOutputBufferSize,
                             boolean enableCompression, boolean enableSharedObject) {

        this.inputOutputFactory = inputOutputFactory;
        this.classLoader = classLoader;
        this.managedContext = managedContext;
        this.globalPartitioningStrategy = partitionStrategy;
        this.outputBufferSize = initialOutputBufferSize;

        PortableHookLoader loader = new PortableHookLoader(portableFactories, classLoader);
        serializationContext = new PortableContextImpl(this, loader.getFactories().keySet(), version);
        for (ClassDefinition cd : loader.getDefinitions()) {
            serializationContext.registerClassDefinition(cd);
        }

        dataSerializerAdapter = new StreamSerializerAdapter(this, new DataSerializer(dataSerializableFactories, classLoader));
        portableSerializer = new PortableSerializer(serializationContext, loader.getFactories());
        portableSerializerAdapter = new StreamSerializerAdapter(this, portableSerializer);

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

    private void registerClassDefinition(ClassDefinition cd, Map<Integer,
            ClassDefinition> classDefMap, boolean checkClassDefErrors) {
        for (int i = 0; i < cd.getFieldCount(); i++) {
            FieldDefinition fd = cd.getField(i);
            if (fd.getType() == FieldType.PORTABLE || fd.getType() == FieldType.PORTABLE_ARRAY) {
                int classId = fd.getClassId();
                ClassDefinition nestedCd = classDefMap.get(classId);
                if (nestedCd != null) {
                    ((ClassDefinitionImpl) cd).addClassDef(nestedCd);
                    registerClassDefinition(nestedCd, classDefMap, checkClassDefErrors);
                    serializationContext.registerClassDefinition(nestedCd);
                } else if (checkClassDefErrors) {
                    throw new HazelcastSerializationException("Could not find registered ClassDefinition for class-id: "
                            + classId);
                }
            }
        }
        serializationContext.registerClassDefinition(cd);
    }

    public Data toData(final Object obj) {
        return toData(obj, globalPartitioningStrategy);
    }

    @SuppressWarnings("unchecked")
    public Data toData(Object obj, PartitioningStrategy strategy) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return (Data) obj;
        }
        try {
            final SerializerAdapter serializer = serializerFor(obj.getClass());
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable serializer for " + obj.getClass());
                }
                throw new HazelcastInstanceNotActiveException();
            }
            final byte[] bytes = serializer.write(obj);
            final Data data = new Data(serializer.getTypeId(), bytes);
            if (obj instanceof Portable) {
                final Portable portable = (Portable) obj;
                data.classDefinition = lookupClassDefinition(portable);
            }
            applyPartitionStrategy(obj, strategy, data);
            return data;
        } catch (Throwable e) {
            handleException(e);
        }
        return null;
    }

    private void applyPartitionStrategy(Object obj, PartitioningStrategy strategy, Data data) {
        if (strategy == null) {
            strategy = globalPartitioningStrategy;
        }
        if (strategy != null) {
            Object pk = strategy.getPartitionKey(obj);
            if (pk != null && pk != obj) {
                final Data partitionKey = toData(pk, EMPTY_PARTITIONING_STRATEGY);
                data.partitionHash = (partitionKey == null) ? -1 : partitionKey.getPartitionHash();
            }
        }
    }

    private ClassDefinition lookupClassDefinition(Portable portable) {
        final int version = PortableVersionHelper.getVersion(portable, serializationContext.getVersion());
        return serializationContext.lookup(portable.getFactoryId(), portable.getClassId(), version);
    }

    public <T> T toObject(final Object object) {
        if (!(object instanceof Data)) {
            return (T) object;
        }

        Data data = (Data) object;
        if (data.bufferSize() == 0 && data.isDataSerializable()) {
            return null;
        }
        try {
            final int typeId = data.type;
            Object obj = readObject(data, typeId);
            if (managedContext != null) {
                obj = managedContext.initialize(obj);
            }
            return (T) obj;
        } catch (Throwable e) {
            handleException(e);
        }
        return null;
    }

    private Object readObject(Data data, int typeId)
            throws IOException {
        final SerializerAdapter serializer = serializerFor(typeId);
        if (serializer == null) {
            if (active) {
                throw new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId);
            }
            throw new HazelcastInstanceNotActiveException();
        }
        if (typeId == SerializationConstants.CONSTANT_TYPE_PORTABLE) {
            serializationContext.registerClassDefinition(data.classDefinition);
        }
        return serializer.read(data);
    }

    public void writeObject(final ObjectDataOutput out, final Object obj) {
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
            if (obj instanceof Portable) {
                final Portable portable = (Portable) obj;
                ClassDefinition classDefinition = serializationContext.lookupOrRegisterClassDefinition(portable);
                classDefinition.writeData(out);
            }
            serializer.write(out, obj);
        } catch (Throwable e) {
            handleException(e);
        }
    }

    public Object readObject(final ObjectDataInput in) {
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
            if (typeId == SerializationConstants.CONSTANT_TYPE_PORTABLE && in instanceof PortableContextAwareInputStream) {
                ClassDefinition classDefinition = new ClassDefinitionImpl();
                classDefinition.readData(in);
                classDefinition = serializationContext.registerClassDefinition(classDefinition);
                PortableContextAwareInputStream ctxIn = (PortableContextAwareInputStream) in;
                ctxIn.setClassDefinition(classDefinition);
            }
            Object obj = serializer.read(in);
            if (managedContext != null) {
                obj = managedContext.initialize(obj);
            }
            return obj;
        } catch (Throwable e) {
            handleException(e);
        }
        return null;
    }

    private void handleException(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            return;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException(e);
    }

    BufferObjectDataOutput pop() {
        BufferObjectDataOutput out = outputPool.poll();
        if (out == null) {
            out = inputOutputFactory.createOutput(outputBufferSize, this);
        }
        return out;
    }

    void push(BufferObjectDataOutput out) {
        if (out != null) {
            out.clear();
            outputPool.offer(out);
        }
    }

    public BufferObjectDataInput createObjectDataInput(byte[] data) {
        return inputOutputFactory.createInput(data, this);
    }

    public BufferObjectDataInput createObjectDataInput(Data data) {
        return inputOutputFactory.createInput(data, this);
    }

    public BufferObjectDataOutput createObjectDataOutput(int size) {
        return inputOutputFactory.createOutput(size, this);
    }

    public ObjectDataOutputStream createObjectDataOutputStream(OutputStream out) {
        return new ObjectDataOutputStream(out, this);
    }

    public ObjectDataInputStream createObjectDataInputStream(InputStream in) {
        return new ObjectDataInputStream(in, this);
    }

    public ObjectDataOutputStream createObjectDataOutputStream(OutputStream out, ByteOrder order) {
        return new ObjectDataOutputStream(out, this, order);
    }

    public ObjectDataInputStream createObjectDataInputStream(InputStream in, ByteOrder order) {
        return new ObjectDataInputStream(in, this, order);
    }

    public void register(Class type, Serializer serializer) {
        if (type == null) {
            throw new IllegalArgumentException("Class type information is required!");
        }
        if (serializer.getTypeId() <= 0) {
            throw new IllegalArgumentException("Type id must be positive! Current: "
                    + serializer.getTypeId() + ", Serializer: " + serializer);
        }
        safeRegister(type, createSerializerAdapter(serializer));
    }

    public void registerGlobal(final Serializer serializer) {
        SerializerAdapter adapter = createSerializerAdapter(serializer);
        if (!global.compareAndSet(null, adapter)) {
            throw new IllegalStateException("Global serializer is already registered!");
        }
        SerializerAdapter current = idMap.putIfAbsent(serializer.getTypeId(), adapter);
        if (current != null && current.getImpl().getClass() != adapter.getImpl().getClass()) {
            global.compareAndSet(adapter, null);
            throw new IllegalStateException("Serializer [" + current.getImpl() + "] has been already registered for type-id: "
                    + serializer.getTypeId());
        }
    }

    private SerializerAdapter createSerializerAdapter(Serializer serializer) {
        final SerializerAdapter s;
        if (serializer instanceof StreamSerializer) {
            s = new StreamSerializerAdapter(this, (StreamSerializer) serializer);
        } else if (serializer instanceof ByteArraySerializer) {
            s = new ByteArraySerializerAdapter((ByteArraySerializer) serializer);
        } else {
            throw new IllegalArgumentException("Serializer must be instance of either StreamSerializer or ByteArraySerializer!");
        }
        return s;
    }

    public SerializerAdapter serializerFor(final Class type) {
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

    private SerializerAdapter lookupSerializer(Class type) {
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

    public SerializerAdapter serializerFor(final int typeId) {
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
        return serializationContext;
    }

    public PortableReader createPortableReader(Data data) {
        return new DefaultPortableReader(portableSerializer, createObjectDataInput(data), data.getClassDefinition());
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
        for (BufferObjectDataOutput output : outputPool) {
            IOUtil.closeResource(output);
        }
        outputPool.clear();
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public ManagedContext getManagedContext() {
        return managedContext;
    }
}
