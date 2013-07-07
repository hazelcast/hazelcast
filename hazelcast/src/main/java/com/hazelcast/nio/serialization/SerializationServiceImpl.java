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
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.ConstantSerializers.*;
import com.hazelcast.nio.serialization.DefaultSerializers.*;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class SerializationServiceImpl implements SerializationService {

    private static final int OUTPUT_BUFFER_SIZE = 4 * 1024;
    private static final int CONSTANT_SERIALIZERS_SIZE = SerializationConstants.CONSTANT_SERIALIZERS_LENGTH;

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
    private final SerializationContextImpl serializationContext;

    private volatile boolean active = true;

    SerializationServiceImpl(InputOutputFactory inputOutputFactory, int version, ClassLoader classLoader,
                             Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                             Map<Integer, ? extends PortableFactory> portableFactories,
                             Collection<ClassDefinition> classDefinitions, boolean checkClassDefErrors,
                             ManagedContext managedContext, boolean enableCompression, boolean enableSharedObject) {

        this.inputOutputFactory = inputOutputFactory;
        this.classLoader = classLoader;
        this.managedContext = managedContext;
        PortableHookLoader loader = new PortableHookLoader(portableFactories, classLoader);
        serializationContext = new SerializationContextImpl(loader.getFactories().keySet(), version);
        for (ClassDefinition cd : loader.getDefinitions()) {
            serializationContext.registerClassDefinition(cd);
        }

        dataSerializerAdapter = new StreamSerializerAdapter(this, new DataSerializer(dataSerializableFactories, classLoader));
        portableSerializer = new PortableSerializer(serializationContext, loader.getFactories());
        portableSerializerAdapter = new StreamSerializerAdapter(this, portableSerializer);

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
        safeRegister(Date.class, new DateSerializer());
        safeRegister(BigInteger.class, new BigIntegerSerializer());
        safeRegister(BigDecimal.class, new BigDecimalSerializer());
        safeRegister(Externalizable.class, new Externalizer(enableCompression));
        safeRegister(Serializable.class, new ObjectSerializer(enableSharedObject, enableCompression));
        safeRegister(Class.class, new ClassSerializer());

        registerClassDefinitions(classDefinitions, checkClassDefErrors);
    }

    private void registerClassDefinitions(final Collection<ClassDefinition> classDefinitions, boolean checkClassDefErrors) {
        final Map<Integer, ClassDefinition> classDefMap = new HashMap<Integer, ClassDefinition>(classDefinitions.size());
        for (ClassDefinition cd : classDefinitions) {
            if (classDefMap.containsKey(cd.getClassId())) {
                throw new HazelcastSerializationException("Duplicate registration found for class-id[" + cd.getClassId() + "]!");
            }
            classDefMap.put(cd.getClassId(), cd);
        }
        for (ClassDefinition classDefinition : classDefinitions) {
            registerClassDefinition(classDefinition, classDefMap, checkClassDefErrors);
        }
    }

    private void registerClassDefinition(ClassDefinition cd, Map<Integer, ClassDefinition> classDefMap, boolean checkClassDefErrors) {
        for (int i = 0; i < cd.getFieldCount(); i++) {
            FieldDefinition fd = cd.get(i);
            if (fd.getType() == FieldType.PORTABLE || fd.getType() == FieldType.PORTABLE_ARRAY) {
                int classId = fd.getClassId();
                ClassDefinition nestedCd = classDefMap.get(classId);
                if (nestedCd != null) {
                    ((ClassDefinitionImpl) cd).addClassDef(nestedCd);
                    registerClassDefinition(nestedCd, classDefMap, checkClassDefErrors);
                    serializationContext.registerClassDefinition(nestedCd);
                } else if (checkClassDefErrors) {
                    throw new HazelcastSerializationException("Could not find registered ClassDefinition for class-id: " + classId);
                }
            }
        }
        serializationContext.registerClassDefinition(cd);
    }

    @SuppressWarnings("unchecked")
    public Data toData(final Object obj) {
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
                data.classDefinition = serializationContext.lookup(portable.getFactoryId(), portable.getClassId());
            }
            if (obj instanceof PartitionAware) {
                final Object pk = ((PartitionAware) obj).getPartitionKey();
                final Data partitionKey = toData(pk);
                data.partitionHash = (partitionKey == null) ? -1 : partitionKey.getPartitionHash();
            }
            return data;
        } catch (Throwable e) {
            handleException(e);
        }
        return null;
    }

    public Object toObject(final Data data) {
        if (data == null || data.bufferSize() == 0) {
            return null;
        }
        try {
            final int typeId = data.type;
            final SerializerAdapter serializer = serializerFor(typeId);
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId);
                }
                throw new HazelcastInstanceNotActiveException();
            }
            if (data.type == SerializationConstants.CONSTANT_TYPE_PORTABLE) {
                serializationContext.registerClassDefinition(data.classDefinition);
            }
            Object obj = serializer.read(data);
            if (managedContext != null) {
                obj = managedContext.initialize(obj);
            }
            return obj;
        } catch (Throwable e) {
            handleException(e);
        }
        return null;
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
            out = inputOutputFactory.createOutput(OUTPUT_BUFFER_SIZE, this);
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
            throw new IllegalArgumentException("Type id must be positive! Current: " + serializer.getTypeId() + ", Serializer: " + serializer);
        }
        safeRegister(type, createSerializerAdapter(serializer));
    }

    public void registerGlobal(final Serializer serializer) {
        if (!global.compareAndSet(null, createSerializerAdapter(serializer))) {
//            throw new IllegalStateException("Fallback serializer is already registered!");
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
        } return s;
    }

    public SerializerAdapter serializerFor(final Class type) {
        if (DataSerializable.class.isAssignableFrom(type)) {
            return dataSerializerAdapter;
        } else if (Portable.class.isAssignableFrom(type)) {
            return portableSerializerAdapter;
        } else {
            final SerializerAdapter serializer;
            if ((serializer = constantTypesMap.get(type)) != null) {
                return serializer;
            }
        }
        SerializerAdapter serializer = typeMap.get(type);
        if (serializer == null) {
            // look for super classes
            Class typeSuperclass = type.getSuperclass();
            final Set<Class> interfaces = new LinkedHashSet<Class>(5);
            getInterfaces(type, interfaces);
            while (typeSuperclass != null) {
                if ((serializer = registerFromSuperType(type, typeSuperclass)) != null) {
                    break;
                }
                getInterfaces(typeSuperclass, interfaces);
                typeSuperclass = typeSuperclass.getSuperclass();
            }
            if (serializer == null) {
                // look for interfaces
                for (Class typeInterface : interfaces) {
                    if ((serializer = registerFromSuperType(type, typeInterface)) != null) {
                        break;
                    }
                }
            }
            if (serializer == null && (serializer = global.get()) != null) {
                safeRegister(type, serializer);
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

    private void safeRegister(final Class type, final Serializer serializer) {
        safeRegister(type, createSerializerAdapter(serializer));
    }

    private void safeRegister(final Class type, final SerializerAdapter serializer) {
        if (constantTypesMap.containsKey(type)) {
            throw new IllegalArgumentException("[" + type + "] serializer cannot be overridden!");
        }
        SerializerAdapter current = typeMap.putIfAbsent(type, serializer);
        if (current != null && current.getImpl().getClass() != serializer.getImpl().getClass()) {
            throw new IllegalStateException("Serializer[" + current + "] has been already registered for type: " + type);
        }
        current = idMap.putIfAbsent(serializer.getTypeId(), serializer);
        if (current != null && current.getImpl().getClass() != serializer.getImpl().getClass()) {
            throw new IllegalStateException("Serializer [" + current + "] has been already registered for type-id: "
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

    public SerializationContext getSerializationContext() {
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

    private class SerializationContextImpl implements SerializationContext {

        final int ctxVersion;
        final Map<Integer, PortableContext> portableContextMap;

        private SerializationContextImpl(Collection<Integer> portableFactories, int version) {
            this.ctxVersion = version;
            final Map<Integer, PortableContext> portableMap = new HashMap<Integer, PortableContext>();
            for (int factoryId : portableFactories) {
                portableMap.put(factoryId, new PortableContext());
            }
            portableContextMap = portableMap; // do not modify!
        }

        public ClassDefinition lookup(int factoryId, int classId) {
            return getPortableContext(factoryId).lookup(classId, ctxVersion);
        }

        public ClassDefinition lookup(int factoryId, int classId, int version) {
            return getPortableContext(factoryId).lookup(classId, version);
        }

        public ClassDefinition createClassDefinition(int factoryId, final byte[] compressedBinary) throws IOException {
            return getPortableContext(factoryId).createClassDefinition(compressedBinary);
        }

        public ClassDefinition registerClassDefinition(final ClassDefinition cd) {
            return getPortableContext(cd.getFactoryId()).registerClassDefinition(cd);
        }

        private void registerNestedDefinitions(ClassDefinitionImpl cd) {
            Collection<ClassDefinition> nestedDefinitions = cd.getNestedClassDefinitions();
            for (ClassDefinition classDefinition : nestedDefinitions) {
                final ClassDefinitionImpl nestedCD = (ClassDefinitionImpl) classDefinition;
                registerClassDefinition(nestedCD);
                registerNestedDefinitions(nestedCD);
            }
        }

        private PortableContext getPortableContext(int factoryId) {
            final PortableContext ctx = portableContextMap.get(factoryId);
            if (ctx == null) {
                throw new HazelcastSerializationException("Could not find PortableFactory for factoryId: " + factoryId);
            }
            return ctx;
        }

        public int getVersion() {
            return ctxVersion;
        }
    }

    private class PortableContext {

        final ConcurrentMap<Long, ClassDefinitionImpl> versionedDefinitions = new ConcurrentHashMap<Long,
                ClassDefinitionImpl>();

        ClassDefinition lookup(int classId, int version) {
            return versionedDefinitions.get(combineToLong(classId, version));
        }

        ClassDefinition createClassDefinition(byte[] compressedBinary) throws IOException {
            final BufferObjectDataOutput out = pop();
            final byte[] binary;
            try {
                decompress(compressedBinary, out);
                binary = out.toByteArray();
            } finally {
                push(out);
            }
            final ClassDefinitionImpl cd = new ClassDefinitionImpl();
            cd.readData(inputOutputFactory.createInput(binary, SerializationServiceImpl.this));
            cd.setBinary(compressedBinary);
            final ClassDefinitionImpl currentCD = versionedDefinitions.putIfAbsent(combineToLong(cd.classId,
                    serializationContext.getVersion()), cd);
            if (currentCD == null) {
                serializationContext.registerNestedDefinitions(cd);
                return cd;
            } else {
                return currentCD;
            }
        }

        ClassDefinition registerClassDefinition(ClassDefinition cd) {
            if (cd == null) return null;
            final ClassDefinitionImpl cdImpl = (ClassDefinitionImpl) cd;
            if (cdImpl.getVersion() < 0) {
                cdImpl.version = serializationContext.getVersion();
            }
            final long versionedClassId = combineToLong(cdImpl.getClassId(), cdImpl.getVersion());
            final ClassDefinitionImpl currentClassDef = versionedDefinitions.putIfAbsent(versionedClassId, cdImpl);
            if (currentClassDef == null) {
                serializationContext.registerNestedDefinitions(cdImpl);
                if (cdImpl.getBinary() == null) {
                    final BufferObjectDataOutput out = pop();
                    try {
                        cdImpl.writeData(out);
                        final byte[] binary = out.toByteArray();
                        out.clear();
                        compress(binary, out);
                        cdImpl.setBinary(out.toByteArray());
                    } catch (IOException e) {
                        throw new HazelcastSerializationException(e);
                    } finally {
                        push(out);
                    }
                }
                return cd;
            }
            return currentClassDef;
        }
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    private static void compress(byte[] input, DataOutput out) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setLevel(Deflater.BEST_COMPRESSION);
        deflater.setInput(input);
        deflater.finish();
        byte[] buf = new byte[input.length / 10];
        while (!deflater.finished()) {
            int count = deflater.deflate(buf);
            out.write(buf, 0, count);
        }
        deflater.end();
    }

    private static void decompress(byte[] compressedData, DataOutput out) throws IOException {
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

    private static long combineToLong(int x, int y) {
        return ((long) x << 32) | ((long) y & 0xFFFFFFFL);
    }

    private static int extractInt(long value, boolean lowerBits) {
        return (lowerBits) ? (int) value : (int) (value >> 32);
    }
}
