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
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ConstantSerializers.*;
import com.hazelcast.nio.serialization.DefaultSerializers.*;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class SerializationServiceImpl implements SerializationService {

    private static final int OUTPUT_STREAM_BUFFER_SIZE = 32 * 1024;
    private static final int CONSTANT_SERIALIZERS_SIZE = SerializationConstants.CONSTANT_SERIALIZERS_LENGTH;

    private final IdentityHashMap<Class, TypeSerializer> constantTypesMap
            = new IdentityHashMap<Class, TypeSerializer>(CONSTANT_SERIALIZERS_SIZE);
    private final TypeSerializer[] constantTypeIds = new TypeSerializer[CONSTANT_SERIALIZERS_SIZE];

    private final ConcurrentMap<Class, TypeSerializer> typeMap = new ConcurrentHashMap<Class, TypeSerializer>();
    private final ConcurrentMap<Integer, TypeSerializer> idMap = new ConcurrentHashMap<Integer, TypeSerializer>();
    private final AtomicReference<TypeSerializer> fallback = new AtomicReference<TypeSerializer>();

    private final Queue<ContextAwareDataOutput> outputPool = new ConcurrentLinkedQueue<ContextAwareDataOutput>();
    private final DataSerializer dataSerializer;
    private final PortableSerializer portableSerializer;
    private final ManagedContext managedContext;
    private final SerializationContext serializationContext;

    public SerializationServiceImpl(int version, PortableFactory portableFactory) {
        this(version, portableFactory, null);
    }

    public SerializationServiceImpl(int version, PortableFactory portableFactory, ManagedContext managedContext) {
        this.managedContext = managedContext;
        serializationContext = new SerializationContextImpl(portableFactory, version);
        registerDefault(DataSerializable.class, dataSerializer = new DataSerializer());
        registerDefault(Portable.class, portableSerializer = new PortableSerializer(serializationContext));
        registerDefault(Byte.class, new ByteSerializer());
        registerDefault(Boolean.class, new BooleanSerializer());
        registerDefault(Character.class, new CharSerializer());
        registerDefault(Short.class, new ShortSerializer());
        registerDefault(Integer.class, new IntegerSerializer());
        registerDefault(Long.class, new LongSerializer());
        registerDefault(Float.class, new FloatSerializer());
        registerDefault(Double.class, new DoubleSerializer());
        registerDefault(byte[].class, new ByteArraySerializer());
        registerDefault(char[].class, new CharArraySerializer());
        registerDefault(short[].class, new ShortArraySerializer());
        registerDefault(int[].class, new IntegerArraySerializer());
        registerDefault(long[].class, new LongArraySerializer());
        registerDefault(float[].class, new FloatArraySerializer());
        registerDefault(double[].class, new DoubleArraySerializer());
        registerDefault(String.class, new StringSerializer());
        safeRegister(Date.class, new DateSerializer());
        safeRegister(BigInteger.class, new BigIntegerSerializer());
        safeRegister(BigDecimal.class, new BigDecimalSerializer());
        safeRegister(Externalizable.class, new Externalizer());
        safeRegister(Serializable.class, new ObjectSerializer());
        safeRegister(Class.class, new ClassSerializer());
    }

    @SuppressWarnings("unchecked")
    public Data toData(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return (Data) obj;
        }
        final ContextAwareDataOutput out = pop();
        try {
            final TypeSerializer serializer = serializerFor(obj.getClass());
            if (serializer == null) {
                throw new NotSerializableException("There is no suitable serializer for " + obj.getClass());
            }
            serializer.write(out, obj);
            final Data data = new Data(serializer.getTypeId(), out.toByteArray());
            if (obj instanceof Portable) {
                data.cd = serializationContext.lookup(((Portable) obj).getClassId());
            }
            if (obj instanceof PartitionAware) {
                final Object pk = ((PartitionAware) obj).getPartitionKey();
                final Data partitionKey = toData(pk);
                final int partitionHash = (partitionKey == null) ? -1 : partitionKey.getPartitionHash();
                data.setPartitionHash(partitionHash);
            }
            return data;
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            if (e instanceof HazelcastSerializationException) {
                throw (HazelcastSerializationException) e;
            }
            throw new HazelcastSerializationException(e);
        } finally {
            push(out);
        }
    }

    private ContextAwareDataOutput pop() {
        ContextAwareDataOutput out = outputPool.poll();
        if (out == null) {
            out = new ContextAwareDataOutput(OUTPUT_STREAM_BUFFER_SIZE, this);
        }
        return out;
    }

    void push(ContextAwareDataOutput out) {
        out.reset();
        outputPool.offer(out);
    }

    public Object toObject(final Data data) {
        if ((data == null) || (data.buffer == null) || (data.buffer.length == 0)) {
            return null;
        }
        ContextAwareDataInput in = null;
        try {
            final int typeId = data.type;
            final TypeSerializer serializer = serializerFor(typeId);
            if (serializer == null) {
                throw new IllegalArgumentException("There is no suitable de-serializer for type " + typeId);
            }
            if (data.type == SerializationConstants.CONSTANT_TYPE_PORTABLE) {
                serializationContext.registerClassDefinition(data.cd);
            }
            in = new ContextAwareDataInput(data, this);
            Object obj = serializer.read(in);
            if (managedContext != null) {
                managedContext.initialize(obj);
            }
            return obj;
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            if (e instanceof HazelcastSerializationException) {
                throw (HazelcastSerializationException) e;
            }
            throw new HazelcastSerializationException(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    public void writeObject(final ObjectDataOutput out, final Object obj) {
        if (obj == null) {
            throw new NullPointerException("Object is required!");
        }
        try {
            final TypeSerializer serializer = serializerFor(obj.getClass());
            if (serializer == null) {
                throw new NotSerializableException("There is no suitable serializer for " + obj.getClass());
            }
            out.writeInt(serializer.getTypeId());
            serializer.write(out, obj);
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            if (e instanceof HazelcastSerializationException) {
                throw (HazelcastSerializationException) e;
            }
            throw new HazelcastSerializationException(e);
        }
    }

    public Object readObject(final ObjectDataInput in) {
        try {
            int typeId = in.readInt();
            final TypeSerializer serializer = serializerFor(typeId);
            if (serializer == null) {
                throw new IllegalArgumentException("There is no suitable de-serializer for type " + typeId);
            }
            Object obj = serializer.read(in);
            if (managedContext != null) {
                managedContext.initialize(obj);
            }
            return obj;
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            if (e instanceof HazelcastSerializationException) {
                throw (HazelcastSerializationException) e;
            }
            throw new HazelcastSerializationException(e);
        }
    }

    public ObjectDataInput createObjectDataInput(byte[] data) {
        return new ContextAwareDataInput(data, this);
    }

    public ObjectDataOutput createObjectDataOutput(int size) {
        return new ContextAwareDataOutput(size, this);
    }

    public PortableSerializer getPortableSerializer() {
        return portableSerializer;
    }

    public void register(final TypeSerializer serializer, final Class type) {
        if (type == null) {
            throw new IllegalArgumentException("Class type information is required!");
        }
        if (serializer.getTypeId() < 0) {
            throw new IllegalArgumentException("Type id must be positive! Current: " + serializer.getTypeId());
        }
        safeRegister(type, serializer);
    }

    public void registerFallback(final TypeSerializer serializer) {
        if (!fallback.compareAndSet(null, serializer)) {
            throw new IllegalStateException("Fallback serializer is already registered!");
        }
    }

    public TypeSerializer serializerFor(final Class type) {
        if (DataSerializable.class.isAssignableFrom(type)) {
            return dataSerializer;
        } else if (Portable.class.isAssignableFrom(type)) {
            return portableSerializer;
        } else {
            final TypeSerializer serializer;
            if ((serializer = constantTypesMap.get(type)) != null) {
                return serializer;
            }
        }
        TypeSerializer serializer = typeMap.get(type);
        if (serializer == null) {
            // look for super classes
            Class typeSuperclass = type.getSuperclass();
            List<Class> interfaces = new LinkedList<Class>();
            Collections.addAll(interfaces, type.getInterfaces());
            while (typeSuperclass != null) {
                if ((serializer = registerFromSuperType(type, typeSuperclass)) != null) {
                    break;
                }
                Collections.addAll(interfaces, typeSuperclass.getInterfaces());
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
            if (serializer == null && (serializer = fallback.get()) != null) {
                safeRegister(type, serializer);
            }
        }
        return serializer;
    }

    private TypeSerializer registerFromSuperType(final Class type, final Class superType) {
        final TypeSerializer serializer = typeMap.get(superType);
        if (serializer != null) {
            safeRegister(type, serializer);
        }
        return serializer;
    }

    private void registerDefault(Class type, TypeSerializer serializer) {
        constantTypesMap.put(type, serializer);
        constantTypeIds[indexForDefaultType(serializer.getTypeId())] = serializer;
    }

    private void safeRegister(final Class type, final TypeSerializer serializer) {
        if (constantTypesMap.containsKey(type)) {
            throw new IllegalArgumentException("[" + type + "] serializer cannot be overridden!");
        }
        TypeSerializer ts = typeMap.putIfAbsent(type, serializer);
        if (ts != null && ts.getClass() != serializer.getClass()) {
            throw new IllegalStateException("Serializer[" + ts + "] has been already registered for type: " + type);
        }
        ts = idMap.putIfAbsent(serializer.getTypeId(), serializer);
        if (ts != null && ts.getClass() != serializer.getClass()) {
            throw new IllegalStateException("Serializer [" + ts + "] has been already registered for type-id: "
                    + serializer.getTypeId());
        }
    }

    public TypeSerializer serializerFor(final int typeId) {
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

    public void destroy() {
        for (TypeSerializer serializer : typeMap.values()) {
            serializer.destroy();
        }
        typeMap.clear();
        idMap.clear();
        fallback.set(null);
        constantTypesMap.clear();
        for (ContextAwareDataOutput output : outputPool) {
            output.close();
        }
        outputPool.clear();
    }

    private class SerializationContextImpl implements SerializationContext {

        final PortableFactory portableFactory;

        final int version;

        final ConcurrentMap<Long, ClassDefinitionImpl> versionedDefinitions = new ConcurrentHashMap<Long,
                ClassDefinitionImpl>();

        private SerializationContextImpl(PortableFactory portableFactory, int version) {
            this.portableFactory = portableFactory;
            this.version = version;
        }

        public ClassDefinitionImpl lookup(int classId) {
            return versionedDefinitions.get(combineToLong(classId, version));
        }

        public ClassDefinitionImpl lookup(int classId, int version) {
            return versionedDefinitions.get(combineToLong(classId, version));
        }

        public Portable createPortable(int classId) {
            return portableFactory.create(classId);
        }

        public ClassDefinitionImpl createClassDefinition(final byte[] compressedBinary) throws IOException {
            final ContextAwareDataOutput out = pop();
            final byte[] binary;
            try {
                decompress(compressedBinary, out);
                binary = out.toByteArray();
            } finally {
                push(out);
            }
            final ClassDefinitionImpl cd = new ClassDefinitionImpl();
            cd.readData(new ContextAwareDataInput(binary, SerializationServiceImpl.this));
            cd.setBinary(binary);
            final ClassDefinitionImpl currentCD = versionedDefinitions.putIfAbsent(combineToLong(cd.classId, version), cd);
            if (currentCD == null) {
                registerNestedDefinitions(cd);
                return cd;
            } else {
                return currentCD;
            }
        }

        private void registerNestedDefinitions(ClassDefinitionImpl cd) throws IOException {
            Collection<ClassDefinition> nestedDefinitions = cd.getNestedClassDefinitions();
            for (ClassDefinition classDefinition : nestedDefinitions) {
                final ClassDefinitionImpl nestedCD = (ClassDefinitionImpl) classDefinition;
                registerClassDefinition(nestedCD);
                registerNestedDefinitions(nestedCD);
            }
        }

        public void registerClassDefinition(ClassDefinition cd) throws IOException {
            if (cd == null) return;
            final ClassDefinitionImpl cdImpl = (ClassDefinitionImpl) cd;
            final long versionedClassId = combineToLong(cdImpl.getClassId(), cdImpl.getVersion());
            if (versionedDefinitions.putIfAbsent(versionedClassId, cdImpl) == null) {
                if (cdImpl.getBinary() == null) {
                    final ContextAwareDataOutput out = pop();
                    try {
                        cdImpl.writeData(out);
                        final byte[] binary = out.toByteArray();
                        out.reset();
                        compress(binary, out);
                        cdImpl.setBinary(out.toByteArray());
                    } finally {
                        push(out);
                    }
                }
            }
        }

        private void compress(byte[] input, OutputStream out) throws IOException {
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

        private void decompress(byte[] compressedData, OutputStream out) throws IOException {
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

        public int getVersion() {
            return version;
        }
    }

    private static long combineToLong(int x, int y) {
        return ((long) x << 32) | ((long) y & 0xFFFFFFFL);
    }

    private static int extractInt(long value, boolean lowerBits) {
        return (lowerBits) ? (int) value : (int) (value >> 32);
    }
}
