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

import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.TypeSerializerConfig;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.ClassLoaderUtil;
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
    private final SerializationContextImpl serializationContext;

    private volatile boolean active = true;

    public SerializationServiceImpl(int version) {
        this(version, Collections.<Integer, DataSerializableFactory>emptyMap(), Collections.<Integer, PortableFactory>emptyMap(), null);
    }

    public SerializationServiceImpl(int version, Map<Integer, ? extends PortableFactory> portableFactoryMap) {
        this(version, Collections.<Integer, DataSerializableFactory>emptyMap(), portableFactoryMap, null);
    }

    public SerializationServiceImpl(SerializationConfig config, ManagedContext managedContext) throws Exception {
        this(config.getPortableVersion(), createDataSerializableFactories(config), createPortableFactories(config), managedContext);
        if (config.getGlobalSerializer() != null) {
            GlobalSerializerConfig globalSerializerConfig = config.getGlobalSerializer();
            TypeSerializer serializer = globalSerializerConfig.getImplementation();
            if (serializer == null) {
                serializer = ClassLoaderUtil.newInstance(globalSerializerConfig.getClassName());
            }
            registerFallback(serializer);
        }
        final Collection<TypeSerializerConfig> typeSerializers = config.getTypeSerializers();
        for (TypeSerializerConfig typeSerializerConfig : typeSerializers) {
            TypeSerializer serializer = typeSerializerConfig.getImplementation();
            if (serializer == null) {
                serializer = ClassLoaderUtil.newInstance(typeSerializerConfig.getClassName());
            }
            Class typeClass = typeSerializerConfig.getTypeClass();
            if (typeClass == null) {
                typeClass = ClassLoaderUtil.loadClass(typeSerializerConfig.getTypeClassName());
            }
            register(serializer, typeClass);
        }
        registerConfiguredClassDefinitions(config);
    }

    private static Map<Integer, DataSerializableFactory> createDataSerializableFactories(SerializationConfig config) throws Exception {
        final Map<Integer, DataSerializableFactory> dataSerializableFactories = new HashMap<Integer, DataSerializableFactory>(config.getDataSerializableFactories());
        final Map<Integer, String> dataSerializableFactoryClasses = config.getDataSerializableFactoryClasses();
        for (Map.Entry<Integer, String> entry : dataSerializableFactoryClasses.entrySet()) {
            if (dataSerializableFactories.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("DataSerializableFactory with factoryId '" + entry.getKey() + "' is already registered!");
            }
            DataSerializableFactory f = ClassLoaderUtil.newInstance(entry.getValue());
            dataSerializableFactories.put(entry.getKey(), f);
        }
        for (Map.Entry<Integer, DataSerializableFactory> entry : dataSerializableFactories.entrySet()) {
            if (entry.getKey() <= 0 ) {
                throw new IllegalArgumentException("DataSerializableFactory factoryId must be positive! -> " + entry.getValue());
            }
        }
        return dataSerializableFactories;
    }

    private static Map<Integer, PortableFactory> createPortableFactories(SerializationConfig config) throws Exception {
        final Map<Integer, PortableFactory> portableFactories = new HashMap<Integer, PortableFactory>(config.getPortableFactories());
        final Map<Integer, String> portableFactoryClasses = config.getPortableFactoryClasses();
        for (Map.Entry<Integer, String> entry : portableFactoryClasses.entrySet()) {
            if (portableFactories.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("PortableFactory with factoryId '" + entry.getKey() + "' is already registered!");
            }
            PortableFactory f = ClassLoaderUtil.newInstance(entry.getValue());
            portableFactories.put(entry.getKey(), f);
        }
        for (Map.Entry<Integer, PortableFactory> entry : portableFactories.entrySet()) {
            if (entry.getKey() <= 0 ) {
                throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> " + entry.getValue());
            }
        }
        return portableFactories;
    }

    private void registerConfiguredClassDefinitions(SerializationConfig config) {
        final Collection<ClassDefinition> classDefinitions = config.getClassDefinitions();
        final Map<Integer, ClassDefinition> classDefMap = new HashMap<Integer, ClassDefinition>(classDefinitions.size());
        for (ClassDefinition cd : classDefinitions) {
            if (classDefMap.containsKey(cd.getClassId())) {
                throw new HazelcastSerializationException("Duplicate registration found for class-id[" + cd.getClassId() + "]!");
            }
            classDefMap.put(cd.getClassId(), cd);
        }
        for (ClassDefinition classDefinition : classDefinitions) {
            registerClassDefinition(classDefinition, classDefMap, config.isCheckClassDefErrors());
        }
    }

    private void registerClassDefinition(ClassDefinition cd, Map<Integer, ClassDefinition> classDefMap, boolean checkClassDefErrors) {
        for (int i = 0; i < cd.getFieldCount(); i++) {
            FieldDefinition fd = cd.get(i);
            if (fd.getType() == FieldType.PORTABLE || fd.getType() == FieldType.PORTABLE_ARRAY) {
                int classId = fd.getClassId();
                ClassDefinition nestedCd = classDefMap.get(classId);
                if (nestedCd != null) {
                    ((ClassDefinitionImpl) cd).add((ClassDefinitionImpl) nestedCd);
                    registerClassDefinition(nestedCd, classDefMap, checkClassDefErrors);
                    serializationContext.registerClassDefinition(nestedCd);
                } else if (checkClassDefErrors) {
                    throw new HazelcastSerializationException("Could not find registered ClassDefinition for class-id: " + classId);
                }
            }
        }
        serializationContext.registerClassDefinition(cd);
    }

    public SerializationServiceImpl(int version, Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                                    Map<Integer, ? extends PortableFactory> portableFactories, ManagedContext managedContext) {
        this.managedContext = managedContext;
        registerConstant(DataSerializable.class, dataSerializer = new DataSerializer(dataSerializableFactories));
        registerConstant(Portable.class, portableSerializer = new PortableSerializer(this, portableFactories));
        registerConstant(Byte.class, new ByteSerializer());
        registerConstant(Boolean.class, new BooleanSerializer());
        registerConstant(Character.class, new CharSerializer());
        registerConstant(Short.class, new ShortSerializer());
        registerConstant(Integer.class, new IntegerSerializer());
        registerConstant(Long.class, new LongSerializer());
        registerConstant(Float.class, new FloatSerializer());
        registerConstant(Double.class, new DoubleSerializer());
        registerConstant(byte[].class, new ByteArraySerializer());
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
        safeRegister(Externalizable.class, new Externalizer());
        safeRegister(Serializable.class, new ObjectSerializer());
        safeRegister(Class.class, new ClassSerializer());

        serializationContext = new SerializationContextImpl(portableSerializer.getFactories().keySet(), version);
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
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable serializer for " + obj.getClass());
                }
                throw new HazelcastInstanceNotActiveException();
            }
            serializer.write(out, obj);
            final Data data = new Data(serializer.getTypeId(), out.toByteArray());
            if (obj instanceof Portable) {
                final Portable portable = (Portable) obj;
                data.classDefinition = serializationContext.lookup(portable.getFactoryId(), portable.getClassId());
            }
            if (obj instanceof PartitionAware) {
                final Object pk = ((PartitionAware) obj).getPartitionKey();
                final Data partitionKey = toData(pk);
                final int partitionHash = (partitionKey == null) ? -1 : partitionKey.getPartitionHash();
                data.setPartitionHash(partitionHash);
            }
            return data;
        } catch (Throwable e) {
            handleException(e);
        } finally {
            push(out);
        }
        return null;
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
        if (data == null || data.bufferSize() == 0) {
            return null;
        }
        ContextAwareDataInput in = null;
        try {
            final int typeId = data.type;
            final TypeSerializer serializer = serializerFor(typeId);
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId);
                }
                throw new HazelcastInstanceNotActiveException();
            }
            if (data.type == SerializationConstants.CONSTANT_TYPE_PORTABLE) {
                serializationContext.registerClassDefinition(data.classDefinition);
            }
            in = new ContextAwareDataInput(data, this);
            Object obj = serializer.read(in);
            if (managedContext != null) {
                obj = managedContext.initialize(obj);
            }
            return obj;
        } catch (Throwable e) {
            handleException(e);
        } finally {
            IOUtil.closeResource(in);
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
            final TypeSerializer serializer = serializerFor(obj.getClass());
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
            final TypeSerializer serializer = serializerFor(typeId);
            if (serializer == null) {
                if (active) {
                    throw new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId);
                }
                throw new HazelcastInstanceNotActiveException();
            }
            final Object obj = serializer.read(in);
            if (managedContext != null) {
                managedContext.initialize(obj);
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

    public ObjectDataInput createObjectDataInput(byte[] data) {
        return new ContextAwareDataInput(data, this);
    }

    public ObjectDataInput createObjectDataInput(Data data) {
        return createObjectDataInput(data.buffer);
    }

    public ObjectDataOutput createObjectDataOutput(int size) {
        return new ContextAwareDataOutput(size, this);
    }

    public ObjectDataOutputStream createObjectDataOutputStream(OutputStream out) {
        return new ObjectDataOutputStream(out, this);
    }

    public ObjectDataInputStream createObjectDataInputStream(InputStream in) {
        return new ObjectDataInputStream(in, this);
    }

    public PortableSerializer getPortableSerializer() {
        return portableSerializer;
    }

    public void register(final TypeSerializer serializer, final Class type) {
        if (type == null) {
            throw new IllegalArgumentException("Class type information is required!");
        }
        if (serializer.getTypeId() <= 0) {
            throw new IllegalArgumentException("Type id must be positive! Current: "
                    + serializer.getTypeId() + ", Serializer: " + serializer);
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

    private void registerConstant(Class type, TypeSerializer serializer) {
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
        active = false;
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
                    final ContextAwareDataOutput out = pop();
                    try {
                        cdImpl.writeData(out);
                        final byte[] binary = out.toByteArray();
                        out.reset();
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

    private static void compress(byte[] input, OutputStream out) throws IOException {
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

    private static void decompress(byte[] compressedData, OutputStream out) throws IOException {
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
