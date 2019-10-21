/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.BooleanSerializer;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.ByteSerializer;
import com.hazelcast.internal.serialization.impl.ConstantSerializers.StringArraySerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassNameFilter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.partition.PartitioningStrategy;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import static com.hazelcast.internal.serialization.impl.ConstantSerializers.BooleanArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.CharArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.CharSerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.DoubleArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.DoubleSerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.FloatArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.FloatSerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.IntegerArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.IntegerSerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.LongArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.LongSerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.ShortArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.ShortSerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.StringSerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.TheByteArraySerializer;
import static com.hazelcast.internal.serialization.impl.ConstantSerializers.UuidSerializer;
import static com.hazelcast.internal.serialization.impl.DataSerializableSerializer.EE_FLAG;
import static com.hazelcast.internal.serialization.impl.DataSerializableSerializer.IDS_FLAG;
import static com.hazelcast.internal.serialization.impl.DataSerializableSerializer.isFlagSet;
import static com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.BigDecimalSerializer;
import static com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.BigIntegerSerializer;
import static com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.ClassSerializer;
import static com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.DateSerializer;
import static com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.HazelcastJsonValueSerializer;
import static com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.JavaSerializer;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createSerializerAdapter;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class SerializationServiceV1 extends AbstractSerializationService {

    private static final int FACTORY_AND_CLASS_ID_BYTE_LENGTH = 8;
    private static final int EE_BYTE_LENGTH = 2;

    private final PortableContextImpl portableContext;
    private final PortableSerializer portableSerializer;

    SerializationServiceV1(AbstractBuilder<?> builder) {
        super(builder);
        PortableHookLoader loader = new PortableHookLoader(builder.portableFactories, builder.getClassLoader());
        portableContext = new PortableContextImpl(this, builder.portableVersion);
        for (ClassDefinition cd : loader.getDefinitions()) {
            portableContext.registerClassDefinition(cd);
        }

        dataSerializerAdapter = createSerializerAdapter(
                new DataSerializableSerializer(builder.dataSerializableFactories, builder.getClassLoader()), this);
        portableSerializer = new PortableSerializer(portableContext, loader.getFactories());
        portableSerializerAdapter = createSerializerAdapter(portableSerializer, this);

        javaSerializerAdapter = createSerializerAdapter(
                new JavaSerializer(builder.enableSharedObject, builder.enableCompression, builder.classNameFilter), this);
        javaExternalizableAdapter = createSerializerAdapter(
                new JavaDefaultSerializers.ExternalizableSerializer(builder.enableCompression, builder.classNameFilter), this);
        registerConstantSerializers();
        registerJavaTypeSerializers();
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type) {
        if (type == DataType.NATIVE) {
            throw new IllegalArgumentException("Native data type is not supported");
        }
        return toData(obj);
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy) {
        if (type == DataType.NATIVE) {
            throw new IllegalArgumentException("Native data type is not supported");
        }
        return toData(obj, strategy);
    }

    @Override
    public <B extends Data> B convertData(Data data, DataType type) {
        if (type == DataType.NATIVE) {
            throw new IllegalArgumentException("Native data type is not supported");
        }
        return (B) data;
    }

    public PortableReader createPortableReader(Data data) throws IOException {
        if (!data.isPortable()) {
            throw new IllegalArgumentException("Given data is not Portable! -> " + data.getType());
        }
        BufferObjectDataInput in = createObjectDataInput(data);
        return portableSerializer.createReader(in);
    }

    public PortableContext getPortableContext() {
        return portableContext;
    }

    private void registerConstantSerializers() {
        registerConstant(null, nullSerializerAdapter);
        registerConstant(DataSerializable.class, dataSerializerAdapter);
        registerConstant(Portable.class, portableSerializerAdapter);
        //primitives and String
        registerConstant(Byte.class, new ByteSerializer());
        registerConstant(Boolean.class, new BooleanSerializer());
        registerConstant(Character.class, new CharSerializer());
        registerConstant(Short.class, new ShortSerializer());
        registerConstant(Integer.class, new IntegerSerializer());
        registerConstant(Long.class, new LongSerializer());
        registerConstant(Float.class, new FloatSerializer());
        registerConstant(Double.class, new DoubleSerializer());
        registerConstant(String.class, new StringSerializer());
        registerConstant(UUID.class, new UuidSerializer());
        //Arrays of primitives and String
        registerConstant(byte[].class, new TheByteArraySerializer());
        registerConstant(boolean[].class, new BooleanArraySerializer());
        registerConstant(char[].class, new CharArraySerializer());
        registerConstant(short[].class, new ShortArraySerializer());
        registerConstant(int[].class, new IntegerArraySerializer());
        registerConstant(long[].class, new LongArraySerializer());
        registerConstant(float[].class, new FloatArraySerializer());
        registerConstant(double[].class, new DoubleArraySerializer());
        registerConstant(String[].class, new StringArraySerializer());
    }

    private void registerJavaTypeSerializers() {
        //Java extensions: more serializers
        registerConstant(Class.class, new ClassSerializer());
        registerConstant(Date.class, new DateSerializer());
        registerConstant(BigInteger.class, new BigIntegerSerializer());
        registerConstant(BigDecimal.class, new BigDecimalSerializer());

        registerConstant(Object[].class, new ArrayStreamSerializer());

        registerConstant(ArrayList.class, new ArrayListStreamSerializer());
        registerConstant(LinkedList.class, new LinkedListStreamSerializer());
        registerConstant(CopyOnWriteArrayList.class, new CopyOnWriteArrayListStreamSerializer());

        registerConstant(HashMap.class, new HashMapStreamSerializer());
        registerConstant(ConcurrentSkipListMap.class, new ConcurrentSkipListMapStreamSerializer());
        registerConstant(ConcurrentHashMap.class, new ConcurrentHashMapStreamSerializer());
        registerConstant(LinkedHashMap.class, new LinkedHashMapStreamSerializer());
        registerConstant(TreeMap.class, new TreeMapStreamSerializer());

        registerConstant(HashSet.class, new HashSetStreamSerializer());
        registerConstant(TreeSet.class, new TreeSetStreamSerializer());
        registerConstant(LinkedHashSet.class, new LinkedHashSetStreamSerializer());
        registerConstant(CopyOnWriteArraySet.class, new CopyOnWriteArraySetStreamSerializer());
        registerConstant(ConcurrentSkipListSet.class, new ConcurrentSkipListSetStreamSerializer());
        registerConstant(ArrayDeque.class, new ArrayDequeStreamSerializer());
        registerConstant(LinkedBlockingQueue.class, new LinkedBlockingQueueStreamSerializer());
        registerConstant(ArrayBlockingQueue.class, new ArrayBlockingQueueStreamSerializer());
        registerConstant(PriorityBlockingQueue.class, new PriorityBlockingQueueStreamSerializer());
        registerConstant(DelayQueue.class, new DelayQueueStreamSerializer());
        registerConstant(SynchronousQueue.class, new SynchronousQueueStreamSerializer());
        registerConstant(LinkedTransferQueue.class, new LinkedTransferQueueStreamSerializer());

        safeRegister(Serializable.class, javaSerializerAdapter);
        safeRegister(Externalizable.class, javaExternalizableAdapter);
        safeRegister(HazelcastJsonValue.class, new HazelcastJsonValueSerializer());
    }

    public void registerClassDefinitions(Collection<ClassDefinition> classDefinitions, boolean checkClassDefErrors) {
        final Map<Integer, ClassDefinition> classDefMap = createHashMap(classDefinitions.size());
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

    protected void registerClassDefinition(ClassDefinition cd, Map<Integer, ClassDefinition> classDefMap,
                                           boolean checkClassDefErrors) {
        final Set<String> fieldNames = cd.getFieldNames();
        for (String fieldName : fieldNames) {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd.getType() == FieldType.PORTABLE || fd.getType() == FieldType.PORTABLE_ARRAY) {
                int classId = fd.getClassId();
                ClassDefinition nestedCd = classDefMap.get(classId);
                if (nestedCd != null) {
                    registerClassDefinition(nestedCd, classDefMap, checkClassDefErrors);
                    portableContext.registerClassDefinition(nestedCd);
                } else if (checkClassDefErrors) {
                    throw new HazelcastSerializationException(
                            "Could not find registered ClassDefinition for class-id: " + classId);
                }
            }
        }
        portableContext.registerClassDefinition(cd);
    }

    final PortableSerializer getPortableSerializer() {
        return portableSerializer;
    }

    /**
     * Init the ObjectDataInput for the given Data skipping the serialization header-bytes and navigating to the position
     * from where the readData() starts reading the object fields.
     *
     * @param data data to initialize the ObjectDataInput with.
     * @return the initialized ObjectDataInput without the header.
     * @throws IOException
     */
    public ObjectDataInput initDataSerializableInputAndSkipTheHeader(Data data) throws IOException {
        ObjectDataInput input = createObjectDataInput(data);
        byte header = input.readByte();
        if (isFlagSet(header, IDS_FLAG)) {
            skipBytesSafely(input, FACTORY_AND_CLASS_ID_BYTE_LENGTH);
        } else {
            input.readUTF();
        }

        if (isFlagSet(header, EE_FLAG)) {
            skipBytesSafely(input, EE_BYTE_LENGTH);
        }
        return input;
    }

    public static Builder builder() {
        return new Builder();
    }

    private void skipBytesSafely(ObjectDataInput input, int count) throws IOException {
        if (input.skipBytes(count) != count) {
            throw new HazelcastSerializationException("Malformed serialization format");
        }
    }

    public abstract static class AbstractBuilder<T extends AbstractBuilder<T>> extends AbstractSerializationService.Builder<T> {

        private int portableVersion;
        private Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories = Collections.emptyMap();
        private Map<Integer, ? extends PortableFactory> portableFactories = Collections.emptyMap();
        private boolean enableCompression;
        private boolean enableSharedObject;
        private ClassNameFilter classNameFilter;

        protected AbstractBuilder() {
        }

        public final T withPortableVersion(int portableVersion) {
            this.portableVersion = portableVersion;
            return self();
        }

        public final T withDataSerializableFactories(
                Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories) {
            this.dataSerializableFactories = dataSerializableFactories;
            return self();
        }

        public Map<Integer, ? extends DataSerializableFactory> getDataSerializableFactories() {
            return dataSerializableFactories;
        }

        public final T withPortableFactories(Map<Integer, ? extends PortableFactory> portableFactories) {
            this.portableFactories = portableFactories;
            return self();
        }

        public final T withEnableCompression(boolean enableCompression) {
            this.enableCompression = enableCompression;
            return self();
        }

        public final T withEnableSharedObject(boolean enableSharedObject) {
            this.enableSharedObject = enableSharedObject;
            return self();
        }

        public final T withClassNameFilter(ClassNameFilter classNameFilter) {
            this.classNameFilter = classNameFilter;
            return self();
        }
    }

    public static final class Builder extends AbstractBuilder<Builder> {

        protected Builder() {
        }

        @Override
        protected Builder self() {
            return this;
        }

        public SerializationServiceV1 build() {
            return new SerializationServiceV1(this);
        }

    }
}
