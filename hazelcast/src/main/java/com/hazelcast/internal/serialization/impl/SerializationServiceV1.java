/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.defaultserializers.ArrayBlockingQueueStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ArrayDequeStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ArrayListStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ArrayStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConcurrentHashMapStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConcurrentSkipListMapStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConcurrentSkipListSetStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.BooleanSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.ByteSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.StringArraySerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.CopyOnWriteArrayListStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.CopyOnWriteArraySetStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.DelayQueueStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.HashMapStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.HashSetStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers;
import com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.EnumSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.OptionalSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.LinkedBlockingQueueStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.LinkedHashMapStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.LinkedHashSetStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.LinkedListStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.LinkedTransferQueueStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.PriorityBlockingQueueStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.PriorityQueueStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.SynchronousQueueStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.TreeMapStreamSerializer;
import com.hazelcast.internal.serialization.impl.defaultserializers.TreeSetStreamSerializer;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.internal.serialization.impl.portable.PortableContextImpl;
import com.hazelcast.internal.serialization.impl.portable.PortableHookLoader;
import com.hazelcast.internal.serialization.impl.portable.PortableSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassNameFilter;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.partition.PartitioningStrategy;

import javax.annotation.Nonnull;
import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.AbstractMap;
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
import java.util.Optional;
import java.util.PriorityQueue;
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

import static com.hazelcast.internal.serialization.impl.DataSerializableSerializer.EE_FLAG;
import static com.hazelcast.internal.serialization.impl.DataSerializableSerializer.IDS_FLAG;
import static com.hazelcast.internal.serialization.impl.DataSerializableSerializer.isFlagSet;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createSerializerAdapter;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.BooleanArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.CharArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.CharSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.DoubleArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.DoubleSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.FloatArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.FloatSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.IntegerArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.IntegerSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.LongArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.LongSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.ShortArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.ShortSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.SimpleEntrySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.StringSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.TheByteArraySerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.UuidSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.BigDecimalSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.BigIntegerSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.ClassSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.DateSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.HazelcastJsonValueSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.JavaSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.LocalDateSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.LocalDateTimeSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.LocalTimeSerializer;
import static com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.OffsetDateTimeSerializer;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class SerializationServiceV1 extends AbstractSerializationService {

    private static final int FACTORY_AND_CLASS_ID_BYTE_LENGTH = 8;
    private static final int EE_BYTE_LENGTH = 2;

    private final PortableContextImpl portableContext;
    private final PortableSerializer portableSerializer;

    SerializationServiceV1(AbstractBuilder<?> builder) {
        super(builder);
        PortableHookLoader loader = new PortableHookLoader(builder.portableFactories, builder.getClassLoader());
        portableContext = new PortableContextImpl(this, builder.portableVersion, builder.checkClassDefErrors);
        for (ClassDefinition cd : loader.getDefinitions()) {
            portableContext.registerClassDefinition(cd);
        }

        dataSerializerAdapter = createSerializerAdapter(new DataSerializableSerializer(
                builder.dataSerializableFactories, builder.getClassLoader()));
        portableSerializer = new PortableSerializer(portableContext, loader.getFactories());
        portableSerializerAdapter = createSerializerAdapter(portableSerializer);

        javaSerializerAdapter = createSerializerAdapter(
                new JavaSerializer(builder.enableSharedObject, builder.enableCompression, builder.classNameFilter));
        javaExternalizableAdapter = createSerializerAdapter(
                new JavaDefaultSerializers.ExternalizableSerializer(builder.enableCompression, builder.classNameFilter));
        registerConstantSerializers(builder.isCompatibility());
        registerJavaTypeSerializers(builder.isCompatibility());
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

    public InternalGenericRecord readAsInternalGenericRecord(Data data) throws IOException {
        if (data.isPortable()) {
            BufferObjectDataInput in = createObjectDataInput(data);
            return portableSerializer.readAsInternalGenericRecord(in);
        }
        if (data.isCompact()) {
            return compactStreamSerializer.readAsInternalGenericRecord(createObjectDataInput(data));
        }
        throw new IllegalArgumentException("Given type does not support query over data, type id " + data.getType());
    }

    @Override
    public Schema extractSchemaFromData(@Nonnull Data data) throws IOException {
        if (!data.isCompact()) {
            throw new IllegalArgumentException("Can not extract schema from given data type " + data.getType());
        }
        return compactStreamSerializer.extractSchema(createObjectDataInput(data));
    }

    @Override
    public Schema extractSchemaFromObject(@Nonnull Object object) {
        if (!isCompactSerializable(object)) {
            throw new IllegalArgumentException("Can not extract schema from given class " + object.getClass());
        }
        return compactStreamSerializer.extractSchema(object);
    }

    public PortableContext getPortableContext() {
        return portableContext;
    }

    private void registerConstantSerializers(boolean isCompatibility) {
        registerConstant(nullSerializerAdapter);
        if (!isCompatibility) {
            registerConstant(compactSerializerAdapter);
            registerConstant(compactWithSchemaSerializerAdapter);
        }
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
        if (!isCompatibility) {
            // compatibility (3.x) members didn't have these serializers
            registerConstant(UUID.class, new UuidSerializer());
            registerConstant(AbstractMap.SimpleEntry.class, new SimpleEntrySerializer());
            registerConstant(AbstractMap.SimpleImmutableEntry.class, new ConstantSerializers.SimpleImmutableEntrySerializer());
        }

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

    private void registerJavaTypeSerializers(boolean isCompatibility) {
        //Java extensions: more serializers
        registerConstant(Class.class, new ClassSerializer(isCompatibility));
        if (!isCompatibility) {
            // compatibility (3.x) members didn't have this serializer
            registerConstant(Optional.class, new OptionalSerializer());
        }
        registerConstant(Date.class, new DateSerializer(isCompatibility));
        registerConstant(BigInteger.class, new BigIntegerSerializer(isCompatibility));
        registerConstant(BigDecimal.class, new BigDecimalSerializer(isCompatibility));

        if (!isCompatibility) {
            // compatibility (3.x) members didn't have this serializer
            registerConstant(Object[].class, new ArrayStreamSerializer());
        }

        registerConstant(ArrayList.class, new ArrayListStreamSerializer(isCompatibility));
        registerConstant(LinkedList.class, new LinkedListStreamSerializer(isCompatibility));
        if (!isCompatibility) {
            // compatibility (3.x) members didn't have these serializers
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
            registerConstant(PriorityQueue.class, new PriorityQueueStreamSerializer());
            registerConstant(DelayQueue.class, new DelayQueueStreamSerializer());
            registerConstant(SynchronousQueue.class, new SynchronousQueueStreamSerializer());
            registerConstant(LinkedTransferQueue.class, new LinkedTransferQueueStreamSerializer());

            registerConstant(LocalTime.class, new LocalTimeSerializer());
            registerConstant(LocalDate.class, new LocalDateSerializer());
            registerConstant(LocalDateTime.class, new LocalDateTimeSerializer());
            registerConstant(OffsetDateTime.class, new OffsetDateTimeSerializer());
        }

        if (isCompatibility) {
            // compatibility (3.x) members have these serializers
            registerConstant(Enum.class, new EnumSerializer());
        }
        safeRegister(Serializable.class, javaSerializerAdapter);
        safeRegister(Externalizable.class, javaExternalizableAdapter);
        safeRegister(HazelcastJsonValue.class, new HazelcastJsonValueSerializer());
    }

    public void registerClassDefinitions(Collection<ClassDefinition> classDefinitions) {
        Map<Integer, Map<Integer, ClassDefinition>> factoryMap = createHashMap(classDefinitions.size());
        for (ClassDefinition cd : classDefinitions) {
            int factoryId = cd.getFactoryId();
            Map<Integer, ClassDefinition> classDefMap = factoryMap.computeIfAbsent(factoryId, k -> new HashMap<>());
            int classId = cd.getClassId();
            if (classDefMap.containsKey(classId)) {
                throw new HazelcastSerializationException("Duplicate registration found for factory-id : "
                        + factoryId + ", class-id " + classId);
            }
            classDefMap.put(classId, cd);
        }
        for (ClassDefinition classDefinition : classDefinitions) {
            registerClassDefinition(classDefinition, factoryMap);
        }
    }

    private void registerClassDefinition(ClassDefinition cd, Map<Integer, Map<Integer, ClassDefinition>> factoryMap) {
        Set<String> fieldNames = cd.getFieldNames();
        for (String fieldName : fieldNames) {
            FieldDefinition fd = cd.getField(fieldName);
            if (fd.getType() == FieldType.PORTABLE || fd.getType() == FieldType.PORTABLE_ARRAY) {
                int factoryId = fd.getFactoryId();
                int classId = fd.getClassId();
                Map<Integer, ClassDefinition> classDefinitionMap = factoryMap.get(factoryId);
                if (classDefinitionMap != null) {
                    ClassDefinition nestedCd = classDefinitionMap.get(classId);
                    if (nestedCd != null) {
                        registerClassDefinition(nestedCd, factoryMap);
                        portableContext.registerClassDefinition(nestedCd);
                        continue;
                    }
                }
                if (portableContext.shouldCheckClassDefinitionErrors()) {
                    throw new HazelcastSerializationException("Could not find registered ClassDefinition for factory-id : "
                            + factoryId + ", class-id " + classId);
                }

            }
        }
        portableContext.registerClassDefinition(cd);
    }

    public final PortableSerializer getPortableSerializer() {
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
            input.readString();
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
        private boolean checkClassDefErrors;

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

        public final T withCheckClassDefErrors(boolean checkClassDefErrors) {
            this.checkClassDefErrors = checkClassDefErrors;
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
