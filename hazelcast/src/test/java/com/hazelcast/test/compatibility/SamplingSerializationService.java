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

package com.hazelcast.test.compatibility;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.CompactGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.test.TestEnvironment;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.newSetFromMap;

/**
 * Serialization service that intercepts and samples serialized objects.
 * Employed to gather samples of serialized objects used in member-to-member communication during test suite
 * execution with mock network.
 *
 * @see TestEnvironment#SAMPLE_SERIALIZED_OBJECTS
 */
public class SamplingSerializationService implements InternalSerializationService {

    static final ConcurrentMap<String, List<byte[]>> SERIALIZED_SAMPLES_PER_CLASS_NAME =
            new ConcurrentHashMap<String, List<byte[]>>(1000);
    // cache classes for which samples have already been captured
    static final Set<String> SAMPLED_CLASSES = newSetFromMap(new ConcurrentHashMap<String, Boolean>(1000));

    private static final int MAX_SERIALIZED_SAMPLES_PER_CLASS = 5;
    // utility strings to locate test classes commonly used as user objects
    private static final String DUMMY_CLASS_PREFIX = "Dummy";
    private static final String TEST_CLASS_SUFFIX = "Test";
    private static final String TEST_PACKAGE_INFIX = ".test";
    private static final String EXAMPLE_PACKAGE_PREFIX = "example.";
    private static final String JET_PACKAGE_PREFIX = "com.hazelcast.jet";
    private static final String SQL_PACKAGE_PREFIX = "com.hazelcast.sql";

    // Only a small subset of Jet classes requires backwards-compatibility. Jet doesn't provide client
    // compatibility nor does it support rolling upgrades.
    private static final Set<String> JET_BACKWARD_COMPATIBLE_CLASSES = Stream
            .of(JobRecord.class, JobSummary.class, JobConfig.class)
            .map(Class::getName)
            .collect(Collectors.toSet());

    protected final InternalSerializationService delegate;

    public SamplingSerializationService(InternalSerializationService delegate) {
        this.delegate = delegate;
    }

    @Override
    public <B extends Data> B toData(Object obj) {
        B data = delegate.toData(obj);
        sampleObject(obj, data == null ? null : data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B toDataWithSchema(Object obj) {
        B data = delegate.toDataWithSchema(obj);
        sampleObject(obj, data == null ? null : data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
        B data = delegate.toData(obj, strategy);
        sampleObject(obj, data == null ? null : data.toByteArray());
        return data;
    }

    @Override
    public void writeObject(ObjectDataOutput out, Object obj) {
        delegate.writeObject(out, obj);
    }

    @Override
    public <T> T readObject(ObjectDataInput in) {
        return (T) delegate.readObject(in);
    }

    @Override
    public <T> T readObject(ObjectDataInput in, Class aClass) {
        return (T) delegate.readObject(in, aClass);
    }

    @Override
    public <T> T toObject(Object data) {
        return (T) delegate.toObject(data);
    }

    @Override
    public <T> T toObject(Object data, Class klazz) {
        return (T) delegate.toObject(data, klazz);
    }

    @Override
    public byte[] toBytes(Object obj) {
        byte[] bytes = delegate.toBytes(obj);
        sampleObject(obj, bytes);
        return bytes;
    }

    @Override
    public byte[] toBytes(Object obj, int leftPadding, boolean insertPartitionHash) {
        byte[] bytes = delegate.toBytes(obj, leftPadding, insertPartitionHash);
        sampleObject(obj, bytes);
        return bytes;
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type) {
        return toData(obj);
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy) {
        return toData(obj, strategy);
    }

    @Override
    public <B extends Data> B convertData(Data data, DataType type) {
        return (B) data;
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(byte[] data) {
        return delegate.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(byte[] data, int offset) {
        return delegate.createObjectDataInput(data, offset);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(Data data) {
        return delegate.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput(int size) {
        return delegate.createObjectDataOutput(size);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput(int initialSize, int firstGrowthSize) {
        return delegate.createObjectDataOutput(initialSize, firstGrowthSize);
    }

    @Override
    public ManagedContext getManagedContext() {
        return delegate.getManagedContext();
    }

    @Override
    public HeapData trimSchema(Data data) {
        return delegate.trimSchema(data);
    }

    @Override
    public InternalGenericRecord readAsInternalGenericRecord(Data data) throws IOException {
        return delegate.readAsInternalGenericRecord(data);
    }

    @Override
    public Schema extractSchemaFromData(@Nonnull Data data) throws IOException {
        return delegate.extractSchemaFromData(data);
    }

    @Override
    public Schema extractSchemaFromObject(@Nonnull Object object) {
        return delegate.extractSchemaFromObject(object);
    }

    @Override
    public boolean isCompactSerializable(Object object) {
        return delegate.isCompactSerializable(object);
    }

    @Override
    public PortableContext getPortableContext() {
        return delegate.getPortableContext();
    }

    @Override
    public void disposeData(Data data) {
        delegate.disposeData(data);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput() {
        return delegate.createObjectDataOutput();
    }

    @Override
    public ByteOrder getByteOrder() {
        return delegate.getByteOrder();
    }

    @Override
    public byte getVersion() {
        return delegate.getVersion();
    }

    @Override
    public void dispose() {
        delegate.dispose();
    }

    // record the given object, then return it
    protected static <T> T sampleObject(T obj, byte[] serializedObject) {
        if (obj == null) {
            return null;
        }

        if (serializedObject != null && shouldAddSerializedSample(obj)) {
            addSerializedSample(obj, serializedObject);
        }
        return obj;
    }

    private static void addSerializedSample(Object obj, byte[] bytes) {
        String className = obj.getClass().getName();
        SERIALIZED_SAMPLES_PER_CLASS_NAME.putIfAbsent(className, new CopyOnWriteArrayList<byte[]>());
        List<byte[]> samples = SERIALIZED_SAMPLES_PER_CLASS_NAME.get(className);
        if (samples.size() < MAX_SERIALIZED_SAMPLES_PER_CLASS) {
            samples.add(bytes);
        }
    }

    private static boolean shouldAddSerializedSample(Object obj) {
        Class klass = obj.getClass();
        if (klass.isPrimitive()) {
            return false;
        }

        String className = klass.getName();

        if (obj instanceof CompactGenericRecord) {
            return false;
        }

        if (SAMPLED_CLASSES.contains(className)) {
            return false;
        }

        if (className.startsWith(JET_PACKAGE_PREFIX) && !JET_BACKWARD_COMPATIBLE_CLASSES.contains(className)) {
            return false;
        }

        if (className.startsWith(SQL_PACKAGE_PREFIX)) {
            return false;
        }

        if (isTestClass(className)) {
            return false;
        }

        if (obj instanceof Data) {
            return false;
        }

        List<byte[]> existingSamples = SERIALIZED_SAMPLES_PER_CLASS_NAME.get(className);
        if (existingSamples == null) {
            return true;
        }
        if (existingSamples.size() < MAX_SERIALIZED_SAMPLES_PER_CLASS) {
            return true;
        }
        SAMPLED_CLASSES.add(className);
        return false;
    }

    public static boolean isTestClass(String className) {
        if (className.contains(TEST_CLASS_SUFFIX) || className.contains(TEST_PACKAGE_INFIX)
                || className.contains(DUMMY_CLASS_PREFIX) || className.startsWith(EXAMPLE_PACKAGE_PREFIX)) {
            return true;
        }
        return false;
    }
}
