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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.SerializerAdapter;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.jet.JetException;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.partition.PartitioningStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createSerializerAdapter;
import static com.hazelcast.jet.impl.util.ReflectionUtils.loadClass;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyMap;

public class DelegatingSerializationService extends AbstractSerializationService {

    private final Map<Class<?>, SerializerAdapter> serializersByClass;
    private final Map<Integer, SerializerAdapter> serializersById;

    private final AbstractSerializationService delegate;

    private volatile boolean active;

    public DelegatingSerializationService(
            Map<Class<?>, ? extends Serializer> serializers,
            AbstractSerializationService delegate
    ) {
        super(delegate);

        if (serializers.isEmpty()) {
            this.serializersByClass = emptyMap();
            this.serializersById = emptyMap();
        } else {
            Map<Class<?>, SerializerAdapter> serializersByClass = new HashMap<>();
            Map<Integer, SerializerAdapter> serializersById = new HashMap<>();
            serializers.forEach((clazz, serializer) -> {
                int typeId = serializer.getTypeId();
                String serializerClassName = serializer.getClass().getName();

                if (typeId <= 0) {
                    throw new IllegalArgumentException("Cannot register Serializer[" + serializerClassName + "] - " +
                            "typeId should be > 0");
                }
                if (serializersById.containsKey(typeId)) {
                    Serializer registered = serializersById.get(typeId).getImpl();
                    throw new IllegalStateException("Cannot register Serializer[" + serializerClassName + "] - " +
                            registered.getClass().getName() + " has been already registered for type ID: " + typeId);
                }

                SerializerAdapter serializerAdapter = createSerializerAdapter(serializer);
                serializersByClass.put(clazz, serializerAdapter);
                serializersById.put(typeId, serializerAdapter);
            });
            this.serializersByClass = serializersByClass;
            this.serializersById = serializersById;
        }

        this.delegate = delegate;

        this.active = true;
    }

    @Override
    public <B extends Data> B toData(Object object, DataType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <B extends Data> B toData(Object object, DataType type, PartitioningStrategy strategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <B extends Data> B convertData(Data data, DataType dataType) {
        throw new UnsupportedOperationException();
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
    public PortableContext getPortableContext() {
        return delegate.getPortableContext();
    }

    @Override
    public SerializerAdapter serializerFor(Object object, boolean includeSchema) {
        Class<?> clazz = object == null ? null : object.getClass();

        SerializerAdapter serializer = null;
        if (clazz != null) {
            serializer = serializersByClass.get(clazz);
        }
        if (serializer == null) {
            try {
                serializer = delegate.serializerFor(object, includeSchema);
            } catch (HazelcastSerializationException hse) {
                throw serializationException(clazz, hse);
            }
        }
        if (serializer == null) {
            throw active ? serializationException(clazz) : new HazelcastInstanceNotActiveException();
        }
        return serializer;
    }

    private RuntimeException serializationException(@Nullable Class<?> clazz, Throwable t) {
        return new JetException("Unable to serialize instance of " + clazz + ": " +
                t.getMessage() + " - Note: You can register a serializer using JobConfig.registerSerializer()", t);
    }

    private RuntimeException serializationException(@Nullable Class<?> clazz) {
        return new JetException("There is no suitable serializer for " + clazz +
                ", did you register it with JobConfig.registerSerializer()?");
    }

    @Override
    public SerializerAdapter serializerFor(int typeId) {
        SerializerAdapter serializer = serializersById.get(typeId);
        if (serializer == null) {
            try {
                serializer = delegate.serializerFor(typeId);
            } catch (HazelcastSerializationException hse) {
                throw serializationException(typeId, hse);
            }
        }
        if (serializer == null) {
            throw active ? serializationException(typeId) : new HazelcastInstanceNotActiveException();
        }
        return serializer;
    }

    private RuntimeException serializationException(int typeId, Throwable t) {
        return new JetException("Unable to deserialize object for type " + typeId + ": " +
                t.getMessage(), t);
    }

    private RuntimeException serializationException(int typeId) {
        return new JetException("There is no suitable de-serializer for type " + typeId + ". "
                + "This exception is likely caused by differences in the serialization configuration between members "
                + "or between clients and members.");
    }

    public boolean hasAddedSerializers() {
        return !serializersByClass.isEmpty();
    }

    @Override
    public void dispose() {
        active = false;
        for (SerializerAdapter serializer : serializersByClass.values()) {
            serializer.destroy();
        }
    }

    public static InternalSerializationService from(
            SerializationService serializationService,
            Map<String, String> serializerConfigs
    ) {
        ClassLoader classLoader = currentThread().getContextClassLoader();
        Map<Class<?>, ? extends Serializer> serializers = new HashMap<>();
        for (Entry<String, String> entry : serializerConfigs.entrySet()) {
            serializers.put(loadClass(classLoader, entry.getKey()), newInstance(classLoader, entry.getValue()));
        }
        return new DelegatingSerializationService(serializers, (AbstractSerializationService) serializationService);
    }
}
