/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.SerializerAdapter;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.partition.PartitioningStrategy;

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

    public DelegatingSerializationService(Map<Class<?>, ? extends Serializer> serializers,
                                          AbstractSerializationService delegate) {
        super(delegate);

        Map<Class<?>, SerializerAdapter> serializersByClass;
        Map<Integer, SerializerAdapter> serializersById;
        if (serializers.isEmpty()) {
            serializersByClass = emptyMap();
            serializersById = emptyMap();
        } else {
            serializersByClass = new HashMap<>();
            serializersById = new HashMap<>();
            serializers.forEach((clazz, serializer) -> {
                if (serializersById.containsKey(serializer.getTypeId())) {
                    Serializer registered = serializersById.get(serializer.getTypeId()).getImpl();
                    throw new IllegalStateException("Cannot register Serializer[" + serializer.getClass().getName()
                            + "] - " + registered.getClass().getName() + " has been already registered for type ID: " +
                            serializer.getTypeId());
                }

                SerializerAdapter serializerAdapter = createSerializerAdapter(serializer, this);
                serializersByClass.put(clazz, serializerAdapter);
                serializersById.put(serializerAdapter.getImpl().getTypeId(), serializerAdapter);
            });
        }
        this.serializersByClass = serializersByClass;
        this.serializersById = serializersById;

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
    public PortableReader createPortableReader(Data data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PortableContext getPortableContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SerializerAdapter serializerFor(Object object) {
        Class<?> clazz = object.getClass();
        SerializerAdapter serializer = serializersByClass.get(clazz);
        if (serializer == null) {
            serializer = delegate.serializerFor(object);
            if (serializer == null) {
                throw active ?
                        new HazelcastSerializationException("There is no suitable serializer for " + clazz) :
                        new HazelcastInstanceNotActiveException();
            }
        }
        return serializer;
    }

    @Override
    public SerializerAdapter serializerFor(int typeId) {
        SerializerAdapter serializer = serializersById.get(typeId);
        if (serializer == null) {
            serializer = delegate.serializerFor(typeId);
            if (serializer == null) {
                throw active ?
                        newHazelcastSerializationException(typeId) :
                        new HazelcastInstanceNotActiveException();
            }
        }
        return serializer;
    }

    @Override
    public void dispose() {
        active = false;
        for (SerializerAdapter serializer : serializersByClass.values()) {
            serializer.destroy();
        }
    }

    public static InternalSerializationService from(SerializationService serializationService,
                                                    Map<String, String> serializerConfigs) {
        ClassLoader classLoader = currentThread().getContextClassLoader();
        Map<Class<?>, ? extends Serializer> serializers = new HashMap<>();
        for (Entry<String, String> entry : serializerConfigs.entrySet()) {
            serializers.put(loadClass(classLoader, entry.getKey()), newInstance(classLoader, entry.getValue()));
        }
        return new DelegatingSerializationService(serializers, (AbstractSerializationService) serializationService);
    }

    private static HazelcastSerializationException newHazelcastSerializationException(int typeId) {
        return new HazelcastSerializationException("There is no suitable de-serializer for type " + typeId + ". "
                + "This exception is likely caused by differences in the serialization configuration between members "
                + "or between clients and members.");
    }
}
