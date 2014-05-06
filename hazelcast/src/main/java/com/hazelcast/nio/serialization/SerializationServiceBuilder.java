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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.ClassLoaderUtil;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;

public final class SerializationServiceBuilder {

    private ClassLoader classLoader;

    private SerializationConfig config;

    private int version = -1;

    private final Map<Integer, DataSerializableFactory> dataSerializableFactories =
            new HashMap<Integer, DataSerializableFactory>();

    private final Map<Integer, PortableFactory> portableFactories = new HashMap<Integer, PortableFactory>();

    private boolean checkClassDefErrors = true;

    private final Set<ClassDefinition> classDefinitions = new HashSet<ClassDefinition>();

    private ManagedContext managedContext;

    private boolean useNativeByteOrder;

    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

    private boolean enableCompression;

    private boolean enableSharedObject = true;

    private boolean allowUnsafe;

    private int initialOutputBufferSize = 4 * 1024;

    private PartitioningStrategy partitioningStrategy;

    private HazelcastInstance hazelcastInstance;

    public SerializationServiceBuilder setVersion(int version) {
        if (version < 0) {
            throw new IllegalArgumentException("Version cannot be negative!");
        }
        this.version = version;
        return this;
    }

    public SerializationServiceBuilder setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    public SerializationServiceBuilder setConfig(SerializationConfig config) {
        this.config = config;
        if (version < 0) {
            version = config.getPortableVersion();
        }
        checkClassDefErrors = config.isCheckClassDefErrors();
        useNativeByteOrder = config.isUseNativeByteOrder();
        byteOrder = config.getByteOrder();
        enableCompression = config.isEnableCompression();
        enableSharedObject = config.isEnableSharedObject();
        allowUnsafe = config.isAllowUnsafe();
        return this;
    }

    public SerializationServiceBuilder addDataSerializableFactory(int id, DataSerializableFactory factory) {
        dataSerializableFactories.put(id, factory);
        return this;
    }

    public SerializationServiceBuilder addPortableFactory(int id, PortableFactory factory) {
        portableFactories.put(id, factory);
        return this;
    }

    public SerializationServiceBuilder addClassDefinition(ClassDefinition cd) {
        classDefinitions.add(cd);
        return this;
    }

    public SerializationServiceBuilder setCheckClassDefErrors(boolean checkClassDefErrors) {
        this.checkClassDefErrors = checkClassDefErrors;
        return this;
    }

    public SerializationServiceBuilder setManagedContext(ManagedContext managedContext) {
        this.managedContext = managedContext;
        return this;
    }

    public SerializationServiceBuilder setUseNativeByteOrder(boolean useNativeByteOrder) {
        this.useNativeByteOrder = useNativeByteOrder;
        return this;
    }

    public SerializationServiceBuilder setByteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
        return this;
    }

    public SerializationServiceBuilder setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        return this;
    }

    public SerializationServiceBuilder setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
        return this;
    }

    public SerializationServiceBuilder setEnableSharedObject(boolean enableSharedObject) {
        this.enableSharedObject = enableSharedObject;
        return this;
    }

    public SerializationServiceBuilder setAllowUnsafe(boolean allowUnsafe) {
        this.allowUnsafe = allowUnsafe;
        return this;
    }

    public SerializationServiceBuilder setPartitioningStrategy(PartitioningStrategy partitionStrategy) {
        this.partitioningStrategy = partitionStrategy;
        return this;
    }

    public SerializationServiceBuilder setInitialOutputBufferSize(int initialOutputBufferSize) {
        if (initialOutputBufferSize <= 0) {
            throw new IllegalArgumentException("Initial buffer size must be positive!");
        }
        this.initialOutputBufferSize = initialOutputBufferSize;
        return this;
    }

    public SerializationService build() {
        if (version < 0) {
            version = 0;
        }
        if (config != null) {
            addConfigDataSerializableFactories(dataSerializableFactories, config, classLoader);
            addConfigPortableFactories(portableFactories, config, classLoader);
            classDefinitions.addAll(config.getClassDefinitions());
        }

        final InputOutputFactory inputOutputFactory = createInputOutputFactory();
        final SerializationServiceImpl ss = new SerializationServiceImpl(inputOutputFactory,
                version, classLoader, dataSerializableFactories,
                portableFactories, classDefinitions, checkClassDefErrors, managedContext, partitioningStrategy,
                initialOutputBufferSize, enableCompression, enableSharedObject);

        SerializerHookLoader serializerHookLoader = new SerializerHookLoader(config, classLoader);
        Map<Class, Object> serializers = serializerHookLoader.getSerializers();
        for (Map.Entry<Class, Object> entry : serializers.entrySet()) {
            Class serializationType = entry.getKey();
            Object value = entry.getValue();
            Serializer serializer;
            if (value instanceof SerializerHook) {
                serializer = ((SerializerHook) value).createSerializer();
            } else {
                serializer = (Serializer) value;
            }
            if (value instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) value).setHazelcastInstance(hazelcastInstance);
            }
            if (ClassLoaderUtil.isInternalType(value.getClass())) {
                ss.safeRegister(serializationType, serializer);
            } else {
                ss.register(serializationType, serializer);
            }
        }

        if (config != null) {
            if (config.getGlobalSerializerConfig() != null) {
                GlobalSerializerConfig globalSerializerConfig = config.getGlobalSerializerConfig();
                Serializer serializer = globalSerializerConfig.getImplementation();
                if (serializer == null) {
                    try {
                        serializer = ClassLoaderUtil.newInstance(classLoader, globalSerializerConfig.getClassName());
                    } catch (Exception e) {
                        throw new HazelcastSerializationException(e);
                    }
                }

                if (serializer instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) serializer).setHazelcastInstance(hazelcastInstance);
                }

                ss.registerGlobal(serializer);
            }
        }
        return ss;
    }

    private InputOutputFactory createInputOutputFactory() {
        if (byteOrder == null) {
            byteOrder = ByteOrder.BIG_ENDIAN;
        }
        if (useNativeByteOrder || byteOrder == ByteOrder.nativeOrder()) {
            byteOrder = ByteOrder.nativeOrder();
            if (allowUnsafe && UnsafeInputOutputFactory.unsafeAvailable()) {
                return new UnsafeInputOutputFactory();
            }
        }
        if (byteOrder == ByteOrder.BIG_ENDIAN) {
            return new ByteArrayInputOutputFactory();
        }
        return new ByteBufferInputOutputFactory(byteOrder);
    }

    private void addConfigDataSerializableFactories(final Map<Integer, DataSerializableFactory> dataSerializableFactories,
                                                    SerializationConfig config, ClassLoader cl) {

        for (Map.Entry<Integer, DataSerializableFactory> entry : config.getDataSerializableFactories().entrySet()) {
            Integer factoryId = entry.getKey();
            DataSerializableFactory factory = entry.getValue();
            if (factoryId <= 0) {
                throw new IllegalArgumentException("DataSerializableFactory factoryId must be positive! -> " + factory);
            }
            if (dataSerializableFactories.containsKey(factoryId)) {
                throw new IllegalArgumentException("DataSerializableFactory with factoryId '"
                        + factoryId + "' is already registered!");
            }
            dataSerializableFactories.put(factoryId, factory);
        }

        for (Map.Entry<Integer, String> entry : config.getDataSerializableFactoryClasses().entrySet()) {
            Integer factoryId = entry.getKey();
            String factoryClassName = entry.getValue();
            if (factoryId <= 0) {
                throw new IllegalArgumentException("DataSerializableFactory factoryId must be positive! -> "
                        + factoryClassName);
            }
            if (dataSerializableFactories.containsKey(factoryId)) {
                throw new IllegalArgumentException("DataSerializableFactory with factoryId '"
                        + factoryId + "' is already registered!");
            }
            DataSerializableFactory factory;
            try {
                factory = ClassLoaderUtil.newInstance(cl, factoryClassName);
            } catch (Exception e) {
                throw new HazelcastSerializationException(e);
            }

            dataSerializableFactories.put(factoryId, factory);
        }

        for (DataSerializableFactory f : dataSerializableFactories.values()) {
            if (f instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) f).setHazelcastInstance(hazelcastInstance);
            }
        }
    }

    private void addConfigPortableFactories(final Map<Integer, PortableFactory> portableFactories,
                                            SerializationConfig config, ClassLoader cl) {

        for (Map.Entry<Integer, PortableFactory> entry : config.getPortableFactories().entrySet()) {
            Integer factoryId = entry.getKey();
            PortableFactory factory = entry.getValue();
            if (factoryId <= 0) {
                throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> " + factory);
            }
            if (portableFactories.containsKey(factoryId)) {
                throw new IllegalArgumentException("PortableFactory with factoryId '" + factoryId + "' is already registered!");
            }
            portableFactories.put(factoryId, factory);
        }

        final Map<Integer, String> portableFactoryClasses = config.getPortableFactoryClasses();
        for (Map.Entry<Integer, String> entry : portableFactoryClasses.entrySet()) {
            Integer factoryId = entry.getKey();
            String factoryClassName = entry.getValue();
            if (factoryId <= 0) {
                throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> " + factoryClassName);
            }
            if (portableFactories.containsKey(factoryId)) {
                throw new IllegalArgumentException("PortableFactory with factoryId '" + factoryId + "' is already registered!");
            }
            PortableFactory factory;
            try {
                factory = ClassLoaderUtil.newInstance(cl, factoryClassName);
            } catch (Exception e) {
                throw new HazelcastSerializationException(e);
            }
            portableFactories.put(factoryId, factory);
        }

        for (PortableFactory f : portableFactories.values()) {
            if (f instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) f).setHazelcastInstance(hazelcastInstance);
            }
        }
    }
}
