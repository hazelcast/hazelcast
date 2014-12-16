/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.UnsafeHelper;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultSerializationServiceBuilder implements SerializationServiceBuilder {

    private static final int DEFAULT_OUT_BUFFER_SIZE = 4 * 1024;

    protected ClassLoader classLoader;

    protected SerializationConfig config;

    protected int version = -1;

    protected final Map<Integer, DataSerializableFactory> dataSerializableFactories =
            new HashMap<Integer, DataSerializableFactory>();

    protected final Map<Integer, PortableFactory> portableFactories = new HashMap<Integer, PortableFactory>();

    protected boolean checkClassDefErrors = true;

    protected final Set<ClassDefinition> classDefinitions = new HashSet<ClassDefinition>();

    protected ManagedContext managedContext;

    protected boolean useNativeByteOrder;

    protected ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

    protected boolean enableCompression;

    protected boolean enableSharedObject;

    protected boolean allowUnsafe;

    protected int initialOutputBufferSize = DEFAULT_OUT_BUFFER_SIZE;

    protected PartitioningStrategy partitioningStrategy;

    protected HazelcastInstance hazelcastInstance;

    @Override
    public SerializationServiceBuilder setVersion(int version) {
        if (version < 0) {
            throw new IllegalArgumentException("Version cannot be negative!");
        }
        this.version = version;
        return this;
    }

    @Override
    public SerializationServiceBuilder setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    @Override
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

    @Override
    public SerializationServiceBuilder addDataSerializableFactory(int id, DataSerializableFactory factory) {
        dataSerializableFactories.put(id, factory);
        return this;
    }

    @Override
    public SerializationServiceBuilder addPortableFactory(int id, PortableFactory factory) {
        portableFactories.put(id, factory);
        return this;
    }

    @Override
    public SerializationServiceBuilder addClassDefinition(ClassDefinition cd) {
        classDefinitions.add(cd);
        return this;
    }

    @Override
    public SerializationServiceBuilder setCheckClassDefErrors(boolean checkClassDefErrors) {
        this.checkClassDefErrors = checkClassDefErrors;
        return this;
    }

    @Override
    public SerializationServiceBuilder setManagedContext(ManagedContext managedContext) {
        this.managedContext = managedContext;
        return this;
    }

    @Override
    public SerializationServiceBuilder setUseNativeByteOrder(boolean useNativeByteOrder) {
        this.useNativeByteOrder = useNativeByteOrder;
        return this;
    }

    @Override
    public SerializationServiceBuilder setByteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
        return this;
    }

    @Override
    public SerializationServiceBuilder setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        return this;
    }

    @Override
    public SerializationServiceBuilder setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
        return this;
    }

    @Override
    public SerializationServiceBuilder setEnableSharedObject(boolean enableSharedObject) {
        this.enableSharedObject = enableSharedObject;
        return this;
    }

    @Override
    public SerializationServiceBuilder setAllowUnsafe(boolean allowUnsafe) {
        this.allowUnsafe = allowUnsafe;
        return this;
    }

    @Override
    public SerializationServiceBuilder setPartitioningStrategy(PartitioningStrategy partitionStrategy) {
        this.partitioningStrategy = partitionStrategy;
        return this;
    }

    @Override
    public SerializationServiceBuilder setInitialOutputBufferSize(int initialOutputBufferSize) {
        if (initialOutputBufferSize <= 0) {
            throw new IllegalArgumentException("Initial buffer size must be positive!");
        }
        this.initialOutputBufferSize = initialOutputBufferSize;
        return this;
    }

    @Override
    public SerializationService build() {
        if (version < 0) {
            version = 0;
        }
        if (config != null) {
            addConfigDataSerializableFactories(dataSerializableFactories, config, classLoader);
            addConfigPortableFactories(portableFactories, config, classLoader);
            classDefinitions.addAll(config.getClassDefinitions());
        }

        InputOutputFactory inputOutputFactory = createInputOutputFactory();
        SerializationServiceImpl ss = createSerializationService(inputOutputFactory);

        registerSerializerHooks(ss);

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

    protected SerializationServiceImpl createSerializationService(InputOutputFactory inputOutputFactory) {
        return new SerializationServiceImpl(inputOutputFactory, version,
                    classLoader, dataSerializableFactories,
                    portableFactories, classDefinitions, checkClassDefErrors, managedContext, partitioningStrategy,
                    initialOutputBufferSize, enableCompression, enableSharedObject);
    }

    private void registerSerializerHooks(SerializationServiceImpl ss) {
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
    }

    protected InputOutputFactory createInputOutputFactory() {
        if (byteOrder == null) {
            byteOrder = ByteOrder.BIG_ENDIAN;
        }
        if (useNativeByteOrder || byteOrder == ByteOrder.nativeOrder()) {
            byteOrder = ByteOrder.nativeOrder();
            if (allowUnsafe && UnsafeHelper.UNSAFE_AVAILABLE) {
                return new UnsafeInputOutputFactory();
            }
        }
        return new ByteArrayInputOutputFactory(byteOrder);
    }

    private void addConfigDataSerializableFactories(Map<Integer, DataSerializableFactory> dataSerializableFactories,
                                                    SerializationConfig config, ClassLoader cl) {

        registerDataSerializableFactories(dataSerializableFactories, config);
        buildDataSerializableFactories(dataSerializableFactories, config, cl);

        for (DataSerializableFactory f : dataSerializableFactories.values()) {
            if (f instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) f).setHazelcastInstance(hazelcastInstance);
            }
        }
    }

    private void registerDataSerializableFactories(Map<Integer, DataSerializableFactory> dataSerializableFactories,
                                                   SerializationConfig config) {
        for (Map.Entry<Integer, DataSerializableFactory> entry : config.getDataSerializableFactories().entrySet()) {
            int factoryId = entry.getKey();
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
    }

    private void buildDataSerializableFactories(Map<Integer, DataSerializableFactory> dataSerializableFactories,
                                                SerializationConfig config, ClassLoader cl) {

        for (Map.Entry<Integer, String> entry : config.getDataSerializableFactoryClasses().entrySet()) {
            int factoryId = entry.getKey();
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
    }

    private void addConfigPortableFactories(final Map<Integer, PortableFactory> portableFactories,
                                            SerializationConfig config, ClassLoader cl) {

        registerPortableFactories(portableFactories, config);
        buildPortableFactories(portableFactories, config, cl);

        for (PortableFactory f : portableFactories.values()) {
            if (f instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) f).setHazelcastInstance(hazelcastInstance);
            }
        }
    }

    private void registerPortableFactories(Map<Integer, PortableFactory> portableFactories, SerializationConfig config) {
        for (Map.Entry<Integer, PortableFactory> entry : config.getPortableFactories().entrySet()) {
            int factoryId = entry.getKey();
            PortableFactory factory = entry.getValue();
            if (factoryId <= 0) {
                throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> " + factory);
            }
            if (portableFactories.containsKey(factoryId)) {
                throw new IllegalArgumentException("PortableFactory with factoryId '" + factoryId + "' is already registered!");
            }
            portableFactories.put(factoryId, factory);
        }
    }

    private void buildPortableFactories(Map<Integer, PortableFactory> portableFactories, SerializationConfig config,
                                        ClassLoader cl) {

        final Map<Integer, String> portableFactoryClasses = config.getPortableFactoryClasses();
        for (Map.Entry<Integer, String> entry : portableFactoryClasses.entrySet()) {
            int factoryId = entry.getKey();
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
    }
}
