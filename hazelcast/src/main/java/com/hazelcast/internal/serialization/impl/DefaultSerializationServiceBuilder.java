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

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationClassNameFilter;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolFactoryImpl;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassNameFilter;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.spi.properties.ClusterProperty;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;

public class DefaultSerializationServiceBuilder implements SerializationServiceBuilder {

    static final ByteOrder DEFAULT_BYTE_ORDER = BIG_ENDIAN;
    // System property to override configured byte order for tests
    private static final String BYTE_ORDER_OVERRIDE_PROPERTY = "hazelcast.serialization.byteOrder";
    private static final int DEFAULT_OUT_BUFFER_SIZE = 4 * 1024;
    protected final Map<Integer, DataSerializableFactory> dataSerializableFactories = new HashMap<>();
    protected final Map<Integer, PortableFactory> portableFactories = new HashMap<>();
    protected final Set<ClassDefinition> classDefinitions = new HashSet<>();
    protected ClassLoader classLoader;
    protected SerializationConfig config;
    protected byte version = -1;
    protected int portableVersion = -1;
    protected boolean checkClassDefErrors = true;
    protected ManagedContext managedContext;
    protected boolean useNativeByteOrder;
    protected ByteOrder byteOrder = DEFAULT_BYTE_ORDER;
    protected boolean enableCompression;
    protected boolean enableSharedObject;
    protected boolean allowUnsafe;
    protected boolean allowOverrideDefaultSerializers;
    protected int initialOutputBufferSize = DEFAULT_OUT_BUFFER_SIZE;
    protected PartitioningStrategy partitioningStrategy;
    protected HazelcastInstance hazelcastInstance;
    protected CompactSerializationConfig compactSerializationConfig;
    protected Supplier<RuntimeException> notActiveExceptionSupplier;
    protected ClassNameFilter classNameFilter;
    protected SchemaService schemaService;
    protected boolean isCompatibility;

    @Override
    public SerializationServiceBuilder setVersion(byte version) {
        byte maxVersion = BuildInfoProvider.getBuildInfo().getSerializationVersion();
        if (version > maxVersion) {
            throw new IllegalArgumentException(
                    "Configured serialization version is higher than the max supported version: " + maxVersion);
        }
        this.version = version;
        return this;
    }

    @Override
    public SerializationServiceBuilder setPortableVersion(int portableVersion) {
        if (portableVersion < 0) {
            throw new IllegalArgumentException("Portable Version cannot be negative!");
        }
        this.portableVersion = portableVersion;
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
        if (portableVersion < 0) {
            portableVersion = config.getPortableVersion();
        }
        checkClassDefErrors = config.isCheckClassDefErrors();
        useNativeByteOrder = config.isUseNativeByteOrder();
        byteOrder = config.getByteOrder();
        enableCompression = config.isEnableCompression();
        enableSharedObject = config.isEnableSharedObject();
        allowUnsafe = config.isAllowUnsafe();
        allowOverrideDefaultSerializers = config.isAllowOverrideDefaultSerializers();
        JavaSerializationFilterConfig filterConfig = config.getJavaSerializationFilterConfig();
        classNameFilter = filterConfig == null ? null : new SerializationClassNameFilter(filterConfig);
        compactSerializationConfig = config.getCompactSerializationConfig();
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
    public SerializationServiceBuilder setNotActiveExceptionSupplier(Supplier<RuntimeException> notActiveExceptionSupplier) {
        this.notActiveExceptionSupplier = notActiveExceptionSupplier;
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
    public SerializationServiceBuilder setSchemaService(SchemaService schemaService) {
        this.schemaService = schemaService;
        return this;
    }

    @Override
    public SerializationServiceBuilder isCompatibility(boolean isCompatibility) {
        this.isCompatibility = isCompatibility;
        return this;
    }

    @Override
    public InternalSerializationService build() {
        initVersions();
        if (config != null) {
            addConfigDataSerializableFactories(dataSerializableFactories, config, classLoader);
            addConfigPortableFactories(portableFactories, config, classLoader);
            classDefinitions.addAll(config.getClassDefinitions());
        }

        InputOutputFactory inputOutputFactory = createInputOutputFactory();
        InternalSerializationService ss = createSerializationService(inputOutputFactory, notActiveExceptionSupplier);

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

                ((AbstractSerializationService) ss)
                        .registerGlobal(serializer, globalSerializerConfig.isOverrideJavaSerialization());
            }
        }
        return ss;
    }

    private void initVersions() {
        if (version < 0) {
            String defaultVal = ClusterProperty.SERIALIZATION_VERSION.getDefaultValue();
            byte versionCandidate = Byte.parseByte(
                    System.getProperty(ClusterProperty.SERIALIZATION_VERSION.getName(), defaultVal));
            byte maxVersion = Byte.parseByte(defaultVal);
            if (versionCandidate > maxVersion) {
                throw new IllegalArgumentException(
                        "Configured serialization version is higher than the max supported version: " + maxVersion);
            }
            version = versionCandidate;
        }
        if (portableVersion < 0) {
            portableVersion = 0;
        }
    }

    protected InternalSerializationService createSerializationService(InputOutputFactory inputOutputFactory,
                                                                      Supplier<RuntimeException> notActiveExceptionSupplier) {
        switch (version) {
            case 1:
                SerializationServiceV1 serializationServiceV1 = SerializationServiceV1.builder()
                    .withInputOutputFactory(inputOutputFactory)
                    .withVersion(version)
                    .withPortableVersion(portableVersion)
                    .withClassLoader(classLoader)
                    .withDataSerializableFactories(dataSerializableFactories)
                    .withPortableFactories(portableFactories)
                    .withManagedContext(managedContext)
                    .withGlobalPartitionStrategy(partitioningStrategy)
                    .withInitialOutputBufferSize(initialOutputBufferSize)
                    .withBufferPoolFactory(new BufferPoolFactoryImpl())
                    .withEnableCompression(enableCompression)
                    .withEnableSharedObject(enableSharedObject)
                    .withNotActiveExceptionSupplier(notActiveExceptionSupplier)
                    .withClassNameFilter(classNameFilter)
                    .withCheckClassDefErrors(checkClassDefErrors)
                    .withAllowOverrideDefaultSerializers(allowOverrideDefaultSerializers)
                    .withCompactSerializationConfig(compactSerializationConfig)
                    .withSchemaService(schemaService)
                    .withCompatibility(isCompatibility)
                    .build();
                serializationServiceV1.registerClassDefinitions(classDefinitions);
                return serializationServiceV1;

            // future version note: add new versions here by adding cases for each version and instantiate it properly
            default:
                throw new IllegalArgumentException("Serialization version is not supported!");
        }
    }

    private void registerSerializerHooks(InternalSerializationService ss) {
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
                ((AbstractSerializationService) ss).safeRegister(serializationType, serializer);
            } else {
                ((AbstractSerializationService) ss).register(serializationType, serializer);
            }
        }
    }

    protected InputOutputFactory createInputOutputFactory() {
        overrideByteOrder();

        if (byteOrder == null) {
            byteOrder = DEFAULT_BYTE_ORDER;
        }
        if (useNativeByteOrder) {
            byteOrder = nativeOrder();
        }
        return byteOrder == nativeOrder() && allowUnsafe && GlobalMemoryAccessorRegistry.MEM_AVAILABLE
                ? new UnsafeInputOutputFactory()
                : new ByteArrayInputOutputFactory(byteOrder);
    }

    protected void overrideByteOrder() {
        String byteOrderOverride = System.getProperty(BYTE_ORDER_OVERRIDE_PROPERTY);
        if (StringUtil.isNullOrEmpty(byteOrderOverride)) {
            return;
        }
        if (BIG_ENDIAN.toString().equals(byteOrderOverride)) {
            byteOrder = BIG_ENDIAN;
        } else if (LITTLE_ENDIAN.toString().equals(byteOrderOverride)) {
            byteOrder = LITTLE_ENDIAN;
        }
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
                throw new IllegalArgumentException(
                        "DataSerializableFactory with factoryId '" + factoryId + "' is already registered!");
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
                throw new IllegalArgumentException("DataSerializableFactory factoryId must be positive! -> " + factoryClassName);
            }
            if (dataSerializableFactories.containsKey(factoryId)) {
                throw new IllegalArgumentException(
                        "DataSerializableFactory with factoryId '" + factoryId + "' is already registered!");
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

    private void addConfigPortableFactories(Map<Integer, PortableFactory> portableFactories, SerializationConfig config,
                                            ClassLoader cl) {
        PortableFactoriesHelper.registerPortableFactories(portableFactories, config);
        PortableFactoriesHelper.buildPortableFactories(portableFactories, config, cl);

        for (PortableFactory f : portableFactories.values()) {
            if (f instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) f).setHazelcastInstance(hazelcastInstance);
            }
        }
    }

    private static class PortableFactoriesHelper {
        private static void registerPortableFactories(final Map<Integer, PortableFactory> portableFactories,
                                                      final SerializationConfig config) {
            for (final Map.Entry<Integer, PortableFactory> entry : config.getPortableFactories().entrySet()) {
                final int factoryId = entry.getKey();
                final PortableFactory factory = entry.getValue();
                if (factoryId <= 0) {
                    throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> " + factory);
                }
                if (portableFactories.containsKey(factoryId)) {
                    throw new IllegalArgumentException("PortableFactory with factoryId '"
                      + factoryId + "' is already registered!");
                }
                portableFactories.put(factoryId, factory);
            }
        }

        private static void buildPortableFactories(final Map<Integer, PortableFactory> portableFactories,
                                                   final SerializationConfig config, final ClassLoader cl) {
            final Map<Integer, String> portableFactoryClasses = config.getPortableFactoryClasses();
            for (final Map.Entry<Integer, String> entry : portableFactoryClasses.entrySet()) {
                int factoryId = entry.getKey();
                String factoryClassName = entry.getValue();
                if (factoryId <= 0) {
                    throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> "
                      + factoryClassName);
                }
                if (portableFactories.containsKey(factoryId)) {
                    throw new IllegalArgumentException("PortableFactory with factoryId '"
                      + factoryId + "' is already registered!");
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
}
