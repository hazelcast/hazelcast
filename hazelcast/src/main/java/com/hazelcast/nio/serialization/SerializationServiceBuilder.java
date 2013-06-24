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
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.nio.ClassLoaderUtil;

import java.nio.ByteOrder;
import java.util.*;

/**
 * @mdogan 6/4/13
 */
public final class SerializationServiceBuilder {

    private ClassLoader classLoader;

    private SerializationConfig config;

    private int version = -1;

    private final Map<Integer, DataSerializableFactory> dataSerializableFactories = new HashMap<Integer, DataSerializableFactory>();

    private final Map<Integer, PortableFactory> portableFactories = new HashMap<Integer, PortableFactory>();

    private boolean checkClassDefErrors = true;

    private final Set<ClassDefinition> classDefinitions = new HashSet<ClassDefinition>();

    private ManagedContext managedContext;

    private boolean useNativeByteOrder = false;

    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

    private boolean enableCompression = false;

    private boolean enableSharedObject = false;

    private boolean allowUnsafe = false;

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
        final SerializationService ss = new SerializationServiceImpl(inputOutputFactory, version, classLoader, dataSerializableFactories,
                portableFactories, classDefinitions, checkClassDefErrors, managedContext, enableCompression, enableSharedObject);

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
                ss.registerGlobal(serializer);
            }

            final Collection<SerializerConfig> typeSerializers = config.getSerializerConfigs();
            for (SerializerConfig serializerConfig : typeSerializers) {
                Serializer serializer = serializerConfig.getImplementation();
                if (serializer == null) {
                    try {
                        serializer = ClassLoaderUtil.newInstance(classLoader, serializerConfig.getClassName());
                    } catch (Exception e) {
                        throw new HazelcastSerializationException(e);
                    }
                }
                Class typeClass = serializerConfig.getTypeClass();
                if (typeClass == null) {
                    try {
                        typeClass = ClassLoaderUtil.loadClass(classLoader, serializerConfig.getTypeClassName());
                    } catch (ClassNotFoundException e) {
                        throw new HazelcastSerializationException(e);
                    }
                }
                ss.register(typeClass, serializer);
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

    private static void addConfigDataSerializableFactories(final Map<Integer, DataSerializableFactory> dataSerializableFactories,
                                                           SerializationConfig config, ClassLoader cl) {

        for (Map.Entry<Integer, DataSerializableFactory> entry : config.getDataSerializableFactories().entrySet()) {
            if (entry.getKey() <= 0 ) {
                throw new IllegalArgumentException("DataSerializableFactory factoryId must be positive! -> " + entry.getValue());
            }
            if (dataSerializableFactories.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("DataSerializableFactory with factoryId '" + entry.getKey() + "' is already registered!");
            }
            dataSerializableFactories.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Integer, String> entry : config.getDataSerializableFactoryClasses().entrySet()) {
            if (entry.getKey() <= 0 ) {
                throw new IllegalArgumentException("DataSerializableFactory factoryId must be positive! -> " + entry.getValue());
            }
            if (dataSerializableFactories.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("DataSerializableFactory with factoryId '" + entry.getKey() + "' is already registered!");
            }
            DataSerializableFactory f;
            try {
                f = ClassLoaderUtil.newInstance(cl, entry.getValue());
            } catch (Exception e) {
                throw new HazelcastSerializationException(e);
            }
            dataSerializableFactories.put(entry.getKey(), f);
        }
    }

    private static void addConfigPortableFactories(final Map<Integer, PortableFactory> portableFactories,
                                                   SerializationConfig config, ClassLoader cl) {

        for (Map.Entry<Integer, PortableFactory> entry : config.getPortableFactories().entrySet()) {
            if (entry.getKey() <= 0 ) {
                throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> " + entry.getValue());
            }
            if (portableFactories.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("PortableFactory with factoryId '" + entry.getKey() + "' is already registered!");
            }
            portableFactories.put(entry.getKey(), entry.getValue());
        }

        final Map<Integer, String> portableFactoryClasses = config.getPortableFactoryClasses();
        for (Map.Entry<Integer, String> entry : portableFactoryClasses.entrySet()) {
            if (entry.getKey() <= 0 ) {
                throw new IllegalArgumentException("PortableFactory factoryId must be positive! -> " + entry.getValue());
            }
            if (portableFactories.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("PortableFactory with factoryId '" + entry.getKey() + "' is already registered!");
            }
            PortableFactory f;
            try {
                f = ClassLoaderUtil.newInstance(cl, entry.getValue());
            } catch (Exception e) {
                throw new HazelcastSerializationException(e);
            }
            portableFactories.put(entry.getKey(), f);
        }
    }
}
