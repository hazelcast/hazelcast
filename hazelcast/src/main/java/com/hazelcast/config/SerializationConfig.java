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

package com.hazelcast.config;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.Collection;
import java.util.Set;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.HashSet;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains the serialization configuration a {@link com.hazelcast.core.HazelcastInstance}.
 */
public class SerializationConfig {

    private int portableVersion;

    private Map<Integer, String> dataSerializableFactoryClasses;

    private Map<Integer, DataSerializableFactory> dataSerializableFactories;

    private Map<Integer, String> portableFactoryClasses;

    private Map<Integer, PortableFactory> portableFactories;

    private GlobalSerializerConfig globalSerializerConfig;

    private Collection<SerializerConfig> serializerConfigs;

    private boolean checkClassDefErrors = true;

    private boolean useNativeByteOrder;

    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

    private boolean enableCompression;

    private boolean enableSharedObject = true;

    private boolean allowUnsafe;

    private Set<ClassDefinition> classDefinitions;

    public SerializationConfig() {
        super();
    }

    public GlobalSerializerConfig getGlobalSerializerConfig() {
        return globalSerializerConfig;
    }

    public SerializationConfig setGlobalSerializerConfig(GlobalSerializerConfig globalSerializerConfig) {
        this.globalSerializerConfig = globalSerializerConfig;
        return this;
    }

    public Collection<SerializerConfig> getSerializerConfigs() {
        if (serializerConfigs == null) {
            serializerConfigs = new LinkedList<SerializerConfig>();
        }
        return serializerConfigs;
    }

    public SerializationConfig addSerializerConfig(SerializerConfig serializerConfig) {
        getSerializerConfigs().add(serializerConfig);
        return this;
    }

    public SerializationConfig setSerializerConfigs(Collection<SerializerConfig> serializerConfigs) {
        this.serializerConfigs = serializerConfigs;
        return this;
    }

    public int getPortableVersion() {
        return portableVersion;
    }

    public SerializationConfig setPortableVersion(int portableVersion) {
        if (portableVersion < 0) {
            throw new IllegalArgumentException("Portable version cannot be negative!");
        }
        this.portableVersion = portableVersion;
        return this;
    }

    public Map<Integer, String> getDataSerializableFactoryClasses() {
        if (dataSerializableFactoryClasses == null) {
            dataSerializableFactoryClasses = new HashMap<Integer, String>();
        }
        return dataSerializableFactoryClasses;
    }

    public SerializationConfig setDataSerializableFactoryClasses(Map<Integer, String> dataSerializableFactoryClasses) {
        this.dataSerializableFactoryClasses = dataSerializableFactoryClasses;
        return this;
    }

    public SerializationConfig addDataSerializableFactoryClass(int factoryId, String dataSerializableFactoryClass) {
        getDataSerializableFactoryClasses().put(factoryId, dataSerializableFactoryClass);
        return this;
    }

    public SerializationConfig addDataSerializableFactoryClass(int factoryId, Class<?
            extends DataSerializableFactory> dataSerializableFactoryClass) {
        String factoryClassName = isNotNull(dataSerializableFactoryClass, "dataSerializableFactoryClass").getName();
        return addDataSerializableFactoryClass(factoryId, factoryClassName);
    }

    public Map<Integer, DataSerializableFactory> getDataSerializableFactories() {
        if (dataSerializableFactories == null) {
            dataSerializableFactories = new HashMap<Integer, DataSerializableFactory>();
        }
        return dataSerializableFactories;
    }

    public SerializationConfig setDataSerializableFactories(Map<Integer, DataSerializableFactory> dataSerializableFactories) {
        this.dataSerializableFactories = dataSerializableFactories;
        return this;
    }

    public SerializationConfig addDataSerializableFactory(int factoryId, DataSerializableFactory dataSerializableFactory) {
        getDataSerializableFactories().put(factoryId, dataSerializableFactory);
        return this;
    }

    public Map<Integer, String> getPortableFactoryClasses() {
        if (portableFactoryClasses == null) {
            portableFactoryClasses = new HashMap<Integer, String>();
        }
        return portableFactoryClasses;
    }

    public SerializationConfig setPortableFactoryClasses(Map<Integer, String> portableFactoryClasses) {
        this.portableFactoryClasses = portableFactoryClasses;
        return this;
    }

    public SerializationConfig addPortableFactoryClass(int factoryId, Class<? extends PortableFactory> portableFactoryClass) {
        String portableFactoryClassName = isNotNull(portableFactoryClass, "portableFactoryClass").getName();
        return addPortableFactoryClass(factoryId, portableFactoryClassName);
    }

    public SerializationConfig addPortableFactoryClass(int factoryId, String portableFactoryClass) {
        getPortableFactoryClasses().put(factoryId, portableFactoryClass);
        return this;
    }

    public Map<Integer, PortableFactory> getPortableFactories() {
        if (portableFactories == null) {
            portableFactories = new HashMap<Integer, PortableFactory>();
        }
        return portableFactories;
    }

    public SerializationConfig setPortableFactories(Map<Integer, PortableFactory> portableFactories) {
        this.portableFactories = portableFactories;
        return this;
    }

    public SerializationConfig addPortableFactory(int factoryId, PortableFactory portableFactory) {
        getPortableFactories().put(factoryId, portableFactory);
        return this;
    }

    public Set<ClassDefinition> getClassDefinitions() {
        if (classDefinitions == null) {
            classDefinitions = new HashSet<ClassDefinition>();
        }
        return classDefinitions;
    }

    public SerializationConfig addClassDefinition(ClassDefinition classDefinition) {
        if (!getClassDefinitions().add(classDefinition)) {
            throw new IllegalArgumentException("ClassDefinition for class-id[" + classDefinition.getClassId()
                    + "] already exists!");
        }
        return this;
    }

    public SerializationConfig setClassDefinitions(Set<ClassDefinition> classDefinitions) {
        this.classDefinitions = classDefinitions;
        return this;
    }

    public boolean isCheckClassDefErrors() {
        return checkClassDefErrors;
    }

    public SerializationConfig setCheckClassDefErrors(boolean checkClassDefErrors) {
        this.checkClassDefErrors = checkClassDefErrors;
        return this;
    }

    public boolean isUseNativeByteOrder() {
        return useNativeByteOrder;
    }

    public SerializationConfig setUseNativeByteOrder(boolean useNativeByteOrder) {
        this.useNativeByteOrder = useNativeByteOrder;
        return this;
    }

    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    public SerializationConfig setByteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
        return this;
    }

    public boolean isEnableCompression() {
        return enableCompression;
    }

    public SerializationConfig setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
        return this;
    }

    public boolean isEnableSharedObject() {
        return enableSharedObject;
    }

    public SerializationConfig setEnableSharedObject(boolean enableSharedObject) {
        this.enableSharedObject = enableSharedObject;
        return this;
    }

    public boolean isAllowUnsafe() {
        return allowUnsafe;
    }

    public SerializationConfig setAllowUnsafe(boolean allowUnsafe) {
        this.allowUnsafe = allowUnsafe;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SerializationConfig{");
        sb.append("portableVersion=").append(portableVersion);
        sb.append(", dataSerializableFactoryClasses=").append(dataSerializableFactoryClasses);
        sb.append(", dataSerializableFactories=").append(dataSerializableFactories);
        sb.append(", portableFactoryClasses=").append(portableFactoryClasses);
        sb.append(", portableFactories=").append(portableFactories);
        sb.append(", globalSerializerConfig=").append(globalSerializerConfig);
        sb.append(", serializerConfigs=").append(serializerConfigs);
        sb.append(", checkClassDefErrors=").append(checkClassDefErrors);
        sb.append(", classDefinitions=").append(classDefinitions);
        sb.append(", byteOrder=").append(byteOrder);
        sb.append(", useNativeByteOrder=").append(useNativeByteOrder);
        sb.append('}');
        return sb.toString();
    }
}
