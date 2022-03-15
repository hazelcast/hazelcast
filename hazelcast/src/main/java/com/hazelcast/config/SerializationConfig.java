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

package com.hazelcast.config;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains the serialization configuration of {@link com.hazelcast.core.HazelcastInstance}.
 */
public class SerializationConfig {

    private int portableVersion;
    private final Map<Integer, String> dataSerializableFactoryClasses;
    private final Map<Integer, DataSerializableFactory> dataSerializableFactories;
    private final Map<Integer, String> portableFactoryClasses;
    private final Map<Integer, PortableFactory> portableFactories;
    private GlobalSerializerConfig globalSerializerConfig;
    private final Collection<SerializerConfig> serializerConfigs;
    private boolean checkClassDefErrors = true;
    private boolean useNativeByteOrder;
    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
    private boolean enableCompression;
    private boolean enableSharedObject = true;
    private boolean allowUnsafe;
    private final Set<ClassDefinition> classDefinitions;
    private JavaSerializationFilterConfig javaSerializationFilterConfig;
    private boolean allowOverrideDefaultSerializers;
    private CompactSerializationConfig compactSerializationConfig;

    public SerializationConfig() {
        dataSerializableFactoryClasses = new HashMap<>();
        dataSerializableFactories = new HashMap<>();
        portableFactoryClasses = new HashMap<>();
        portableFactories = new HashMap<>();
        serializerConfigs = new LinkedList<>();
        classDefinitions = new HashSet<>();
        compactSerializationConfig = new CompactSerializationConfig();
    }

    public SerializationConfig(SerializationConfig serializationConfig) {
        portableVersion = serializationConfig.portableVersion;
        dataSerializableFactoryClasses = new HashMap<>(serializationConfig.dataSerializableFactoryClasses);
        dataSerializableFactories = new HashMap<>(serializationConfig.dataSerializableFactories);
        portableFactoryClasses = new HashMap<>(serializationConfig.portableFactoryClasses);
        portableFactories = new HashMap<>(serializationConfig.portableFactories);
        globalSerializerConfig = serializationConfig.globalSerializerConfig == null
                ? null : new GlobalSerializerConfig(serializationConfig.globalSerializerConfig);
        serializerConfigs = new LinkedList<>();
        for (SerializerConfig serializerConfig : serializationConfig.serializerConfigs) {
            serializerConfigs.add(new SerializerConfig(serializerConfig));
        }
        checkClassDefErrors = serializationConfig.checkClassDefErrors;
        useNativeByteOrder = serializationConfig.useNativeByteOrder;
        byteOrder = serializationConfig.byteOrder;
        enableCompression = serializationConfig.enableCompression;
        enableSharedObject = serializationConfig.enableSharedObject;
        allowUnsafe = serializationConfig.allowUnsafe;
        classDefinitions = new HashSet<>(serializationConfig.classDefinitions);
        javaSerializationFilterConfig = serializationConfig.javaSerializationFilterConfig == null
                ? null : new JavaSerializationFilterConfig(serializationConfig.javaSerializationFilterConfig);
        compactSerializationConfig = new CompactSerializationConfig(serializationConfig.compactSerializationConfig);
    }

    /**
     * @return the global serializer configuration
     * @see com.hazelcast.config.GlobalSerializerConfig
     */
    public GlobalSerializerConfig getGlobalSerializerConfig() {
        return globalSerializerConfig;
    }

    /**
     * @param globalSerializerConfig configuration of serializer that will be used
     *                               if no other serializer is applicable
     * @return GlobalSerializerConfig
     */
    public SerializationConfig setGlobalSerializerConfig(GlobalSerializerConfig globalSerializerConfig) {
        this.globalSerializerConfig = globalSerializerConfig;
        return this;
    }

    /**
     * @return list of {@link com.hazelcast.config.SerializerConfig}s
     */
    public Collection<SerializerConfig> getSerializerConfigs() {
        return serializerConfigs;
    }

    /**
     * @param serializerConfig serializer configuration of a class type
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig addSerializerConfig(SerializerConfig serializerConfig) {
        getSerializerConfigs().add(serializerConfig);
        return this;
    }

    /**
     * @param serializerConfigs lists of serializer configs that will be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setSerializerConfigs(Collection<SerializerConfig> serializerConfigs) {
        isNotNull(serializerConfigs, "serializerConfigs");
        this.serializerConfigs.clear();
        this.serializerConfigs.addAll(serializerConfigs);
        return this;
    }

    /**
     * Portable version will be used to differentiate two versions of the same class that have changes on the class,
     * like adding/removing a field or changing a type of a field.
     *
     * @return version of portable classes
     */
    public int getPortableVersion() {
        return portableVersion;
    }

    /**
     * @param portableVersion int value for the version of portable classes
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setPortableVersion(int portableVersion) {
        if (portableVersion < 0) {
            throw new IllegalArgumentException("Portable version cannot be negative!");
        }
        this.portableVersion = portableVersion;
        return this;
    }

    /**
     * @return map of factory ID and corresponding factory class names
     * @see com.hazelcast.nio.serialization.DataSerializableFactory
     */
    public Map<Integer, String> getDataSerializableFactoryClasses() {
        return dataSerializableFactoryClasses;
    }

    /**
     * @param dataSerializableFactoryClasses map of factory ID and corresponding factory class names
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.DataSerializableFactory
     */
    public SerializationConfig setDataSerializableFactoryClasses(Map<Integer, String> dataSerializableFactoryClasses) {
        isNotNull(dataSerializableFactoryClasses, "dataSerializableFactoryClasses");
        this.dataSerializableFactoryClasses.clear();
        this.dataSerializableFactoryClasses.putAll(dataSerializableFactoryClasses);
        return this;
    }

    /**
     * @param factoryId                    factory ID of dataSerializableFactory to be registered
     * @param dataSerializableFactoryClass name of dataSerializableFactory class to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.DataSerializableFactory
     */
    public SerializationConfig addDataSerializableFactoryClass(int factoryId, String dataSerializableFactoryClass) {
        getDataSerializableFactoryClasses().put(factoryId, dataSerializableFactoryClass);
        return this;
    }

    /**
     * @param factoryId                    factory ID of dataSerializableFactory to be registered
     * @param dataSerializableFactoryClass dataSerializableFactory class to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.DataSerializableFactory
     */
    public SerializationConfig addDataSerializableFactoryClass(int factoryId, Class<?
            extends DataSerializableFactory> dataSerializableFactoryClass) {
        String factoryClassName = isNotNull(dataSerializableFactoryClass, "dataSerializableFactoryClass").getName();
        return addDataSerializableFactoryClass(factoryId, factoryClassName);
    }

    /**
     * @return map of factory ID and corresponding dataSerializable factories
     * @see com.hazelcast.nio.serialization.DataSerializableFactory
     */
    public Map<Integer, DataSerializableFactory> getDataSerializableFactories() {
        return dataSerializableFactories;
    }

    /**
     * @param dataSerializableFactories map of factory ID and corresponding dataSerializable objects
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.DataSerializableFactory
     */
    public SerializationConfig setDataSerializableFactories(Map<Integer, DataSerializableFactory> dataSerializableFactories) {
        isNotNull(dataSerializableFactories, "dataSerializableFactories");
        this.dataSerializableFactories.clear();
        this.dataSerializableFactories.putAll(dataSerializableFactories);
        return this;
    }

    /**
     * @param factoryId               factory ID of DataSerializableFactory to be registered
     * @param dataSerializableFactory DataSerializableFactory object to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.DataSerializableFactory
     */
    public SerializationConfig addDataSerializableFactory(int factoryId, DataSerializableFactory dataSerializableFactory) {
        getDataSerializableFactories().put(factoryId, dataSerializableFactory);
        return this;
    }

    /**
     * @return map of factory ID and corresponding portable factory names
     * @see com.hazelcast.nio.serialization.PortableFactory
     */
    public Map<Integer, String> getPortableFactoryClasses() {
        return portableFactoryClasses;
    }

    /**
     * @param portableFactoryClasses map of factory ID and corresponding factory class names
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.PortableFactory
     */
    public SerializationConfig setPortableFactoryClasses(Map<Integer, String> portableFactoryClasses) {
        isNotNull(portableFactoryClasses, "portableFactoryClasses");
        this.portableFactoryClasses.clear();
        this.portableFactoryClasses.putAll(portableFactoryClasses);
        return this;
    }

    /**
     * @param factoryId            factory ID of portableFactory to be registered
     * @param portableFactoryClass portableFactory class to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.PortableFactory
     */
    public SerializationConfig addPortableFactoryClass(int factoryId, Class<? extends PortableFactory> portableFactoryClass) {
        String portableFactoryClassName = isNotNull(portableFactoryClass, "portableFactoryClass").getName();
        return addPortableFactoryClass(factoryId, portableFactoryClassName);
    }

    /**
     * @param factoryId            factory ID of portableFactory to be registered
     * @param portableFactoryClass name of the portableFactory class to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.PortableFactory
     */
    public SerializationConfig addPortableFactoryClass(int factoryId, String portableFactoryClass) {
        getPortableFactoryClasses().put(factoryId, portableFactoryClass);
        return this;
    }

    /**
     * @return map of factory ID and corresponding portable factories
     * @see com.hazelcast.nio.serialization.PortableFactory
     */
    public Map<Integer, PortableFactory> getPortableFactories() {
        return portableFactories;
    }

    /**
     * @param portableFactories map of factory ID and corresponding factory objects
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.PortableFactory
     */
    public SerializationConfig setPortableFactories(Map<Integer, PortableFactory> portableFactories) {
        isNotNull(portableFactories, "portableFactories");
        this.portableFactories.clear();
        this.portableFactories.putAll(portableFactories);
        return this;
    }

    /**
     * @param factoryId       factory ID of portableFactory to be registered
     * @param portableFactory portableFactory object to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see com.hazelcast.nio.serialization.PortableFactory
     */
    public SerializationConfig addPortableFactory(int factoryId, PortableFactory portableFactory) {
        getPortableFactories().put(factoryId, portableFactory);
        return this;
    }

    /**
     * @return registered class definitions of portable classes
     * @see ClassDefinition
     */
    public Set<ClassDefinition> getClassDefinitions() {
        return classDefinitions;
    }

    /**
     * @param classDefinition the class definition to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see ClassDefinition
     */
    public SerializationConfig addClassDefinition(ClassDefinition classDefinition) {
        if (!getClassDefinitions().add(classDefinition)) {
            throw new IllegalArgumentException("ClassDefinition for class-id[" + classDefinition.getClassId()
                    + "] already exists!");
        }
        return this;
    }

    /**
     * @param classDefinitions set of class definitions to be registered
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @see ClassDefinition
     */
    public SerializationConfig setClassDefinitions(Set<ClassDefinition> classDefinitions) {
        isNotNull(classDefinitions, "classDefinitions");
        this.classDefinitions.clear();
        this.classDefinitions.addAll(classDefinitions);
        return this;
    }

    /**
     * Default value is {@code true} (enabled).
     * When enabled, serialization system will check for class definitions error at start and throw an Serialization
     * Exception with error definition.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isCheckClassDefErrors() {
        return checkClassDefErrors;
    }

    /**
     * When enabled, serialization system will check for class definitions error at start and throw an Serialization
     * Exception with error definition.
     *
     * @param checkClassDefErrors set to {@code false} to disable
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setCheckClassDefErrors(boolean checkClassDefErrors) {
        this.checkClassDefErrors = checkClassDefErrors;
        return this;
    }

    /**
     * @return {@code true} if serialization is configured to use native byte order of the underlying platform
     */
    public boolean isUseNativeByteOrder() {
        return useNativeByteOrder;
    }

    /**
     * @param useNativeByteOrder set to {@code true} to use native byte order of the underlying platform
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setUseNativeByteOrder(boolean useNativeByteOrder) {
        this.useNativeByteOrder = useNativeByteOrder;
        return this;
    }

    /**
     * Note that result of useNativeByteOrder is not reflected to return value of this method.
     *
     * @return configured byte order
     */
    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    /**
     * Note that configuring use native byte order as enabled will override the byte order set by this method.
     *
     * @param byteOrder that serialization will use
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setByteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
        return this;
    }

    /**
     * Enables compression when default java serialization is used.
     *
     * @return {@code true} if compression enabled
     */
    public boolean isEnableCompression() {
        return enableCompression;
    }

    /**
     * Enables compression when default java serialization is used.
     *
     * @param enableCompression set to {@code true} to enable compression
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
        return this;
    }

    /**
     * Enables shared object when default java serialization is used.
     * <p>
     * Default value is {@code true}.
     *
     * @return {@code true} if enabled
     */
    public boolean isEnableSharedObject() {
        return enableSharedObject;
    }

    /**
     * Enables shared object when default java serialization is used.
     *
     * @param enableSharedObject set to {@code false} to disable
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setEnableSharedObject(boolean enableSharedObject) {
        this.enableSharedObject = enableSharedObject;
        return this;
    }

    /**
     * Unsafe, it is not public api of java. Use with caution!
     * <p>
     * Default value is {@code false}.
     *
     * @return {@code true} if using unsafe is allowed, {@code false} otherwise
     */
    public boolean isAllowUnsafe() {
        return allowUnsafe;
    }

    /**
     * Unsafe, it is not public api of java. Use with caution!
     *
     * @param allowUnsafe set to {@code true} to allow usage of unsafe
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     */
    public SerializationConfig setAllowUnsafe(boolean allowUnsafe) {
        this.allowUnsafe = allowUnsafe;
        return this;
    }

    /**
     * @return {@code true} if default serializers may be overridden by custom serializers
     * @since 4.2
     */
    public boolean isAllowOverrideDefaultSerializers() {
        return allowOverrideDefaultSerializers;
    }

    /**
     * This configuration should be used cautiously.
     * <p>
     * Default serializers are used heavily by the hazelcast internally.
     * If any of the instance in the same cluster overrides a default serializer,
     * all the members and clients in the same cluster must override it with the same serializer.
     * </p>
     *
     * <p>
     * This configuration is introduced specifically to support the following case:
     * <ul>
     *   <li>There was a custom configuration by the user for a type</li>
     *   <li>Hazelcast decided to add a default serializer for the same type in a future release</li>
     * </ul>
     * </p>
     * <p>
     * To be able to support Rolling Upgrade from an old version to the new version, one needs to make sure
     * of the backward compatibility of the serialization. As a solution, the user will set this property
     * on the new version so that new instances will be able to override new default serializers
     * with their existing custom serializers.
     *
     * @param allowOverrideDefaultSerializers value to set
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @since 4.2
     */
    public SerializationConfig setAllowOverrideDefaultSerializers(final boolean allowOverrideDefaultSerializers) {
        this.allowOverrideDefaultSerializers = allowOverrideDefaultSerializers;
        return this;
    }

    /**
     * @return the javaSerializationFilterConfig
     */
    public JavaSerializationFilterConfig getJavaSerializationFilterConfig() {
        return javaSerializationFilterConfig;
    }

    /**
     * Allows to configure deserialization protection filter.
     *
     * @param javaSerializationFilterConfig the javaSerializationFilterConfig to set (may be {@code null})
     */
    public SerializationConfig setJavaSerializationFilterConfig(JavaSerializationFilterConfig javaSerializationFilterConfig) {
        this.javaSerializationFilterConfig = javaSerializationFilterConfig;
        return this;
    }

    /**
     * @param compactSerializationConfig config for the compact serialization format
     * @return configured {@link com.hazelcast.config.SerializerConfig} for chaining
     * @since 5.0
     */
    @Beta
    public SerializationConfig setCompactSerializationConfig(@Nonnull CompactSerializationConfig compactSerializationConfig) {
        checkNotNull(compactSerializationConfig, "compactSerializationConfig");
        this.compactSerializationConfig = compactSerializationConfig;
        return this;
    }

    /**
     * @return compact serialization config
     * @since 5.0
     */
    @Beta
    public CompactSerializationConfig getCompactSerializationConfig() {
        return compactSerializationConfig;
    }

    @Override
    public String toString() {
        return "SerializationConfig{"
                + "portableVersion=" + portableVersion
                + ", dataSerializableFactoryClasses=" + dataSerializableFactoryClasses
                + ", dataSerializableFactories=" + dataSerializableFactories
                + ", portableFactoryClasses=" + portableFactoryClasses
                + ", portableFactories=" + portableFactories
                + ", globalSerializerConfig=" + globalSerializerConfig
                + ", serializerConfigs=" + serializerConfigs
                + ", checkClassDefErrors=" + checkClassDefErrors
                + ", classDefinitions=" + classDefinitions
                + ", byteOrder=" + byteOrder
                + ", useNativeByteOrder=" + useNativeByteOrder
                + ", javaSerializationFilterConfig=" + javaSerializationFilterConfig
                + ", allowOverrideDefaultSerializers=" + allowOverrideDefaultSerializers
                + '}';
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SerializationConfig that = (SerializationConfig) o;

        return portableVersion == that.portableVersion
                && checkClassDefErrors == that.checkClassDefErrors
                && useNativeByteOrder == that.useNativeByteOrder
                && enableCompression == that.enableCompression
                && enableSharedObject == that.enableSharedObject
                && allowUnsafe == that.allowUnsafe
                && allowOverrideDefaultSerializers == that.allowOverrideDefaultSerializers
                && dataSerializableFactoryClasses.equals(that.dataSerializableFactoryClasses)
                && dataSerializableFactories.equals(that.dataSerializableFactories)
                && portableFactoryClasses.equals(that.portableFactoryClasses)
                && portableFactories.equals(that.portableFactories)
                && Objects.equals(globalSerializerConfig, that.globalSerializerConfig)
                && serializerConfigs.equals(that.serializerConfigs)
                && Objects.equals(byteOrder, that.byteOrder)
                && classDefinitions.equals(that.classDefinitions)
                && Objects.equals(javaSerializationFilterConfig, that.javaSerializationFilterConfig)
                && Objects.equals(compactSerializationConfig, that.compactSerializationConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(portableVersion, dataSerializableFactoryClasses, dataSerializableFactories, portableFactoryClasses,
                portableFactories, globalSerializerConfig, serializerConfigs, checkClassDefErrors, useNativeByteOrder, byteOrder,
                enableCompression, enableSharedObject, allowUnsafe, allowOverrideDefaultSerializers,
                classDefinitions, javaSerializationFilterConfig, compactSerializationConfig);
    }


}
