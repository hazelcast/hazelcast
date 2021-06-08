/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.nio.serialization.compact.CompactSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class CompactSerializationConfig {

    private final Map<String, TriTuple<Class, String, CompactSerializer>> classNameToRegistryMap;
    private final Map<Class, TriTuple<Class, String, CompactSerializer>> classToRegistryMap;

    public CompactSerializationConfig(CompactSerializationConfig compactSerializationConfig) {
        this.classNameToRegistryMap = compactSerializationConfig.classNameToRegistryMap;
        this.classToRegistryMap = compactSerializationConfig.classToRegistryMap;
    }

    public CompactSerializationConfig() {
        this.classNameToRegistryMap = new ConcurrentHashMap<>();
        this.classToRegistryMap = new ConcurrentHashMap<>();
    }


    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     * <p>
     * class name is determined automatically from clazz. It is full path including package by default
     * fields are determined automatically from class via reflection
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz) {
        TriTuple<Class, String, CompactSerializer> registry = TriTuple.of(clazz, clazz.getName(), null);
        TriTuple<Class, String, CompactSerializer> oldRegistry = classNameToRegistryMap.putIfAbsent(clazz.getName(), registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class name " + clazz.getName());
        }
        oldRegistry = classToRegistryMap.putIfAbsent(clazz, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class " + clazz);
        }
    }

    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     * <p>
     * fields are determined automatically from class via reflection
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz, String typeName) {
        checkNotNull(typeName, "typeName");
        TriTuple<Class, String, CompactSerializer> registry = TriTuple.of(clazz, typeName, null);
        TriTuple<Class, String, CompactSerializer> oldRegistry = classNameToRegistryMap.putIfAbsent(typeName, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class name " + clazz.getName());
        }
        oldRegistry = classToRegistryMap.putIfAbsent(clazz, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class " + clazz);
        }
    }

    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz, String typeName, CompactSerializer<T> explicitSerializer) {
        checkNotNull(typeName, "typeName");
        checkNotNull(explicitSerializer, "explicitSerializer");
        TriTuple<Class, String, CompactSerializer> registry = TriTuple.of(clazz, typeName, explicitSerializer);
        TriTuple<Class, String, CompactSerializer> oldRegistry = classNameToRegistryMap.putIfAbsent(typeName, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class name " + typeName);
        }
        oldRegistry = classToRegistryMap.putIfAbsent(clazz, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class " + clazz);
        }
    }

    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     * class name is determined automatically from clazz. It is full path including package by default
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz, CompactSerializer<T> explicitSerializer) {
        checkNotNull(explicitSerializer, "explicitSerializer");
        TriTuple<Class, String, CompactSerializer> registry = TriTuple.of(clazz, clazz.getName(), explicitSerializer);
        TriTuple<Class, String, CompactSerializer> oldRegistry = classNameToRegistryMap.putIfAbsent(clazz.getName(), registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class name " + clazz.getName());
        }
        oldRegistry = classToRegistryMap.putIfAbsent(clazz, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class " + clazz);
        }
    }

    public Map<String, TriTuple<Class, String, CompactSerializer>> getRegistries() {
        return Collections.unmodifiableMap(classNameToRegistryMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactSerializationConfig that = (CompactSerializationConfig) o;
        return Objects.equals(classNameToRegistryMap, that.classNameToRegistryMap)
                && Objects.equals(classToRegistryMap, that.classToRegistryMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classNameToRegistryMap, classToRegistryMap);
    }
}
