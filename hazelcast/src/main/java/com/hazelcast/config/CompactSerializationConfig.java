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
        this.classNameToRegistryMap = new ConcurrentHashMap<>(compactSerializationConfig.classNameToRegistryMap);
        this.classToRegistryMap = new ConcurrentHashMap<>(compactSerializationConfig.classToRegistryMap);
    }

    public CompactSerializationConfig() {
        this.classNameToRegistryMap = new ConcurrentHashMap<>();
        this.classToRegistryMap = new ConcurrentHashMap<>();
    }


    /**
     * Registers the class to be serialized via compact serializer.
     * Overrides Portable, Identified, Java Serializable, or GlobalSerializer.
     * <p>
     * Type name is determined automatically from the class, which is its
     * fully qualified class name.
     * Field types are determined automatically from the class via reflection.
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz) {
        checkNotNull(clazz, "Class cannot be null");
        register0(clazz, clazz.getName(), null);
    }

    /**
     * Registers the class to be serialized via compact serializer.
     * Overrides Portable, Identified, Java Serializable, or GlobalSerializer.
     * <p>
     * Field types are determined automatically from the class via reflection.
     *
     * @param clazz Class to be serialized via compact serializer
     * @param typeName Type name of the class
     */
    public <T> void register(Class<T> clazz, String typeName) {
        checkNotNull(clazz, "Class cannot be null");
        checkNotNull(typeName, "Type name cannot be null");
        register0(clazz, typeName, null);
    }

    /**
     * Registers the class to be serialized via compact serializer.
     * Overrides Portable, Identified, Java Serializable, or GlobalSerializer.
     *
     * @param clazz Class to be serialized via compact serializer
     * @param typeName Type name of the class
     * @param explicitSerializer Serializer to be used for the given class
     */
    public <T> void register(Class<T> clazz, String typeName, CompactSerializer<T> explicitSerializer) {
        checkNotNull(clazz, "Class cannot be null");
        checkNotNull(typeName, "Type name cannot be null");
        checkNotNull(explicitSerializer, "Explicit serializer cannot be null");
        register0(clazz, typeName, explicitSerializer);
    }

    /**
     * Registers the class to be serialized via compact serializer.
     * Overrides Portable, Identified, Java Serializable, or GlobalSerializer.
     * <p>
     * Type name is determined automatically from the class, which is its
     * fully qualified class name.
     *
     * @param clazz Class to be serialized via compact serializer
     * @param explicitSerializer Serializer to be used for the given class
     */
    public <T> void register(Class<T> clazz, CompactSerializer<T> explicitSerializer) {
        checkNotNull(clazz, "Class cannot be null");
        checkNotNull(explicitSerializer, "Explicit serializer cannot be null");
        register0(clazz, clazz.getName(), explicitSerializer);
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

    private <T> void register0(Class<T> clazz, String typeName, CompactSerializer<T> explicitSerializer) {
        TriTuple<Class, String, CompactSerializer> registry = TriTuple.of(clazz, typeName, explicitSerializer);
        TriTuple<Class, String, CompactSerializer> oldRegistry = classNameToRegistryMap.putIfAbsent(typeName, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for the type name " + typeName);
        }
        oldRegistry = classToRegistryMap.putIfAbsent(clazz, registry);
        if (oldRegistry != null) {
            throw new InvalidConfigurationException("Already have a registry for class " + clazz);
        }
    }
}
