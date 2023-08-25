/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * An accessor for the package-private fields of the
 * {@link CompactSerializationConfig}.
 * <p>
 * This is intended to be used while registering serializers and Compact
 * serializable classes with declarative configuration. This kind of accessor is
 * necessary as the register methods on the {@link CompactSerializationConfig}
 * accepts concrete {@link Class} or
 * {@link com.hazelcast.nio.serialization.compact.CompactSerializer} instances
 * rather than the string representation of the fully qualified class names.
 * <p>
 * Also, it enables us to access registered classes using the programmatic API,
 * without providing a public API on the {@link CompactSerializationConfig}.
 */
@PrivateApi
public final class CompactSerializationConfigAccessor {

    private CompactSerializationConfigAccessor() {
    }

    /**
     * Registers an explicit compact serializer for the given class and type
     * name.
     */
    public static void registerSerializer(CompactSerializationConfig config, String serializerClassName) {
        checkNotNull(serializerClassName, "Explicit serializer class name cannot be null");
        config.serializerClassNames.add(serializerClassName);
    }

    /**
     * Registers a reflective compact serializer for the given class name. The
     * type name will be the same with the class name.
     */
    public static void registerClass(CompactSerializationConfig config, String className) {
        checkNotNull(className, "Class name cannot be null");
        config.compactSerializableClassNames.add(className);
    }

    /**
     * Returns the list of Compact serializer class names.
     */
    public static List<String> getSerializerClassNames(CompactSerializationConfig config) {
        return config.serializerClassNames;
    }

    /**
     * Returns the list of Compact serializable class names.
     */
    public static List<String> getCompactSerializableClassNames(CompactSerializationConfig config) {
        return config.compactSerializableClassNames;
    }

    /**
     * Returns the map of the type names to programmatic registrations.
     */
    public static Map<String, TriTuple<Class, String, CompactSerializer>> getRegistrations(
            CompactSerializationConfig config) {
        return config.typeNameToRegistration;
    }
}
