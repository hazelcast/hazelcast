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

package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.SerializationService;

import java.util.ServiceLoader;

/**
 * This interface is used to automatically register serializers from external
 * Hazelcast or user modules.
 * <p>
 * Both types of {@link Serializer}s are supported: {@link StreamSerializer} and
 * {@link ByteArraySerializer}. The serializers need to be registered using a file
 * named "com.hazelcast.SerializerHook" in META-INF/services. Those services files
 * are not registered using the standard Java 6+ {@link ServiceLoader}, but with a
 * Hazelcast version that is capable of working with multiple class loaders to
 * support JEE and OSGi environments.
 *
 * @param <T> the type of the serialized object
 */
public interface SerializerHook<T> {

    /**
     * Returns the actual class type of the serialized object
     */
    Class<T> getSerializationType();

    /**
     * Creates a new serializer for the serialization type
     */
    default Serializer createSerializer() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a new serializer for the serialization type
     *
     * @since 5.4
     */
    default Serializer createSerializer(SerializationService serializationService) {
        return createSerializer();
    }

    /**
     * Indicates if this serializer can be overridden by defining a custom
     * serializer in the configurations (via code or configuration file)
     */
    boolean isOverwritable();
}
