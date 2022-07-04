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

package com.hazelcast.nio.serialization;

/**
 * This interface is used to automatically register serializers from external
 * Hazelcast or user modules.<br>
 * Both types of {@link com.hazelcast.nio.serialization.Serializer}s are supported
 * ({@link com.hazelcast.nio.serialization.StreamSerializer} and
 * {@link com.hazelcast.nio.serialization.ByteArraySerializer}).
 * It needs to be registered using a file called "com.hazelcast.SerializerHook"
 * in META-INF/services.
 * Those services files are not registered using the standard Java6+ java.util.ServiceLoader
 * but with a Hazelcast version that is capable of working with multiple class loaders
 * to support JEE and OSGi environments.
 *
 * @param <T> the type of the serialized object
 */
public interface SerializerHook<T> {

    /**
     * Returns the actual class type of the serialized object
     *
     * @return the serialized object type
     */
    Class<T> getSerializationType();

    /**
     * Creates a new serializer for the serialization type
     *
     * @return a new serializer instance
     */
    Serializer createSerializer();

    /**
     * Defines if this serializer can be overridden by defining a custom
     * serializer in the configurations (codebase or configuration file)
     *
     * @return if the serializer is overwritable
     */
    boolean isOverwritable();

}
