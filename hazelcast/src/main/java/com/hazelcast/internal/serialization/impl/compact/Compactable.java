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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.compact.CompactSerializer;

/**
 * Interface to customize how an object is written in Compact Format.
 * One can either implement `CompactSerializer` and register
 * it via {@link com.hazelcast.config.CompactSerializationConfig#register(Class, CompactSerializer)}
 * or the serialized object can implement this interface so that the serialization
 * system gets the related compact serializer. This interface is used by code
 * generation an not intended to be a public API.
 *
 * @param <T> Type of the object to be serialized
 */
public interface Compactable<T> {

    /**
     * Returns the compact serializer that will be used the serialize
     * instances of class {@code T}.
     */
    CompactSerializer<T> getCompactSerializer();
}
