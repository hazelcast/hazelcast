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

package com.hazelcast.nio.serialization.compact;

import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;

/**
 * Defines the contract of the serializers used for Compact serialization.
 * <p>
 * After defining a serializer for the objects of the class {@code T}, it can be
 * registered using the
 * {@link com.hazelcast.config.CompactSerializationConfig}.
 * <p>
 * {@link #write(CompactWriter, Object)} and {@link #read(CompactReader)}
 * methods must be consistent with each other.
 *
 * @param <T> Type of the serialized/deserialized class
 * @since 5.2
 */
public interface CompactSerializer<T> {
    /**
     * @param reader reader to read fields of an object
     * @return the object created as a result of read method
     * @throws HazelcastSerializationException in case of failure to read
     */
    @Nonnull
    T read(@Nonnull CompactReader reader);

    /**
     * @param writer CompactWriter to serialize the fields onto
     * @param object to be serialized.
     * @throws HazelcastSerializationException in case of failure to write
     */
    void write(@Nonnull CompactWriter writer, @Nonnull T object);

    /**
     * Returns the unique type name for the class {@link T}.
     * <p>
     * If the class {@link T} is ever evolved by adding or removing fields, the
     * type name for the evolved serializers must be the same with the initial
     * version.
     *
     * @return the type name
     */
    @Nonnull
    String getTypeName();

    /**
     * @return the class to be serialized with this serializer.
     */
    @Nonnull
    Class<T> getCompactClass();
}
