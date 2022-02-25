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

package com.hazelcast.internal.serialization;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.partition.PartitioningStrategy;

/**
 * SPI to serialize user objects to {@link Data} and back to Object
 * {@link Data} is the internal representation of binary data in hazelcast.
 */
public interface SerializationService {

    /**
     * Serializes an object to a {@link Data}.
     * <p>
     * This method can safely be called with a {@link Data} instance. In that case, that instance is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param obj the object to serialize.
     * @return the serialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when serialization fails.
     */
    <B extends Data> B toData(Object obj);

    /**
     * Serializes an object to a {@link Data} that contains the
     * {@link com.hazelcast.internal.serialization.impl.compact.Schema} in the
     * binary, if the {@code object} is compact serializable. If not,
     * this method is same as the {@link #toData(Object)}.
     * <p>
     * An object is compact serializable if
     * <ul>
     *     <li>it is registered for compact serialization via
     *     {@link com.hazelcast.config.CompactSerializationConfig}</li>
     *     <li>there is no serializer registered for it, and it can serialized
     *     reflectively with {@link com.hazelcast.internal.serialization.impl.compact.ReflectiveCompactSerializer}</li>
     * </ul>
     * <p>
     * This method can safely be called with a {@link Data} instance. The schema
     * will be included only if the type of the data instance is {@code TYPE_COMPACT}.
     * For any other data type, the instance will be returned as it is. Note that,
     * if the data type is already {@code TYPE_COMPACT_WITH_SCHEMA}, it will also
     * be returned as it is, since the schema is already included in it.
     *
     * @param obj the object to serialize.
     * @return the serialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when serialization fails.
     */
    <B extends Data> B toDataWithSchema(Object obj);

    /**
     * Serializes an object to a {@link Data}.
     * <p>
     * This method can safely be called with a {@link Data} instance. In that case, that instance is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param obj      the object to serialize.
     * @param strategy strategy is used to calculate partition ID of the resulting data see {@link PartitioningStrategy}
     * @return the serialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when serialization fails.
     */
    <B extends Data> B toData(Object obj, PartitioningStrategy strategy);

    /**
     * Deserializes an object.
     * <p>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param data the data to deserialize.
     * @return the deserialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when deserialization fails.
     */
    <T> T toObject(Object data);

    /**
     * Deserializes an object.
     * <p>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param data  the data to deserialize.
     * @param klazz The class to instantiate when deserializing the object.
     * @return the deserialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when deserialization fails.
     */
    <T> T toObject(Object data, Class klazz);

    /**
     * see {@link com.hazelcast.config.Config#setManagedContext(ManagedContext)}
     *
     * @return ManagedContext that is set by user in Config
     */
    ManagedContext getManagedContext();

    /**
     * Trims the {@link com.hazelcast.internal.serialization.impl.compact.Schema}
     * from the {@code data}, if it includes the schema in its serialized form.
     * If not, this method will return the {@code data} as it is, without
     * changing it.
     * <p>
     * Trimming process takes care of the nested schemas, as well as the top
     * level schema.
     *
     * @param data the data to be trimmed, if it contains a schema.
     * @return the data without schema in it.
     */
    <B extends Data> B trimSchema(Data data);
}
