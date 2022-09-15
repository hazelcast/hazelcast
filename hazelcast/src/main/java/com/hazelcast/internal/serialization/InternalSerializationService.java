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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitioningStrategy;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteOrder;

public interface InternalSerializationService extends SerializationService, Disposable {

    byte VERSION_1 = 1;

    /**
     * Writes the obj to a byte array. This call is exactly the same as calling {@link #toData(Object)} and
     * then calling {@link Data#toByteArray()}. But it doesn't force a HeapData object being created.
     * <p>
     * <b>IMPORTANT:</b> The byte order used to serialize {@code obj}'s serializer type ID is always
     * {@link ByteOrder#BIG_ENDIAN}.
     */
    byte[] toBytes(Object obj);

    /**
     * Writes an object to a byte-array.
     * <p>
     * It allows for configurable padding on the left.
     * <p>
     * The padded bytes are not zero'd out since they will be written by the caller. Zero'ing them out would be waste of
     * time.
     * <p>
     * If you want to convert an object to a Data (or its byte representation) then you want to have the partition hash, because
     * that is part of the Data-definition.
     * <p>
     * But if you want to serialize an object to a byte-array and don't care for the Data partition-hash, the hash can be
     * disabled.
     * <p>
     * <b>IMPORTANT:</b> The byte order used to serialize {@code obj}'s serializer type ID is the byte order
     * configured in {@link SerializationConfig#getByteOrder()}.
     *
     * @param obj                 object to write to byte array
     * @param leftPadding         offset from beginning of byte array to start writing the object's bytes
     * @param insertPartitionHash {@code true} to include the partition hash in the byte array, otherwise {@code false}
     */
    byte[] toBytes(Object obj, int leftPadding, boolean insertPartitionHash);

    <B extends Data> B toData(Object obj, DataType type);

    <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy);

    <B extends Data> B convertData(Data data, DataType type);

    void writeObject(ObjectDataOutput out, Object obj);

    <T> T readObject(ObjectDataInput in);

    <T> T readObject(ObjectDataInput in, Class aClass);

    void disposeData(Data data);

    BufferObjectDataInput createObjectDataInput(byte[] data);

    BufferObjectDataInput createObjectDataInput(byte[] data, int offset);

    BufferObjectDataInput createObjectDataInput(Data data);

    BufferObjectDataOutput createObjectDataOutput(int size);

    /**
     * Creating new {@link BufferObjectDataOutput}. The size of the internal buffer is set to initialSize at construction.
     * When the buffer is too small to accept new bytes the buffer will grow to at least firstGrowthSize.
     */
    BufferObjectDataOutput createObjectDataOutput(int initialSize, int firstGrowthSize);

    BufferObjectDataOutput createObjectDataOutput();

    /**
     * The returned InternalGenericRecord is not thread safe.
     * It is not meant to return to the user from our API's like IMap, IQueue etc.
     * It will only be used with {@link com.hazelcast.internal.serialization.impl.GenericRecordQueryReader} for
     * query purposes
     *
     * @param data
     * @return InternalGenericRecord if data type supports it(Portable or Compact), otherwise throws
     * IllegalArgumentException
     * @throws IOException
     */
    InternalGenericRecord readAsInternalGenericRecord(Data data) throws IOException;

    /**
     * @param data to extract the schema from
     * @return schema of the given Compact Data
     * @throws IOException
     * @throws IllegalArgumentException if given data is not in the Compact format
     */
    Schema extractSchemaFromData(@Nonnull Data data) throws IOException;

    /**
     * @param object to extract the schema from
     * @return schema of the given Compact Data
     * @throws IllegalArgumentException if given object is not compact serializable
     *                                  see {@link #isCompactSerializable(Object)}
     */
    Schema extractSchemaFromObject(@Nonnull Object object);

    /**
     * Returns {@code true} if the {@code object} is compact serializable.
     * <p>
     * It will return {@code true} if
     * <ul>
     *     <li>it is registered as compact serializable through configuration.</li>
     *     <li>it cannot be serialized through other mechanisms and the object might
     *     be serialized reflectively (zero-config use case).</li>
     * </ul>
     */
    boolean isCompactSerializable(Object object);

    PortableContext getPortableContext();

    ClassLoader getClassLoader();

    /**
     * Returns the byte order used when serializing/deserializing objects. A notable exception is the top-level object's
     * {@code serializerTypeId}: when using {@link #toBytes(Object)}, the object's {@code serializerTypeId} is serialized
     * always in {@link ByteOrder#BIG_ENDIAN}, while when using {@link #toBytes(Object, int, boolean)}, the configured
     * byte order is used. Inner objects (for example those serialized within an
     * {@code IdentifiedDataSerializable}'s {@code writeData(ObjectDataOutput)} with
     * {@link ObjectDataOutput#writeObject(Object)}) always use the configured byte order.
     *
     * @return the {@link ByteOrder} which is configured for this {@link InternalSerializationService}.
     * @see com.hazelcast.config.SerializationConfig#setByteOrder(ByteOrder)
     * @see com.hazelcast.config.SerializationConfig#setUseNativeByteOrder(boolean)
     * @see com.hazelcast.config.SerializationConfig#setAllowUnsafe(boolean)
     */
    ByteOrder getByteOrder();

    byte getVersion();

}
