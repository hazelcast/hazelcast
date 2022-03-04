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

import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.partition.PartitioningStrategy;

/**
 * Data is basic unit of serialization. It stores binary form of an object serialized
 * by {@link SerializationService#toData(Object)}.
 */
public interface Data {

    /**
     * Returns byte array representation of internal binary format.
     *
     * @return binary data
     */
    byte[] toByteArray();

    /**
     * Returns serialization type of binary form. It's defined by
     * {@link Serializer#getTypeId()}
     *
     * @return serializer type ID
     */
    int getType();

    /**
     * Returns the total size of Data in bytes.
     * <p>
     * The total size is the size of a byte array to contain the full content of this data object. For example in case
     * of an HeapData object, the total size will be the length of the payload.
     *
     * @return total size
     */
    int totalSize();

    /**
     * Copies the payload contained in the Data to the destination buffer.
     * <p>
     * The dest byte-buffer needs to be large enough to contain the payload. Otherwise an exception is thrown.
     * <p>
     * The reason this method exists instead of relying on the {@link #toByteArray()} is the existence of the NativeMemoryData.
     * With the NativeMemoryData it would lead to a temporary byte-array. This method prevents this temporary byte-array needing
     * to be created.
     *
     * @param dest    to byte-buffer to write to
     * @param destPos the position in the destination buffer.
     */
    void copyTo(byte[] dest, int destPos);

    /**
     * Returns size of internal binary data in bytes
     *
     * @return internal data size
     */
    int dataSize();

    /**
     * Returns approximate heap cost of this Data object in bytes.
     *
     * @return approximate heap cost
     */
    int getHeapCost();

    /**
     * Returns partition hash calculated for serialized object.
     * Partition hash is used to determine partition of a Data and is calculated using
     * {@link PartitioningStrategy} during serialization.
     * <p>
     * If partition hash is not set then standard <code>hashCode()</code> is used.
     *
     * @return partition hash
     * @see com.hazelcast.partition.PartitionAware
     * @see PartitioningStrategy
     * @see SerializationService#toData(Object, PartitioningStrategy)
     */
    int getPartitionHash();

    /**
     * Returns true if Data has partition hash, false otherwise.
     *
     * @return true if Data has partition hash, false otherwise.
     */
    boolean hasPartitionHash();

    /**
     * Returns 64-bit hash code for this Data object.
     *
     * @return 64-bit hash code
     */
    long hash64();

    /**
     * Returns true if this Data is created from a {@link com.hazelcast.nio.serialization.Portable} object,
     * false otherwise.
     *
     * @return true if source object is <code>Portable</code>, false otherwise.
     */
    boolean isPortable();

    /**
     * Returns true if this Data is created from a {@link com.hazelcast.core.HazelcastJsonValue} object,
     * false otherwise
     *
     * @return true if source object is <code>HazelcastJsonValue</code>, false otherwise.
     */
    boolean isJson();

    /**
     * Returns true if this Data is serialized in CompactFormat
     * {@link com.hazelcast.config.CompactSerializationConfig}
     *
     * @return true if source object is serialized in compact format, false otherwise.
     */
    boolean isCompact();

}
