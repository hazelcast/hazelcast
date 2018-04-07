/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICompletableFuture;

import javax.cache.Cache;


/**
 * todo:
 * - ClassField should have a 'Type'
 * - xml configuration
 * - no available check in case of variable length record
 * - mechanism for fast loading huge quantities of data.
 * - mechanism to constrain the total memory of a segment
 *      perhaps this should be taken care of on the partition level? So partition is allowed to have a max size and
 *      the segments just take from this available space when incrementing.
 * - 64 bit hash; 32bit hash only effectively maps up tp 4.3B items
 * - concurrent access segment
 * - get segment usage data
 *      - number of items
 *      - memory allocated
 *      - memory used
 *      - load factor
 * - data region
 *      - garbage collection
 *      - currently the data segment size increases with a fixed factor of 2, this should be configurable.
 *      - instead of having a single data region, perhaps chop it up in multiple so that growing is less of pain?
 * - add optional statistics to the map entry
 *      hits
 *      lastAccessTime
 *      lastUpdateTime
 * - how to deal with finding a particular field in a non fixed length record?
 *        so imagine there are 2 byte-array fields, then the second byte array can only be found if the first
 *        byte-array is known, unless a table is kept. For fixed length fields this is less of an issue; they
 *        can be written first. Perhaps keeping such a table should be optional; makes sense if you don't need
 *        all fields and can deal with some extra memory consumption
 * - replication
 * - dictionary write variable length key
 * - values iterator
 *          - this will force dealing with non contiguous memory
 * - mapping
 *          - type checking should be added to the codec.
 *          - blob option: so no analysis of the object; just store the blob
 *                  - for key
 *                  - for value
 *          - enum
 *          - fixed length strings
 *          - string field
 *          - currently primitive fields are without order.
 *          - boolean compression by storing in bit
 *          - Primitive wrapper compression: every wrapper only needs a bit for a null marker; not a byte.
 *          - primitive array overflow checking
 *          - support for primitive-array key
 *          - in IMap the serialized bytes of the key are stored, but in the
 *          dictionary the content of the object is stored as key
 *          - primitive array field test not working
 *          - PrimitiveWrappers should not need 'null' byte since they can't be null.
 *          - write array does no availability check
 *
 * done:
 * - for put/get the key isn't checked; just the hashcode.
 * - when dictionary proxy is created, type validation logic should be done.
 *       - it is confusing that only on true interaction errors are detected. This should be done
 *       as soon as the proxy is created.
 * - add tests for illegal type of fields
 * - overwrite value for variable size record not working
 * - when partition count configured with same value as segment count then error
 * - replace: variable length; don't fix fragmentation, just write the value in different location
 * - basic remove; don't fix fragmentation, just remove the item
 * - pulled out AbstractCodeGen
 * - support for byte-array value
 * - add tests for illegal classes (interface e.g.)
 * - support for primitive-array value
 *
 * @param <K>
 * @param <V>
 */
public interface Dictionary<K, V> extends Cache<K, V>, DistributedObject {

    /**
     * Gets the value asynchronous.
     *
     * @param key the key of the value to get.
     * @return the future.
     * @throws NullPointerException if key is null.
     */
    ICompletableFuture<V> getAsync(K key);

    ICompletableFuture<Void> putAsync(K key, V value);

    ICompletableFuture<Boolean> removeAsync(K key);

    /**
     * Returns the number of items in the dictionary.
     *
     * @return number  of items.
     */
    long size();

    /**
     * Gets the MemoryInfo of all partitions for this dictionary.
     *
     * @return the MemoryInfo.
     */
    MemoryInfo memoryInfo();

    /**
     * Gets the memory info of a specific partition.
     *
     * @param partitionId the id of the partition
     * @return the MemoryInfo for the partition
     */
    MemoryInfo memoryInfo(int partitionId);

    /**
     * Creates a prepared aggregation.
     *
     * @return the prepared aggregation.
     */
    PreparedAggregation prepare(AggregationRecipe recipe);
}
