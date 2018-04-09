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
import java.util.Iterator;
import java.util.Map;


/**
 * todo:
 * - aggregation
 * - concurrency
 *          - concurrent access segment
 *          - concurrent operations in single partition and partition migration
 *          - caller runs?
 * - mechanism for fast loading huge quantities of data.
 * - 64 bit hash; 32bit hash only effectively maps up tp 4.3B items
 * - get segment usage data
 *      - number of items
 *      - memory allocated
 *      - memory used
 *      - load factor
 * - data region
 *      - shrinking of the data region gets below the low water mark
 *      - compact function that compact all the records in the data region and moves them to one side.
 *          - the offset table needs to be updated so that the offset of the key is pointing to the right
 *          offset. A simple way to do it is to delete and then insert.
 *      - currently the data segment size increases with a fixed factor of 2, this should be configurable.
 *      - instead of having a single data region, perhaps chop it up in multiple so that growing is less of pain?
 *      - mechanism to constrain the total memory of a segment
 *        perhaps this should be taken care of on the partition level? So partition is allowed to have a max size and
 *        the segments just take from this available space when incrementing. Or maybe like HD native-memory global.
 *      - explicit option for compaction? So will defragment the memory and ditch everything not used
 *      - currently there is no knowledge how much data memory is garbage (it isn't easy to determine the size
 *      of an non fixed length entry).
 *
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
 * - values iterator
 *          - this will force dealing with non contiguous memory
 * - mapping
 *          - length info
 *              - if the value is fixed length and the value is a primitive array,
 *              no additional size info is needed since the size of the key can be
 *              determined based on the length of the key (which is already in the front
 *              and the size of the value is fixed length
 *          - a variable sized entry should always have a 'size' header so that one can jump
 *          over a record. Also needed for removal. Currently the only way to determine the size
 *          is to read the full key/value
 *          - Should the key be put next to the value? If one needs to scan through a huge amount of values and isn't
 *          needing the key, then memory bandwidth is wasted.
 *          - ClassField should have a 'Type'
 *          - ambiguity between fixed length records and primitive wrappers
 *          - key-checking limited to fixed length records
 *          - no available check in case of variable length record
 *          - dictionary write variable length key
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
 *          - certain primitive wrapper values can be cached instead of recreated (litter reduction)
 *          - a non fixed length map entry should add the size in the header.
 *
 * done:
 * - xml configuration
 * - fixed test failure in dictionary.clear
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

    /**
     * Gets the entries for the given partition/segmentId.
     *
     * This method is temporary; it will help to test the underlying logic for iteration.
     *
     * @param partitionId
     * @param segmentId
     * @return
     */
    Iterator<Map.Entry<K,V>> entries(int partitionId, int segmentId);
}
