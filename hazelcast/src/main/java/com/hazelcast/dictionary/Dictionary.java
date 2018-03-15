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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.dataseries.MemoryInfo;


/**
 * todo:
 * - for put/get the key isn't checked; just the hashcode.
 * - remove
 * - 64 bit hash; 32bit hash only effectively maps up tp 4.3B items
 * - concurrent access segment
 * - get segment usage data
 *      - number of items
 *      - memory allocated
 *      - memory used
 *      - load factor
 *  - in IMap the serialized bytes of the key are stored, but in the
 *    dictionary the content of the object is stored as key
 *  - support for byte-array value
 *  - support for byte-array key
 *  - there should not be any need for explicit support for primitive wrappers for keys
 *  - currently the segment size increases with a fixed factor of 2, this should be configurable.
 *  - type checking should be added to the codec.
 *  - add optional statistics to the map entry
 *          hits
 *          lastAccessTime
 *          lastUpdateTime
 *  - how to deal with finding a particular field in a non fixed length record?
 *  so imagine there are 2 byte-array fields, then the second byte array can only be found if the first
 *  byte-array is known, unless a table is kept. For fixed length fields this is less of an issue; they
 *  can be written first. Perhaps keeping such a table should be optional; makes sense if you don't need
 *  all fields and can deal with some extra memory consumption
 *
 * done:
 * - memory info
 *
 * @param <K>
 * @param <V>
 */
public interface Dictionary<K, V> {

    /**
     * Gets the value with the given key.
     *
     * @param key the key
     * @return the found item or null if item not found.
     * @throws NullPointerException if key is null.
     */
    V get(K key);

    ICompletableFuture<V> getAsync(K key);

    /**
     * Puts an entry in the dictionary.
     *
     * If the item already exists, it is overwritten.
     *
     * @param key
     * @param value
     * @throws NullPointerException if key or value is null.
     */
    void put(K key, V value);

    ICompletableFuture<Void> putAsync(K key, V value);

    /**
     * Removes the entry with the given key.
     *
     * @param key
     * @return the item removed, or null if item didn't exist.
     * @throws NullPointerException if key is null.
     */
    V remove(K key);

    ICompletableFuture<V> removeAsync(K key);

    /**
     * Returns the number of items in the dictionary.
     *
     * @return number  of items.
     */
    long size();

    /**
     * Clears the Dictionary.
     */
    void clear();

    MemoryInfo memoryInfo();

    MemoryInfo memoryInfo(int partitionId);
}
