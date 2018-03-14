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


/**
 * todo:
 * - should the offset table be on the segment?
 *      - the advantage is that is a lot easier to control the total datasize
 *      and the data section can grow until it runs into the footer.
 * - the offset table can't grow
 * - for put/get the key isn't checked; just the hashcode.
 * - remove
 * - memory info
 * - concurrent access segment
 * - get segment usage data
 *      - number of items
 *      - memory allocated
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
 *
 * done:
 * - map.put overwrite not implemented
 * - there is no upper bound for the segment size
 * - entry model: fields are stored based on fieldname, but this causes problems with shadow fields.
 * - clear
 * - primitive wrappers don't need to be supported explicitly for values
 * - support for private fields of pojo's.
 * - growing of segment
 * - size
 * - compiler make use of dictionary directory
 * - lazy allocation of memory to the segment.
 *  - the encoder doesn't know the end address when writing, so it doesn't know
 *  when the segment is full.
 * - get
 * - create the key hashtable
 * - put should update key hashtable
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
}
