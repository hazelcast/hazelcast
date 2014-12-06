/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import java.util.Collection;

/**
 * Base interface for Hazelcast distributed multi-maps.
 *
 * @see MultiMap
 * @see TransactionalMultiMap
 * @param <K>
 * @param <V>
 */
public interface BaseMultiMap<K, V> extends DistributedObject {

    /**
     * Stores a key-value pair in the multimap.
     *
     * @param key   the key to be stored
     * @param value the value to be stored
     * @return true if the size of the multimap is increased, false if the multimap
     *         already contains the key-value pair.
     */
    boolean put(K key, V value);

    /**
     * Returns the collection of values associated with the key.
     *
     * @param key the key whose associated values are returned
     * @return the collection of the values associated with the key
     */
    Collection<V> get(K key);

    /**
     * Removes the given key value pair from the multimap.
     *
     * @param key   the key of the entry to remove
     * @param value the value of the entry to remove
     * @return true if the size of the multimap changed after the remove operation, false otherwise.
     */
    boolean remove(Object key, Object value);

    /**
     * Removes all the entries associated with the given key.
     *
     * @param key the key of the entries to remove
     * @return the collection of removed values associated with the given key. The returned collection
     *         might be modifiable but it has no effect on the multimap.
     */
    Collection<V> remove(Object key);

    /**
     * Returns the number of values matching the given key in the multimap.
     *
     * @param key the key whose number of values is to be returned
     * @return the number of values matching the given key in the multimap
     */
    int valueCount(K key);

    /**
     * Returns the number of key-value pairs in the multimap.
     *
     * @return the number of key-value pairs in the multimap
     */
    int size();
}
