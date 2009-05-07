/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A specialized map whose keys can be associated with multiple values.
 *
 * @author oztalip
 */
public interface MultiMap<K, V> extends ICommon {
    /**
     * Returns the name of this multimap.
     * @return the name of this multimap
     */
    String getName();

    /**
     * Stores a key-value pair in the multimap.
     *
     * @param key   the key to be stored
     * @param value the value to be stored
     * @return true if size of the multimap is increased, false if the multimap
     *         already contains the key-value pair.
     */
    boolean put(K key, V value);

    /**
     * Returns the collection of values associated with the key.
     *
     * @param key the key whose associated values are to be returned
     * @return the collection of the values associated with the key.
     */
    Collection<V> get(K key);

    /**
     * Removes the given key value pair from the multimap.
     *
     * @param key   the key of the entry to remove
     * @param value the value of the entry to remove
     * @return true if the size of the multimap changed after the remove operation, false otherwise.
     */
    boolean remove(K key, V value);

    /**
     * Removes all the entries with the given key.
     *
     * @param key the key of the entries to remove
     * @return the collection of removed values associated with the given key. Returned collection
     *         might be modifiable but it has no effect on the multimap
     */
    Collection<V> remove(K key);

    /**
     * Returns the set of keys in the multimap.
     *
     * @return the set of keys in the multimap. Returned set might be modifiable
     *         but it has no effect on the multimap
     */
    Set<K> keySet();

    /**
     * Returns the collection of values in the multimap.
     *
     * @return the collection of values in the multimap. Returned collection might be modifiable
     *         but it has no effect on the multimap
     */
    Collection<V> values();

    /**
     * Returns the set of key-value pairs in the multimap.
     *
     * @return the set of key-value pairs in the multimap. Returned set might be modifiable
     *         but it has no effect on the multimap
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Returns whether the multimap contains an entry with the key.
     *
     * @param key the key whose existence is checked.
     * @return true if the multimap contains an entry with the key, false otherwise.
     */
    boolean containsKey(K key);

    /**
     * Returns whether the multimap contains an entry with the value.
     *
     * @param value the value whose existence is checked.
     * @return true if the multimap contains an entry with the value, false otherwise.
     */
    boolean containsValue(V value);

    /**
     * Returns whether the multimap contains the given key-value pair.
     *
     * @param key   the key whose existence is checked.
     * @param value the value whose existence is checked.
     * @return true if the multimap contains the key-value pair, false otherwise.
     */
    boolean containsEntry(K key, V value);

    /**
     * Returns the number of key-value pairs in the multimap.
     *
     * @return the number of key-value pairs in the multimap.
     */
    int size();

    /**
     * Clears the multimap. Removes all key-value pairs.
     */
    void clear();
}
