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
import java.util.Map;
import java.util.Set;

/**
 * Hazelcast distributed map implementation is an in-memory data store but
 * it can be backed by any type of data store such as RDBMS, OODBMS, or simply
 * a file based data store.
 * <p/>
 * IMap.get(key) normally returns the value that is available in-memory. If the entry
 * doesn't exist in-memory, Hazelcast returns null. If a Loader implementation
 * is provided then, instead of returning null, Hazelcast will load the unknown entry by
 * calling the implementation's load (key) or loadAll(keys) methods. Loaded entries
 * will be placed into the distributed map and they will stay in-memory until they are
 * explicitly removed or implicitly evicted (if eviction is configured).
 */
public interface MapLoader<K, V> {
    /**
     * Loads the value of a given key. If distributed map doesn't contain the value
     * for the given key then Hazelcast will call implementation's load (key) method
     * to obtain the value. Implementation can use any means of loading the given key;
     * such as an O/R mapping tool, simple SQL or reading a file etc.
     *
     * @param key
     * @return value of the key
     */
    V load(K key);

    /**
     * Loads given keys. This is batch load operation so that implementation can
     * optimize the multiple loads.
     *
     * @param keys keys of the values entries to load
     * @return map of loaded key-value pairs.
     */
    Map<K, V> loadAll(Collection<K> keys);

    /**
     * Loads all of the keys from the store.
     *
     * It is
     *
     * @return all the keys
     */
    Set<K> loadAllKeys();
}
