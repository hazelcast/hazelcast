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

package com.hazelcast.cache.impl.nearcache;

import com.hazelcast.monitor.NearCacheStats;

/**
 * {@link NearCacheRecordStore} is the contract point to store keys and values as
 * {@link com.hazelcast.cache.impl.nearcache.NearCacheRecord} internally and serve them.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface NearCacheRecordStore<K, V> {

    /**
     * Gets the value associated with the given <code>key</code>.
     *
     * @param key the key of the requested value
     * @return the value associated with the given <code>key</code>
     */
    V get(K key);

    /**
     * Puts the value as associated with the given <code>key</code>.
     *
     * @param key   the key of the value will be stored
     * @param value the value will be stored
     */
    void put(K key, V value);

    /**
     * Removes the value associated with the given <code>key</code>.
     *
     * @param key the key of the value will be removed
     * @return <code>true</code> if value was removed, otherwise <code>false</code>
     */
    boolean remove(K key);

    /**
     * Removes all stored values.
     */
    void clear();

    /**
     * Clears record store and destroy it.
     */
    void destroy();

    /**
     * Get the {@link com.hazelcast.monitor.NearCacheStats} instance to monitor this store.
     *
     * @return the {@link com.hazelcast.monitor.NearCacheStats} instance to monitor this store
     */
    NearCacheStats getNearCacheStats();

    /**
     * Selects the best candidate object between given <code>candidates</code> to store.
     *
     * @param candidates the candidates where the best one will be selected
     * @return the best candidate object between given <code>candidates</code> to store
     */
    Object selectToSave(Object... candidates);

    /**
     * Gets the count of stored records.
     *
     * @return the count of stored records
     */
    int size();

}
