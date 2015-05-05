/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * {@link com.hazelcast.cache.impl.nearcache.NearCacheRecord} internally and to serve them.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface NearCacheRecordStore<K, V> {

    /**
     * Gets the value associated with the given <code>key</code>.
     *
     * @param key the key from which to get the associated value.
     * @return the value associated with the given <code>key</code>.
     */
    V get(K key);

    /**
     * Puts (associates) a value with the given <code>key</code>.
     *
     * @param key   the key to which the given value will be associated.
     * @param value the value that will be associated with the key.
     */
    void put(K key, V value);

    /**
     * Removes the value associated with the given <code>key</code>.
     *
     * @param key the key from which the value will be removed.
     * @return <code>true</code> if the value was removed, otherwise <code>false</code>.
     */
    boolean remove(K key);

    /**
     * Removes all stored values.
     */
    void clear();

    /**
     * Clears the record store and destroys it.
     */
    void destroy();

    /**
     * Get the {@link com.hazelcast.monitor.NearCacheStats} instance to monitor this record store.
     *
     * @return the {@link com.hazelcast.monitor.NearCacheStats} instance to monitor this record store.
     */
    NearCacheStats getNearCacheStats();

    /**
     * Selects the best candidate object to store from the given <code>candidates</code>.
     *
     * @param candidates the candidates from which the best candidate object will be selected.
     * @return the best candidate object to store, selected from the given <code>candidates</code>.
     */
    Object selectToSave(Object... candidates);

    /**
     * Gets the number of stored records.
     *
     * @return the number of stored records.
     */
    int size();

    /**
     * Performs expiration and evicts expired records.
     */
    void doExpiration();

    /**
     * Does eviction as specified configuration {@link com.hazelcast.config.EvictionConfig}
     * in {@link com.hazelcast.config.NearCacheConfig}.
     */
    void doEvictionIfRequired();

}
