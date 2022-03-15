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

package com.hazelcast.internal.nearcache;

import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.spi.impl.InitializingObject;

import javax.annotation.Nullable;

/**
 * {@link NearCacheRecordStore} is the contract point to store keys
 * and values as {@link NearCacheRecord} internally and to serve them.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface NearCacheRecordStore<K, V> extends InitializingObject {

    /**
     * Gets the value associated with the given {@code key}.
     *
     * @param key the key from which to get the associated value.
     * @return the value associated with the given {@code key}.
     */
    V get(K key);

    /**
     * @see NearCache#put
     */
    void put(K key, Data keyData, V value, Data valueData);

    /**
     * @see NearCache#tryReserveForUpdate
     */
    long tryReserveForUpdate(K key, Data keyData, NearCache.UpdateSemantic updateSemantic);

    /**
     * @see NearCache#tryPublishReserved
     */
    @Nullable
    V tryPublishReserved(K key, V value, long reservationId, boolean deserialize);

    /**
     * Removes the value associated with the given {@code key}
     * and increases the invalidation statistics.
     *
     * @param key the key of the value will be invalidated
     */
    void invalidate(K key);

    /**
     * Removes all stored values.
     */
    void clear();

    /**
     * Clears the record store and destroys it.
     */
    void destroy();

    /**
     * Gets the number of stored records.
     *
     * @return the number of stored records.
     */
    int size();

    /**
     * Gets the record associated with the given {@code key}.
     *
     * @param key the key from which to get the associated {@link NearCacheRecord}.
     * @return the {@link NearCacheRecord} associated with the given {@code key}.
     */
    NearCacheRecord getRecord(K key);

    /**
     * Get the {@link NearCacheStats} instance to monitor this record store.
     *
     * @return the {@link NearCacheStats} instance to monitor this record store.
     */
    NearCacheStats getNearCacheStats();

    /**
     * Performs expiration and evicts expired records.
     */
    void doExpiration();

    /**
     * Does eviction as specified configuration {@link com.hazelcast.config.EvictionConfig}
     * in {@link com.hazelcast.config.NearCacheConfig}.
     *
     * @param withoutMaxSizeCheck set {@code true} to evict regardless of a max
     *                            size check, otherwise set {@code false} to evict
     *                            after a max size check.
     * @return {@code true} to indicate eviction
     * is done otherwise returns {@code false}
     */
    boolean doEviction(boolean withoutMaxSizeCheck);

    /**
     * Loads the keys into the Near Cache.
     */
    void loadKeys(DataStructureAdapter<Object, ?> adapter);

    /**
     * Persists the key set of the Near Cache.
     */
    void storeKeys();

    /**
     * @see StaleReadDetector
     */
    void setStaleReadDetector(StaleReadDetector detector);
}
