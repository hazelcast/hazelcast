/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.InitializingObject;

/**
 * {@link NearCacheRecordStore} is the contract point to store keys and values as
 * {@link NearCacheRecord} internally and to serve them.
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
     * Puts (associates) a value with the given {@code key}.
     *
     * @param key       the key to which the given value will be associated.
     * @param keyData   the key as {@link Data} to which the given value will be associated.
     * @param value     the value that will be associated with the key.
     * @param valueData
     */
    void put(K key, Data keyData, V value, Data valueData);

    /**
     * Tries to reserve supplied key for update. <p> If one thread takes
     * reservation, only that thread can update the key.
     *
     * @param key     key to be reserved for update
     * @param keyData key to be reserved for update as {@link Data}
     * @return reservation ID if reservation succeeds, else returns {@link
     * NearCacheRecord#NOT_RESERVED}
     */
    long tryReserveForUpdate(K key, Data keyData);

    /**
     * Tries to update reserved key with supplied value. If update
     * happens, value is published. Publishing means making the value
     * readable to all threads. If update fails, record is not updated.
     *
     * @param key           reserved key for update
     * @param value         value to be associated with reserved key
     * @param reservationId ID for this reservation
     * @param deserialize   eagerly deserialize
     *                      returning value
     * @return associated value if deserialize is {@code
     * true} and update succeeds, otherwise returns null
     */
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
     */
    void doEviction(boolean withoutMaxSizeCheck);

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
