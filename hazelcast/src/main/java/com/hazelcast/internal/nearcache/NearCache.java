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

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nullable;

/**
 * {@link NearCache} is the contract point to store keys and values in underlying
 * {@link NearCacheRecordStore}.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface NearCache<K, V> extends InitializingObject {

    /**
     * Default expiration task initial delay time as seconds
     */
    int DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS = 5;

    /**
     * Default expiration task delay time as seconds
     */
    int DEFAULT_EXPIRATION_TASK_PERIOD_SECONDS = 5;

    String PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS
            = "hazelcast.internal.nearcache.expiration.task.initial.delay.seconds";

    String PROP_EXPIRATION_TASK_PERIOD_SECONDS
            = "hazelcast.internal.nearcache.expiration.task.period.seconds";

    HazelcastProperty TASK_INITIAL_DELAY_SECONDS
            = new HazelcastProperty(PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS,
            DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS);

    HazelcastProperty TASK_PERIOD_SECONDS
            = new HazelcastProperty(PROP_EXPIRATION_TASK_PERIOD_SECONDS,
            DEFAULT_EXPIRATION_TASK_PERIOD_SECONDS);

    /**
     * Indicates how a near cache is updated.
     */
    enum UpdateSemantic {
        /**
         * A read-only operation is updating near cache,
         * like {@link com.hazelcast.map.IMap#get}
         * or {@link com.hazelcast.map.IMap#getAll}
         */
        READ_UPDATE,
        /**
         * A write operation is updating near cache, like
         * {@link com.hazelcast.cache.ICache#put} or
         * {@link com.hazelcast.cache.ICache#putIfAbsent}
         */
        WRITE_UPDATE
    }

    /**
     * NULL Object
     */
    Object CACHED_AS_NULL = new Object();

    /**
     * NOT_CACHED Object
     */
    Object NOT_CACHED = new Object();

    /**
     * Gets the name of this {@link NearCache} instance.
     *
     * @return the name of this {@link NearCache} instance
     */
    String getName();

    /**
     * @return config object used by {@link NearCache} instance.
     */
    NearCacheConfig getNearCacheConfig();

    /**
     * Gets the value associated with the given {@code key}.
     *
     * @param key the key of the requested value
     * @return the value associated with the given {@code key}
     */
    V get(K key);

    /**
     * Puts (associates) a value with the given {@code key}.
     *
     * @param key       the key of the value will be stored
     * @param keyData   the key as {@link Data} of the value will be stored
     * @param value     the value will be stored
     * @param valueData the value as {@link Data} of the value will be stored
     */
    void put(K key, Data keyData, V value, Data valueData);

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
     * Gets the count of stored records.
     *
     * @return the count of stored records
     */
    int size();

    /**
     * Get the {@link NearCacheStats} instance to
     * monitor this store.
     *
     * @return the {@link NearCacheStats} instance
     * to monitor this store
     */
    NearCacheStats getNearCacheStats();

    /**
     * Checks if the Near Cache key is stored in serialized format or
     * by-reference.
     *
     * @return {@code true} if the key is stored in serialized format,
     * {@code false} if stored by-reference.
     */
    boolean isSerializeKeys();

    /**
     * Executes the Near Cache pre-loader on the given {@link
     * DataStructureAdapter}.
     */
    void preload(DataStructureAdapter<Object, ?> adapter);

    /**
     * Stores the keys of the Near Cache.
     */
    void storeKeys();

    /**
     * Checks if the pre-loading of the Near Cache is done.
     *
     * @return {@code true} if the pre-loading is done, {@code false}
     * otherwise.
     */
    boolean isPreloadDone();

    /**
     * Used to access non-standard methods of an implementation. <p> If
     * this method is called on a wrapper object, result is wrapped
     * object.
     *
     * @param clazz the type of returning object.
     * @param <T>   the type
     *              of the class modeled by this Class object
     * @return an instance of the supplied clazz type.
     * @throws IllegalArgumentException if no implementation found for the supplied clazz type.
     */
    <T> T unwrap(Class<T> clazz);

    /**
     * Reservations are done with this method.
     *
     * Tries to reserve supplied key for update. <p> If one thread takes
     * reservation, only that thread can update the key.
     *
     * @param key            key to be reserved for update
     * @param keyData        key to be reserved for update as {@link Data}
     * @param updateSemantic when {@link UpdateSemantic#WRITE_UPDATE}
     *                       reservation is done with cacheOnUpdate semantics,
     *                       otherwise reservations are done without that semantic.
     * @return reservation ID if reservation succeeds, else returns {@link
     * NearCacheRecord#NOT_RESERVED}
     */
    long tryReserveForUpdate(K key, Data keyData, UpdateSemantic updateSemantic);

    /**
     * Tries to update reserved key with supplied value. If update
     * happens, value is published. Publishing means making the value
     * readable to all threads. If update fails, record is not updated.
     *
     * @param key           reserved key for update
     * @param value         value to be associated with reserved key
     * @param reservationId ID for this reservation
     * @param deserialize   when {@code true} eagerly deserialize
     *                      returning value
     * @return associated value if deserialize is {@code
     * true} and update succeeds, otherwise returns null
     */
    @Nullable
    V tryPublishReserved(K key, V value, long reservationId, boolean deserialize);
}
