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

package com.hazelcast.cache;


import javax.cache.expiry.ExpiryPolicy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Hazelcast cache extension interface
 * @param <K> key
 * @param <V> value
 */
public interface ICache<K, V>
        extends javax.cache.Cache<K, V> {

    //region async extentions

    /**
     * Asynchronously get an entry from cache
     *
     * @param key
     * @return
     */
    Future<V> getAsync(K key);

    /**
     * Asynchronously get an entry from cache with a provided expiry policy
     *
     * @param key
     * @param expiryPolicy
     * @return
     */
    Future<V> getAsync(K key, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in the cache.
     * @param key
     * @param value
     * @return
     */
    Future<Void> putAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in the cache using a custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in the cache if not already exist
     * using a custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    Future<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * @param key
     * @param value
     * @return
     */
    Future<V> getAndPutAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed using a custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously removes the mapping for a key from this cache if it is present.
     * @param key
     * @return
     */
    Future<Boolean> removeAsync(K key);

    /**
     *  Asynchronously removes the mapping for a key only if currently mapped to the
     * given value.
     * @param key
     * @param oldValue
     * @return
     */
    Future<Boolean> removeAsync(K key, V oldValue);

    /**
     *  Asynchronously removes the entry for a key returning the removed value if one existed.
     * @param key
     * @return
     */
    Future<V> getAndRemoveAsync(K key);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to a
     * given value.
     * @param key
     * @param oldValue
     * @param newValue
     * @return
     */
    Future<Boolean> replaceAsync(K key, V oldValue, V newValue);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to a
     * given value using custom expiry policy
     * @param key
     * @param oldValue
     * @param newValue
     * @param expiryPolicy
     * @return
     */
    Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to some
     * @param key
     * @param value
     * @return
     */
    Future<V> getAndReplaceAsync(K key, V value);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to some
     * using custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion

    //region method with expirypolicy

    /**
     * Get with custom expiry policy
     * @param key
     * @param expiryPolicy
     * @return
     */
    V get(K key, ExpiryPolicy expiryPolicy);

    /**
     * getAll with custom expiry policy
     * @param keys
     * @param expiryPolicy
     * @return
     */
    Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy);

    /**
     * put with custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     */
    void put(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * getAndPut with custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    V getAndPut(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * putAll with custom expiry policy
     * @param map
     * @param expiryPolicy
     */
    void putAll(java.util.Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy);

    /**
     * putIfAbsent with custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * replace with custom expiry policy
     * @param key
     * @param oldValue
     * @param newValue
     * @param expiryPolicy
     * @return
     */
    boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    /**
     * replace with custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    boolean replace(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * getAndReplace with custom expiry policy
     * @param key
     * @param value
     * @param expiryPolicy
     * @return
     */
    V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion

    /**
     * total cache size
     * @return
     */
    int size();

    //    CacheStats getStats();

}
