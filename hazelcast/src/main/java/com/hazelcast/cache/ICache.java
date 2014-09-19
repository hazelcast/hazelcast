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
     * @param key the key whose associated value is to be returned
     * @return Future from which the value of the key can be retrieved.
     * @see javax.cache.Cache#get(K)
     * @see java.util.concurrent.Future
     */
    Future<V> getAsync(K key);

    /**
     * Asynchronously get an entry from cache with a provided expiry policy
     *
     * @param key the key whose associated value is to be returned
     * @param expiryPolicy custom expiry policy for this operation
     * @return Future from which the value of the key can be retrieved.
     * @see javax.cache.Cache#get(K)
     * @see java.util.concurrent.Future
     */
    Future<V> getAsync(K key, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in the cache.
     *
     * @param key the key whose associated value is to be returned
     * @param value value to be associated with the specified key
     * @return Future
     * @see javax.cache.Cache#put(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Void> putAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in the cache using a
     * custom expiry policy
     *
     * @param key the key whose associated value is to be returned
     * @param value value to be associated with the specified key
     * @param expiryPolicy custom expiry Policy for this operation
     * @return Future
     * @see javax.cache.Cache#put(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in the cache if not already exist
     * using a custom expiry policy
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expiryPolicy
     * @return
     * @see javax.cache.Cache#putIfAbsent(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * @param key the key whose associated value is to be returned
     * @param value value to be associated with the specified key
     * @return Future
     * @see javax.cache.Cache#getAndPut(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndPutAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed using a custom expiry policy
     * @param key the key whose associated value is to be returned
     * @param value value to be associated with the specified key
     * @param expiryPolicy custom expiry Policy for this operation
     * @return Future
     * @see javax.cache.Cache#getAndPut(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously removes the mapping for a key from this cache if it is present.
     * @param key the key whose associated value is to be returned
     * @return Future
     * @see javax.cache.Cache#remove(K)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> removeAsync(K key);

    /**
     * Asynchronously removes the mapping for a key only if currently mapped to the
     * given value.
     * @param key the key whose associated value is to be returned
     * @param oldValue value expected to be associated with the specified key
     * @return Future
     * @see javax.cache.Cache#remove(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> removeAsync(K key, V oldValue);

    /**
     * Asynchronously removes the entry for a key returning the removed value if one existed.
     *
     * @param key the key whose associated value is to be returned
     * @return Future
     * @see javax.cache.Cache#getAndRemove(K)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndRemoveAsync(K key);

    /**
     * Asynchronously replaces the entry for a key
     *
     * @param key the key whose associated value is to be returned
     * @param value value to be associated with the specified key
     * @return Future
     * @see javax.cache.Cache#replace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V value);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to some
     * value.
     * @param key  the key with which the specified value is associated
     * @param value the value to be associated with the specified key
     * @param expiryPolicy custom expiry Policy for this operation
     * @return
     * @see javax.cache.Cache#replace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to a
     * given value.
     * @param key      key with which the specified value is associated
     * @param oldValue value expected to be associated with the specified key
     * @param newValue value to be associated with the specified key
     * @return
     * @see javax.cache.Cache#replace(K,V,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V oldValue, V newValue);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to a
     * given value using custom expiry policy
     * @param key      key with which the specified value is associated
     * @param oldValue value expected to be associated with the specified key
     * @param newValue value to be associated with the specified key
     * @param expiryPolicy custom expiry Policy for this operation
     * @return
     * @see javax.cache.Cache#replace(K,V,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);


    /**
     * Asynchronously replaces the entry for a key only if currently mapped to some
     * @param key  the key with which the specified value is associated
     * @param value the value to be associated with the specified key
     * @return
     * @see javax.cache.Cache#getAndReplace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndReplaceAsync(K key, V value);

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to some
     * using custom expiry policy
     * @param key  the key with which the specified value is associated
     * @param value the value to be associated with the specified key
     * @param expiryPolicy custom expiry Policy for this operation
     * @return
     * @see javax.cache.Cache#getAndReplace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion

    //region method with expirypolicy

    /**
     * Get with custom expiry policy
     * @param key the key whose associated value is to be returned
     * @param expiryPolicy
     * @return
     * @see javax.cache.Cache#get(K)
     * @see java.util.concurrent.Future
     */
    V get(K key, ExpiryPolicy expiryPolicy);

    /**
     * getAll with custom expiry policy
     * @param keys The keys whose associated values are to be returned.
     * @param expiryPolicy
     * @return
     * @see javax.cache.Cache#getAll(java.util.Set)
     * @see java.util.concurrent.Future
     */
    Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy);

    /**
     * put with custom expiry policy
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expiryPolicy
     * @see javax.cache.Cache#put(K,V)
     * @see java.util.concurrent.Future
     */
    void put(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * getAndPut with custom expiry policy
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expiryPolicy
     * @return
     * @see javax.cache.Cache#getAndPut(K,V)
     * @see java.util.concurrent.Future
     */
    V getAndPut(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * putAll with custom expiry policy
     * @param map mappings to be stored in this cache
     * @param expiryPolicy
     * @see javax.cache.Cache#putAll(java.util.Map)
     * @see java.util.concurrent.Future
     */
    void putAll(java.util.Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy);

    /**
     * putIfAbsent with custom expiry policy
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expiryPolicy
     * @return
     * @see javax.cache.Cache#putIfAbsent(K,V)
     * @see java.util.concurrent.Future
     */
    boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * replace with custom expiry policy
     * @param key      key with which the specified value is associated
     * @param oldValue value expected to be associated with the specified key
     * @param newValue value to be associated with the specified key
     * @param expiryPolicy
     * @return
     * @see javax.cache.Cache#replace(K,V,V)
     * @see java.util.concurrent.Future
     */
    boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    /**
     * replace with custom expiry policy
     * @param key  the key with which the specified value is associated
     * @param value the value to be associated with the specified key
     * @param expiryPolicy
     * @return
     */
    boolean replace(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * getAndReplace with custom expiry policy
     * @param key   key with which the specified value is associated
     * @param value value to be associated with the specified key
     * @param expiryPolicy
     * @return
     * @see javax.cache.Cache#getAndReplace(K,V)
     * @see java.util.concurrent.Future
     */
    V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion

    /**
     * total entry count
     * @return total entry count
     */
    int size();

    /**
     * Directly access to local Cache Statistics
     * @return CacheStatistics
     */
    CacheStatistics getLocalCacheStatistics();

}
