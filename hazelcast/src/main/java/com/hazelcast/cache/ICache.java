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
 * Hazelcast {@link javax.cache.Cache} extension
 *<p>
 *     Hazelcast provides extension methods to {@link javax.cache.Cache}.
 * </p>
 * <p>
 *     There are three set of extensions:
 *     <ul>
 *         <li>asynchronous version of all cache operations.</li>
 *         <li>cache operations with custom ExpiryPolicy parameter to apply on that specific operation.</li>
 *         <li>uncategorized like {@link #size()}.</li>
 *     </ul>
 *</p>
 *<p>
 *     A method ending with Async is the asynchronous version of that method (for example {@link #getAsync(K)},
 *     {@link #replaceAsync(K,V)} ).<br/>
 *     These methods return a Future where you can get the result or wait for the operation to be completed.
 *
 *     <pre>
 *         <code>ICache&lt;String , SessionData&gt; icache =  cache.unwrap( ICache.class );
 *         Future&lt;SessionData&gt; future = icache.getAsync(&quot;key-1&quot; ) ;
 *         SessionData sessionData = future.get();
 *         </code>
 *     </pre>
 *</p>
 *<p>
 *     This interface can be accessed through {@link javax.cache.Cache#unwrap(Class)}.
 *</p>
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see javax.cache.Cache
 * @since 3.3.1
 */
public interface ICache<K, V>
        extends javax.cache.Cache<K, V> {

    //region async extentions

    /**
     * Asynchronously gets an entry from cache.
     *
     * @param key the key whose associated value is to be returned.
     * @return Future from which the value of the key can be retrieved.
     * @see javax.cache.Cache#get(K)
     * @see java.util.concurrent.Future
     */
    Future<V> getAsync(K key);

    /**
     * Asynchronously gets an entry from cache with a provided expiry policy.
     *
     * @param key the key whose associated value is to be returned.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return Future from which the value of the key can be retrieved.
     * @see javax.cache.Cache#get(K)
     * @see java.util.concurrent.Future
     */
    Future<V> getAsync(K key, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in the cache.
     *
     * @param key the key whose associated value is to be returned.
     * @param value the value to be associated with the specified key.
     * @return Future
     * @see javax.cache.Cache#put(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Void> putAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in the cache using a
     * custom expiry policy.
     *
     * @param key the key whose associated value is to be returned.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return Future
     * @see javax.cache.Cache#put(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in the cache if not already exist,
     * using a custom expiry policy.
     * @param key   the key with which the specified value is to be associated.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#putIfAbsent(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * @param key the key whose associated value is to be returned.
     * @param value the value to be associated with the specified key.
     * @return Future
     * @see javax.cache.Cache#getAndPut(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndPutAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed using a custom expiry policy.
     * @param key the key whose associated value is to be returned.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return Future
     * @see javax.cache.Cache#getAndPut(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously removes the mapping for a key from this cache if it is present.
     * @param key the key whose associated value is to be returned.
     * @return Future
     * @see javax.cache.Cache#remove(K)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> removeAsync(K key);

    /**
     * Asynchronously removes the mapping for a key only if it is currently mapped to the
     * given value.
     * @param key the key whose associated value is to be returned.
     * @param oldValue the value expected to be associated with the specified key.
     * @return Future
     * @see javax.cache.Cache#remove(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> removeAsync(K key, V oldValue);

    /**
     * Asynchronously removes the entry for a key returning the removed value if one existed.
     *
     * @param key the key whose associated value is to be returned.
     * @return Future
     * @see javax.cache.Cache#getAndRemove(K)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndRemoveAsync(K key);

    /**
     * Asynchronously replaces the entry for a key.
     *
     * @param key the key whose associated value is to be returned.
     * @param value the value to be associated with the specified key.
     * @return Future
     * @see javax.cache.Cache#replace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V value);

    /**
     * Asynchronously replaces the entry for a key only if it is currently mapped to some
     * value.
     * @param key  the key with which the specified value is associated.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#replace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously replaces the entry for a key only if it is currently mapped to a
     * given value.
     * @param key     the key with which the specified value is associated.
     * @param oldValue the value expected to be associated with the specified key.
     * @param newValue the value to be associated with the specified key.
     * @return
     * @see javax.cache.Cache#replace(K,V,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V oldValue, V newValue);

    /**
     * Asynchronously replaces the entry for a key only if it is currently mapped to a
     * given value using custom expiry policy.
     * @param key      the key with which the specified value is associated.
     * @param oldValue the value expected to be associated with the specified key.
     * @param newValue the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#replace(K,V,V)
     * @see java.util.concurrent.Future
     */
    Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);


    /**
     * Asynchronously replaces the entry for a key only if it is currently mapped to some value.
     * @param key  the key with which the specified value is associated.
     * @param value the value to be associated with the specified key.
     * @return
     * @see javax.cache.Cache#getAndReplace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndReplaceAsync(K key, V value);

    /**
     * Asynchronously replaces the entry for a key only if it is currently mapped to some value
     * using custom expiry policy.
     * @param key  the key with which the specified value is associated.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#getAndReplace(K,V)
     * @see java.util.concurrent.Future
     */
    Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion

    //region method with expirypolicy

    /**
     * Gets a key with custom expiry policy.
     * @param key the key whose associated value is to be returned.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#get(K)
     * @see java.util.concurrent.Future
     */
    V get(K key, ExpiryPolicy expiryPolicy);

    /**
     * getAll operation with custom expiry policy.
     * @param keys the keys whose associated values are to be returned.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#getAll(java.util.Set)
     * @see java.util.concurrent.Future
     */
    Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy);

    /**
     * put operation with custom expiry policy.
     * @param key   the key with which the specified value is to be associated.
     * @param value value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @see javax.cache.Cache#put(K,V)
     * @see java.util.concurrent.Future
     */
    void put(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * getAndPut operation with custom expiry policy.
     * @param key   the key with which the specified value is to be associated.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#getAndPut(K,V)
     * @see java.util.concurrent.Future
     */
    V getAndPut(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * putAll operation with custom expiry policy.
     * @param map the mappings to be stored in this cache.
     * @param expiryPolicy custom expiry policy for this operation.
     * @see javax.cache.Cache#putAll(java.util.Map)
     * @see java.util.concurrent.Future
     */
    void putAll(java.util.Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy);

    /**
     * putIfAbsent operation with custom expiry policy.
     * @param key   the key with which the specified value is to be associated.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#putIfAbsent(K,V)
     * @see java.util.concurrent.Future
     */
    boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * replace operation with custom expiry policy.
     * @param key      the key with which the specified value is associated.
     * @param oldValue the value expected to be associated with the specified key.
     * @param newValue the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#replace(K,V,V)
     * @see java.util.concurrent.Future
     */
    boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    /**
     * replace operation with custom expiry policy.
     * @param key  the key with which the specified value is associated.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     */
    boolean replace(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * getAndReplace operation with custom expiry policy.
     * @param key   the key with which the specified value is associated.
     * @param value the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy for this operation.
     * @return
     * @see javax.cache.Cache#getAndReplace(K,V)
     * @see java.util.concurrent.Future
     */
    V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion

    /**
     * Total entry count.
     * @return total entry count
     */
    int size();

    /**
     * Closes the cache, clears the internal content and releases any resource.
     *
     * @see javax.cache.CacheManager#destroyCache(String)
     */
    void destroy();

    /**
     * Directly access to local Cache Statistics.
     * @return CacheStatistics
     */
    CacheStatistics getLocalCacheStatistics();

}
