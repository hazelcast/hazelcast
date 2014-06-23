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


import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.annotation.Beta;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

@Beta
public interface ICache<K, V> extends DistributedObject, javax.cache.Cache<K, V> {

    //region async extentions
    Future<V> getAsync(K key);

    Future<V> getAsync(K key, ExpiryPolicy expiryPolicy);

    Future<Void> putAsync(K key, V value);

    Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy);

    Future<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy);

    Future<V> getAndPutAsync(K key, V value);

    Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy);

    Future<Boolean> removeAsync(K key);

    Future<Boolean> removeAsync(K key, V oldValue);

    Future<V> getAndRemoveAsync(K key);

    Future<Boolean> replaceAsync(K key, V oldValue, V newValue);

    Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    Future<V> getAndReplaceAsync(K key, V value);

    Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion


    //region method with expirypolicy
    V get(K key, ExpiryPolicy expiryPolicy);

    Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy);

    void put(K key, V value, ExpiryPolicy expiryPolicy);

    V getAndPut(K key, V value, ExpiryPolicy expiryPolicy);

    void putAll(java.util.Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy);

    boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy);

    boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    boolean replace(K key, V value, ExpiryPolicy expiryPolicy);

    V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy);
    //endregion

    int size();

    CacheMXBean getCacheMXBean();

    CacheStatisticsMXBean getCacheStatisticsMXBean();

    void setStatisticsEnabled(boolean enabled);

//    CacheStats getStats();


}
