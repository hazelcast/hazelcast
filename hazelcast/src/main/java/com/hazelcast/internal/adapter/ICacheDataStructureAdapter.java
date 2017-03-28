/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.adapter;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.monitor.LocalMapStats;

import java.util.Map;
import java.util.Set;

public class ICacheDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final ICache<K, V> cache;

    public ICacheDataStructureAdapter(ICache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void set(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public V put(K key, V value) {
        return cache.getAndPut(key, value);
    }

    @Override
    public V replace(K key, V newValue) {
        return cache.getAndReplace(key, newValue);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return cache.replace(key, oldValue, newValue);
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return cache.getAsync(key);
    }

    @Override
    public void putAll(Map<K, V> map) {
        cache.putAll(map);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return cache.getAll(keys);
    }

    @Override
    public void remove(K key) {
        cache.remove(key);
    }

    @Override
    public ICompletableFuture<V> removeAsync(K key) {
        return cache.getAndRemoveAsync(key);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }
}
