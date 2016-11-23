/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.monitor.LocalMapStats;

import javax.cache.Cache;
import java.util.Map;
import java.util.Set;

public class ICacheDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final Cache<K, V> cache;

    public ICacheDataStructureAdapter(Cache<K, V> cache) {
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
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return new SimpleCompletedFuture<V>(cache.get(key));
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
    public LocalMapStats getLocalMapStats() {
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }
}
