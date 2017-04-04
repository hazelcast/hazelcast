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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;

import java.util.Map;
import java.util.Set;

public class IMapDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final IMap<K, V> map;

    public IMapDataStructureAdapter(IMap<K, V> map) {
        this.map = map;
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public void set(K key, V value) {
        map.set(key, value);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return map.putIfAbsent(key, value) == null;
    }

    @Override
    @MethodNotAvailable
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    public V replace(K key, V newValue) {
        return map.replace(key, newValue);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return map.replace(key, oldValue, newValue);
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return map.getAsync(key);
    }

    @Override
    public void putAll(Map<K, V> map) {
        this.map.putAll(map);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return map.getAll(keys);
    }

    @Override
    public void remove(K key) {
        map.remove(key);
    }

    @Override
    public boolean remove(K key, V oldValue) {
        return map.remove(key, oldValue);
    }

    @Override
    public ICompletableFuture<V> removeAsync(K key) {
        return map.removeAsync(key);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return map.getLocalMapStats();
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }
}
