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
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.monitor.LocalMapStats;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ReplicatedMapDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final ReplicatedMap<K, V> map;

    public ReplicatedMapDataStructureAdapter(ReplicatedMap<K, V> map) {
        this.map = map;
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public void set(K key, V value) {
        map.put(key, value);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    @MethodNotAvailable
    public boolean putIfAbsent(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public V replace(K key, V newValue) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public boolean replace(K key, V oldValue, V newValue) {
        throw new MethodNotAvailableException();
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    @MethodNotAvailable
    public ICompletableFuture<V> getAsync(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    public void putAll(Map<K, V> map) {
        this.map.putAll(map);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        Map<K, V> result = new HashMap<K, V>(keys.size());
        for (K key : keys) {
            result.put(key, map.get(key));
        }
        return result;
    }

    public void remove(K key) {
        map.remove(key);
    }

    @Override
    @MethodNotAvailable
    public boolean remove(K key, V oldValue) {
        throw new MethodNotAvailableException();
    }

    @Override
    @MethodNotAvailable
    public ICompletableFuture<V> removeAsync(K key) {
        throw new MethodNotAvailableException();
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }
}
