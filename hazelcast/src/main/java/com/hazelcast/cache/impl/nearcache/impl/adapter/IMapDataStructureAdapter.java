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

package com.hazelcast.cache.impl.nearcache.impl.adapter;

import com.hazelcast.core.IMap;

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
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return map.getAll(keys);
    }
}
