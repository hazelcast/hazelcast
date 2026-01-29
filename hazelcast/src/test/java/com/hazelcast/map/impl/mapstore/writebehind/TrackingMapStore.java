/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.hazelcast.map.MapStore;

public class TrackingMapStore<K, V> implements MapStore<K, V> {

    private final Map<K, V> store = new ConcurrentHashMap<>();

    private final Map<K, V> expectedWrites = new ConcurrentHashMap<>();

    private long lastWrite = System.currentTimeMillis();

    @Override
    public V load(K key) {
        return store.get(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        return keys.stream().collect(Collectors.toMap(Function.identity(), this::load));
    }

    @Override
    public Iterable<K> loadAllKeys() {
        return store.keySet();
    }

    @Override
    public void store(K key, V value) {
        store.put(key, value);
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        expectedWrites.computeIfPresent(key, (k, v) -> Objects.equal(v, value) ? null : v);
        lastWrite = System.currentTimeMillis();
    }

    @Override
    public void storeAll(Map<K, V> map) {
        map.forEach(this::store);
    }

    @Override
    public void delete(K key) {
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        keys.forEach(this::delete);
    }

    public void expectWrite(K key, V value) {
        expectedWrites.put(key, value);
    }

    public boolean allExpectedWritesComplete() {
        return expectedWrites.isEmpty();
    }

    public boolean lastWriteAtLeastMsAgo(long ms) {
        return lastWrite + ms < System.currentTimeMillis();
    }
}
