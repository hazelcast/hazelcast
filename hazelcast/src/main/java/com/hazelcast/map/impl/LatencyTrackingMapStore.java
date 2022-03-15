/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.internal.util.Timer;
import com.hazelcast.map.MapStore;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;

import java.util.Collection;
import java.util.Map;

public class LatencyTrackingMapStore<K, V> implements MapStore<K, V> {
    static final String KEY = "MapStoreLatency";

    private final LatencyProbe deleteProbe;
    private final LatencyProbe deleteAllProbe;
    private final LatencyProbe storeProbe;
    private final LatencyProbe storeAllProbe;
    private final MapStore<K, V> delegate;

    public LatencyTrackingMapStore(MapStore<K, V> delegate, StoreLatencyPlugin plugin, String mapName) {
        this.delegate = delegate;
        this.deleteProbe = plugin.newProbe(KEY, mapName, "delete");
        this.deleteAllProbe = plugin.newProbe(KEY, mapName, "deleteAll");
        this.storeProbe = plugin.newProbe(KEY, mapName, "store");
        this.storeAllProbe = plugin.newProbe(KEY, mapName, "storeAll");
    }

    @Override
    public V load(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Iterable<K> loadAllKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void store(K key, V value) {
        long startNanos = Timer.nanos();
        try {
            delegate.store(key, value);
        } finally {
            storeProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void storeAll(Map<K, V> map) {
        long startNanos = Timer.nanos();
        try {
            delegate.storeAll(map);
        } finally {
            storeAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void delete(K key) {
        long startNanos = Timer.nanos();
        try {
            delegate.delete(key);
        } finally {
            deleteProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        long startNanos = Timer.nanos();
        try {
            delegate.deleteAll(keys);
        } finally {
            deleteAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

}
