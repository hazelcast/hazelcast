/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MapLoader;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;

import java.util.Collection;
import java.util.Map;

public class LatencyTrackingMapLoader<K, V> implements MapLoader<K, V> {

    static final String KEY = "MapStoreLatency";

    private final LatencyProbe loadProbe;
    private final LatencyProbe loadAllKeysProbe;
    private final LatencyProbe loadAllProbe;
    private final MapLoader<K, V> delegate;

    public LatencyTrackingMapLoader(MapLoader<K, V> delegate, StoreLatencyPlugin plugin, String mapName) {
        this.delegate = delegate;
        this.loadProbe = plugin.newProbe(KEY, mapName, "load");
        this.loadAllProbe = plugin.newProbe(KEY, mapName, "loadAll");
        this.loadAllKeysProbe = plugin.newProbe(KEY, mapName, "loadAllKeys");
    }

    @Override
    public V load(K key) {
        long startNanos = System.nanoTime();
        try {
            return delegate.load(key);
        } finally {
            loadProbe.recordValue(System.nanoTime() - startNanos);
        }
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        long startNanos = System.nanoTime();
        try {
            return delegate.loadAll(keys);
        } finally {
            loadAllProbe.recordValue(System.nanoTime() - startNanos);
        }
    }

    @Override
    public Iterable<K> loadAllKeys() {
        long startNanos = System.nanoTime();
        try {
            return delegate.loadAllKeys();
        } finally {
            loadAllKeysProbe.recordValue(System.nanoTime() - startNanos);
        }
    }
}
