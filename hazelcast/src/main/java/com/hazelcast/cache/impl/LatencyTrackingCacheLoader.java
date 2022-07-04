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

package com.hazelcast.cache.impl;

import com.hazelcast.spi.impl.tenantcontrol.TenantContextual;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;
import com.hazelcast.internal.util.Timer;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.Map;

public class LatencyTrackingCacheLoader<K, V> implements CacheLoader<K, V> {

    static final String KEY = "CacheLoaderLatency";

    private final TenantContextual<CacheLoader<K, V>> delegate;
    private final LatencyProbe loadProbe;
    private final LatencyProbe loadAllProbe;

    public LatencyTrackingCacheLoader(TenantContextual<CacheLoader<K, V>> delegate, StoreLatencyPlugin plugin, String cacheName) {
        this.delegate = delegate;
        this.loadProbe = plugin.newProbe(KEY, cacheName, "load");
        this.loadAllProbe = plugin.newProbe(KEY, cacheName, "loadAll");
    }

    @Override
    public V load(K k) throws CacheLoaderException {
        long startNanos = Timer.nanos();
        try {
            return delegate.get().load(k);
        } finally {
            loadProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> iterable) throws CacheLoaderException {
        long startNanos = Timer.nanos();
        try {
            return delegate.get().loadAll(iterable);
        } finally {
            loadAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }
}
