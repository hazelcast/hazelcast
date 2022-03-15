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

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;

public class LatencyTrackingCacheWriter<K, V> implements CacheWriter<K, V> {

    static final String KEY = "CacheStoreLatency";

    private final TenantContextual<CacheWriter<K, V>> delegate;
    private final LatencyProbe writeProbe;
    private final LatencyProbe writeAllProbe;
    private final LatencyProbe deleteProbe;
    private final LatencyProbe deleteAllProbe;

    public LatencyTrackingCacheWriter(TenantContextual<CacheWriter<K, V>> delegate, StoreLatencyPlugin plugin, String cacheName) {
        this.delegate = delegate;
        this.writeProbe = plugin.newProbe(KEY, cacheName, "write");
        this.writeAllProbe = plugin.newProbe(KEY, cacheName, "writeAll");
        this.deleteProbe = plugin.newProbe(KEY, cacheName, "delete");
        this.deleteAllProbe = plugin.newProbe(KEY, cacheName, "deleteAll");
    }

    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        long startNanos = Timer.nanos();
        try {
            delegate.get().write(entry);
        } finally {
            writeProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> collection) throws CacheWriterException {
        long startNanos = Timer.nanos();
        try {
            delegate.get().writeAll(collection);
        } finally {
            writeAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void delete(Object o) throws CacheWriterException {
        long startNanos = Timer.nanos();
        try {
            delegate.get().delete(o);
        } finally {
            deleteProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void deleteAll(Collection<?> collection) throws CacheWriterException {
        long startNanos = Timer.nanos();
        try {
            delegate.get().deleteAll(collection);
        } finally {
            deleteAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }
}
