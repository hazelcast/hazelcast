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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.QueueStore;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;
import com.hazelcast.internal.util.Timer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LatencyTrackingQueueStore<T> implements QueueStore<T> {
    static final String KEY = "QueueStoreLatency";

    private final LatencyProbe loadProbe;
    private final LatencyProbe loadAllKeysProbe;
    private final LatencyProbe loadAllProbe;
    private final LatencyProbe deleteProbe;
    private final LatencyProbe deleteAllProbe;
    private final LatencyProbe storeProbe;
    private final LatencyProbe storeAllProbe;
    private final QueueStore<T> delegate;

    public LatencyTrackingQueueStore(QueueStore<T> delegate, StoreLatencyPlugin plugin, String queueName) {
        this.delegate = delegate;
        this.loadProbe = plugin.newProbe(KEY, queueName, "load");
        this.loadAllProbe = plugin.newProbe(KEY, queueName, "loadAll");
        this.loadAllKeysProbe = plugin.newProbe(KEY, queueName, "loadAllKeys");
        this.deleteProbe = plugin.newProbe(KEY, queueName, "delete");
        this.deleteAllProbe = plugin.newProbe(KEY, queueName, "deleteAll");
        this.storeProbe = plugin.newProbe(KEY, queueName, "store");
        this.storeAllProbe = plugin.newProbe(KEY, queueName, "storeAll");
    }

    @Override
    public void store(Long key, T value) {
        long startNanos = Timer.nanos();
        try {
            delegate.store(key, value);
        } finally {
            storeProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void storeAll(Map<Long, T> map) {
        long startNanos = Timer.nanos();
        try {
            delegate.storeAll(map);
        } finally {
            storeAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void delete(Long key) {
        long startNanos = Timer.nanos();
        try {
            delegate.delete(key);
        } finally {
            deleteProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public void deleteAll(Collection<Long> keys) {
        long startNanos = Timer.nanos();
        try {
            delegate.deleteAll(keys);
        } finally {
            deleteAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public T load(Long key) {
        long startNanos = Timer.nanos();
        try {
            return delegate.load(key);
        } finally {
            loadProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public Map<Long, T> loadAll(Collection<Long> keys) {
        long startNanos = Timer.nanos();
        try {
            return delegate.loadAll(keys);
        } finally {
            loadAllProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public Set<Long> loadAllKeys() {
        long startNanos = Timer.nanos();
        try {
            return delegate.loadAllKeys();
        } finally {
            loadAllKeysProbe.recordValue(Timer.nanosElapsed(startNanos));
        }
    }
}
