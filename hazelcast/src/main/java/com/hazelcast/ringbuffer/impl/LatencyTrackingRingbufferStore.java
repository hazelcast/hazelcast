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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.core.RingbufferStore;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;

/**
 * A {@link RingbufferStore} that decorates an RingbufferStore with latency tracking instrumentation.
 *
 * @param <T>
 */
class LatencyTrackingRingbufferStore<T> implements RingbufferStore<T> {
    static final String KEY = "RingbufferStoreLatency";

    private final LatencyProbe loadProbe;
    private final LatencyProbe getLargestSequenceProbe;
    private final LatencyProbe storeProbe;
    private final LatencyProbe storeAllProbe;
    private final RingbufferStore<T> delegate;

    LatencyTrackingRingbufferStore(RingbufferStore<T> delegate, StoreLatencyPlugin plugin, String ringbufferName) {
        this.delegate = delegate;
        this.loadProbe = plugin.newProbe(KEY, ringbufferName, "load");
        this.getLargestSequenceProbe = plugin.newProbe(KEY, ringbufferName, "getLargestSequence");
        this.storeProbe = plugin.newProbe(KEY, ringbufferName, "store");
        this.storeAllProbe = plugin.newProbe(KEY, ringbufferName, "storeAll");
    }

    @Override
    public void store(long sequence, T data) {
        long startNanos = System.nanoTime();
        try {
            delegate.store(sequence, data);
        } finally {
            storeProbe.recordValue(System.nanoTime() - startNanos);
        }
    }

    @Override
    public void storeAll(long firstItemSequence, T[] items) {
        long startNanos = System.nanoTime();
        try {
            delegate.storeAll(firstItemSequence, items);
        } finally {
            storeAllProbe.recordValue(System.nanoTime() - startNanos);
        }
    }

    @Override
    public T load(long sequence) {
        long startNanos = System.nanoTime();
        try {
            return delegate.load(sequence);
        } finally {
            loadProbe.recordValue(System.nanoTime() - startNanos);
        }
    }

    @Override
    public long getLargestSequence() {
        long startNanos = System.nanoTime();
        try {
            return delegate.getLargestSequence();
        } finally {
            getLargestSequenceProbe.recordValue(System.nanoTime() - startNanos);
        }
    }
}
