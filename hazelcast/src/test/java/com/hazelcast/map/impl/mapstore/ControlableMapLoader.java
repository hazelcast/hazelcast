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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.MapLoader;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A controllable wrapper around a {@link MapLoader} for testing purposes.
 * <p>
 * Supports pausing/resuming key and value loading, counting loaded entries,
 * and asserting load behavior.
 */
public class ControlableMapLoader<K, V> implements MapLoader<K, V> {

    private volatile MapLoader<K, V> delegate;

    // Flags to control pausing of loading operations
    private volatile boolean stopKeyLoading;
    private volatile boolean stopValueLoading;

    // Latches for coordinating pauses and resumes during tests
    private volatile CountDownLatch loadValuesLatch;
    private volatile CountDownLatch loadKeyLatch = new CountDownLatch(1);

    private volatile CountDownLatch resumeKeyLatch = new CountDownLatch(1);
    private volatile CountDownLatch pauseKeyLatch = new CountDownLatch(1);

    private volatile CountDownLatch resumeValueLatch = new CountDownLatch(1);
    private volatile CountDownLatch pauseValueLatch = new CountDownLatch(1);

    private final AtomicInteger loadedValueCount = new AtomicInteger(0);
    private final AtomicInteger loadAllKeysInvocations = new AtomicInteger(0);

    public ControlableMapLoader(MapLoader<K, V> delegate, int loadValuesLatchCount) {
        this.delegate = delegate;
        this.loadValuesLatch = new CountDownLatch(loadValuesLatchCount);
    }

    public ControlableMapLoader(MapLoader<K, V> delegate) {
        this(delegate, 0);
    }

    @Override
    public V load(K key) {
        return delegate.load(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        loadValuesLatch.countDown();
        loadedValueCount.addAndGet(keys.size());

        if (stopValueLoading) {
            pause(pauseValueLatch, resumeValueLatch);
        }

        return delegate.loadAll(keys);
    }

    @Override
    public Iterable<K> loadAllKeys() {
        loadKeyLatch.countDown();
        loadAllKeysInvocations.incrementAndGet();

        if (stopKeyLoading) {
            pause(pauseKeyLatch, resumeKeyLatch);
        }

        return delegate.loadAllKeys();
    }

    /**
     * Resets counters, latches, and optionally the delegate for reuse.
     */
    public void reset(int valueLatchCount) {
        reset(valueLatchCount, 1);
    }

    public void reset(int valueLatchCount, int pauseValueLatch) {
        this.resumeKeyLatch = new CountDownLatch(1);
        this.pauseKeyLatch = new CountDownLatch(1);
        this.resumeValueLatch = new CountDownLatch(1);
        this.pauseValueLatch = new CountDownLatch(pauseValueLatch);
        this.loadAllKeysInvocations.set(0);
        this.loadedValueCount.set(0);
        this.loadValuesLatch = new CountDownLatch(valueLatchCount);
        this.loadKeyLatch = new CountDownLatch(1);
    }

    public void reset() {
        reset(0);
    }

    public void reset(MapLoader<K, V> delegate) {
        reset();
        this.delegate = delegate;
    }

    /** Pause/resume controls for key loading */
    public void pauseKeyLoading() {
        this.stopKeyLoading = true;
    }

    public void awaitKeyLoadPaused() throws InterruptedException {
        pauseKeyLatch.await();
    }

    public void resumeKeyLoading() {
        resumeKeyLatch.countDown();
    }

    /** Pause/resume controls for value loading */
    public void pauseValueLoading(int count) {
        pauseValueLatch = new CountDownLatch(count);
        this.stopValueLoading = true;
    }

    public void pauseValueLoading() {
        pauseValueLatch = new CountDownLatch(1);
        this.stopValueLoading = true;
    }

    public void awaitValueLoadPaused() throws InterruptedException {
        pauseValueLatch.await();
    }

    public void resumeValueLoading() {
        resumeValueLatch.countDown();
    }

    /** Helper method to handle pausing until resumed */
    private void pause(CountDownLatch pause, CountDownLatch resume) {
        try {
            pause.countDown();
            resume.await();
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    /** Assertion helpers for tests */
    public void assertNoFullLoadTriggered() {
        assertFullLoadCountEquals(0);
    }

    public void assertFullLoadTriggeredOnce() {
        assertFullLoadCountEquals(1);
    }

    public void assertFullLoadCountEquals(int expectedCount) {
        assertThat(loadAllKeysInvocations.get())
                .as("Expected loadAllKeys to be called %d times, but was %d", expectedCount, loadAllKeysInvocations.get())
                .isEqualTo(expectedCount);
    }

    public void assertLoadedEntriesCount(int expectedCount) {
        assertThat(loadedValueCount.get())
                .as("Expected loadAll to load %d entries, but was %d", expectedCount, loadedValueCount.get())
                .isEqualTo(expectedCount);
    }
}
