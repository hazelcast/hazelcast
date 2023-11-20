/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;

/**
 * Implementation of an LRU cache optimized for read-heavy use cases.
 * <p>
 * It stores the entries in a {@link ConcurrentHashMap}, along with the last
 * access time. It allows the size to grow beyond the capacity, up to
 * `cleanupThreshold`, at which point the inserting thread will remove a batch
 * of the eldest items in two passes.
 * <p>
 * The cleanup process isn't synchronized to guarantee that the capacity is not
 * exceeded. The cache is available during the cleanup for reads and writes. If
 * there's a large number of writes by many threads, the one thread doing the
 * cleanup might not be quick enough and there's no upper bound on the actual
 * size of the cache. This is done to optimize the happy path when the keys fit
 * into the cache.
 */
public class ReadOptimizedLruCache<K, V> {

    // package-visible for tests
    final ConcurrentMap<K, ValueAndTimestamp<V>> cache;

    private final AtomicBoolean cleanupLock = new AtomicBoolean();
    private final int capacity;
    private final int cleanupThreshold;

    /**
     * @param cleanupThreshold The size at which the cache will clean up oldest
     *     entries in batch. `cleanupThreshold - capacity` entries will be removed.
     */
    public ReadOptimizedLruCache(int capacity, int cleanupThreshold) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity <= 0");
        }
        if (cleanupThreshold <= capacity) {
            throw new IllegalArgumentException("cleanupThreshold <= capacity");
        }

        this.capacity = capacity;
        this.cleanupThreshold = cleanupThreshold;

        cache = new ConcurrentHashMap<>(cleanupThreshold);
    }

    public V getOrDefault(K key, V defaultValue) {
        final V existingValue = get(key);
        return existingValue != null ? existingValue : defaultValue;
    }

    public V get(K key) {
        ValueAndTimestamp<V> valueFromCache = cache.get(key);
        if (valueFromCache == null) {
            return null;
        }
        valueFromCache.touch();
        return valueFromCache.value;
    }

    public void put(K key, V value) {
        if (value == null) {
            throw new IllegalArgumentException("Null values are disallowed");
        }

        ValueAndTimestamp<V> oldValue = cache.put(key, new ValueAndTimestamp<>(value));
        if (oldValue == null && cache.size() > cleanupThreshold) {
            doCleanup();
        }
    }

    /**
     * Checks the existence of {@code key} and puts {@code mappingFn(key)} if not
     * exists, in a non-atomic way. It does not block the callers and is free from
     * deadlocks unlike {@link ConcurrentHashMap#computeIfAbsent}. However, it may
     * overwrite a just-put entry or skip computing a just-removed key.
     */
    public V computeIfAbsent(K key, Function<K, V> mappingFn) {
        V value = get(key);
        if (value != null) {
            return value;
        }
        value = mappingFn.apply(key);
        put(key, value);
        return value;
    }

    public void remove(K key) {
        cache.remove(key);
    }

    private void doCleanup() {
        // if no thread is cleaning up, we'll do it
        if (!cleanupLock.compareAndSet(false, true)) {
            return;
        }

        try {
            int entriesToRemove = cache.size() - capacity;
            if (entriesToRemove <= 0) {
                // this can happen if the cache is concurrently modified
                return;
            }
            PriorityQueue<Long> oldestTimestamps =
                    new PriorityQueue<>(entriesToRemove + 1, Comparator.<Long>naturalOrder().reversed());

            // 1st pass
            for (ValueAndTimestamp<V> valueAndTimestamp : cache.values()) {
                oldestTimestamps.add(valueAndTimestamp.timestamp);
                if (oldestTimestamps.size() > entriesToRemove) {
                    oldestTimestamps.poll();
                }
            }

            // find out the highest value in the queue - the value, below which entries will be removed
            if (oldestTimestamps.isEmpty()) {
                // this can happen if the cache is concurrently modified
                return;
            }
            long removeThreshold = oldestTimestamps.poll();

            // 2nd pass
            cache.values().removeIf(v -> v.timestamp <= removeThreshold);
        } finally {
            cleanupLock.set(false);
        }
    }

    // package-visible for tests
    static class ValueAndTimestamp<V> {
        private static final AtomicLongFieldUpdater<ValueAndTimestamp> TIMESTAMP_UPDATER =
                AtomicLongFieldUpdater.newUpdater(ValueAndTimestamp.class, "timestamp");

        final V value;
        volatile long timestamp;

        ValueAndTimestamp(V value) {
            this.value = value;
            touch();
        }

        public void touch() {
            TIMESTAMP_UPDATER.lazySet(this, System.nanoTime());
        }
    }
}
