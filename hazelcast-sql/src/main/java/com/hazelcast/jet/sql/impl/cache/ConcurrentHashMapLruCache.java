/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.cache;

import javax.annotation.concurrent.GuardedBy;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * Implementation of a simple LRU cache based on {@link ConcurrentHashMap}. It is faster than Guava cache when
 * there are a lot of reads and few writes.
 */
public class ConcurrentHashMapLruCache<K, V> implements Cache<K, V> {
    // Package-private scope for tests
    final Map<K, V> cache;
    final Deque<K> keyQueue = new ConcurrentLinkedDeque<>();

    // This is the main part why this implementation is better than Guava cache. Guava uses ReentrantLock, this
    // implementation uses ReentrantReadWriteLock which allows multiple readers.
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final int maxCapacity;
    private final int fastPathMaxCapacity;

    // Need to be volatile - it is used without any other guard
    private volatile int size;

    public ConcurrentHashMapLruCache(int maxCapacity) {
        this(maxCapacity, 1);
    }

    /**
     * @param fastPathMaxCapacity to which size of the cache we should get values from cache without updating
     *                            the key queue.
     */
    public ConcurrentHashMapLruCache(int maxCapacity, int fastPathMaxCapacity) {
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity <= 0");
        }
        if (fastPathMaxCapacity >= maxCapacity) {
            throw new IllegalArgumentException("fastPathMaxCapacity >= maxCapacity");
        }

        this.fastPathMaxCapacity = fastPathMaxCapacity;
        this.maxCapacity = maxCapacity;
        this.cache = new ConcurrentHashMap<>(maxCapacity);
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> valueFunction) {
        V valueFromCache = cache.get(key);

        if (valueFromCache != null) {
            if (size <= fastPathMaxCapacity) { // No need to manipulate Deque
                return valueFromCache;
            }

            lock.readLock().lock();
            try {
                moveToTheEndOfADeque(key);
                return valueFromCache;
            } finally {
                lock.readLock().unlock();
            }
        }

        lock.writeLock().lock();
        try {
            // Same as DCL - we need to recheck if we have value at cache.
            valueFromCache = cache.get(key);
            if (valueFromCache != null) {
                moveToTheEndOfADeque(key);
                return valueFromCache;
            }

            return handleNewCacheEntry(key, valueFunction);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @GuardedBy("lock") // write
    private V handleNewCacheEntry(K key, Function<K, V> valueFunction) {
        if (cache.size() == this.maxCapacity) {
            removeLruKey();
        }

        V value = valueFunction.apply(key);
        keyQueue.offer(key);
        cache.put(key, value);
        size = cache.size();

        return value;
    }

    @GuardedBy("lock") // write
    private void removeLruKey() {
        K lruKey = keyQueue.poll();
        cache.remove(lruKey);
    }

    @GuardedBy("lock") // read or write
    private void moveToTheEndOfADeque(K key) {
        if (keyQueue.removeLastOccurrence(key)) {
            keyQueue.offer(key);
        }
        // No else: key might be already removed by handleNewCacheEntry before acquiring read lock, so since we
        // don't always have a write-lock, just stay removed from the cache. Two threads may also run computeIfAbsent
        // method for the same key, only one succeed with removeLastOccurrence.
    }
}
