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

package com.hazelcast.internal.util.concurrent;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Thread-Safe Counter of things. It allows to count items without worrying about nulls.
 *
 * @param <T>
 */
public final class ConcurrentItemCounter<T> {

    protected final ConcurrentMap<T, AtomicLong> map = new ConcurrentHashMap<T, AtomicLong>();

    /**
     * Returns the total counts.
     */
    public long total() {
        return map.values().parallelStream().collect(Collectors.summingLong(AtomicLong::get));
    }

    /**
     * Returns an iterator over all keys.
     *
     * @return the key iterator.
     */
    public Set<T> keySet() {
        return map.keySet();
    }

    /**
     * Get current counter for an item item
     *
     * @param item
     * @return current state of a counter for item
     */
    public long get(T item) {
        AtomicLong count = map.get(item);
        return count == null ? 0 : count.get();
    }

    /**
     * Set counter of item to value
     *
     * @param item to set set the value for
     * @param value a new value
     */
    public void set(T item, long value) {
        getItemCounter(item).set(value);
    }

    /**
     * Increases the count by one for the given item.
     *
     * @param item
     */
    public void inc(T item) {
        add(item, 1);
    }

    /**
     * Add delta to the item
     *
     * @param item
     * @param delta
     */
    public void add(T item, long delta) {
        getItemCounter(item).addAndGet(delta);
    }

    /**
     * Reset state of the counter to 0.
     */
    public void reset() {
        clear();
    }

    /**
     * Clears the counter.
     */
    public void clear() {
        map.clear();
    }

    /**
     * Set counter for item and return previous value
     */
    public long getAndSet(T item, long value) {
        return getItemCounter(item).getAndSet(value);
    }

    public void remove(T item) {
        map.remove(item);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConcurrentItemCounter<?> that = (ConcurrentItemCounter<?>) o;
        if (!map.equals(that.map)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return map.toString();
    }

    private AtomicLong getItemCounter(T item) {
        return map.computeIfAbsent(item, k -> new AtomicLong());
    }

}
