/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Non Thread-Safe Counter of things. It allows to count items without worrying about nulls.
 *
 * @param <T>
 */
public final class ItemCounter<T> {
    private final Map<T, Long> map = new HashMap<T, Long>();

    /**
     * Get current counter for an item item
     *
     * @param item
     * @return current state of a counter for item
     */
    public long get(T item) {
        Long count = map.get(item);
        return count == null ? 0 : count;
    }

    /**
     * Set counter of item to value
     *
     * @param item to set set the value for
     * @param value a new value
     */
    public void set(T item, long value) {
        map.put(item, value);
    }

    /**
     * Add delta to the item
     *
     * @param item
     * @param delta
     */
    public void add(T item, long delta) {
        Long count = map.get(item);
        if (count == null) {
            count = delta;
        } else {
            count += delta;
        }
        map.put(item, count);
    }

    /**
     * Reset state of the counter
     *
     */
    public void reset() {
        map.clear();
    }

    /**
     * Set counter for item and return previous value
     *
     * @param item
     * @param value
     * @return
     */
    public long getAndSet(T item, long value) {
        Long count = map.get(item);
        if (count == null) {
            count = 0L;
        }
        map.put(item, value);
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ItemCounter that = (ItemCounter) o;

        if (!map.equals(that.map)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }
}
