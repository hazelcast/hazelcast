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
    private final Map<T, MutableLong> map = new HashMap<T, MutableLong>();

    /**
     * Get current counter for an item item
     *
     * @param item
     * @return current state of a counter for item
     */
    public long get(T item) {
        MutableLong count = map.get(item);
        return count == null ? 0 : count.value;
    }

    /**
     * Set counter of item to value
     *
     * @param item to set set the value for
     * @param value a new value
     */
    public void set(T item, long value) {
        MutableLong entry = map.get(item);
        if (entry == null) {
            entry = MutableLong.valueOf(value);
            map.put(item, entry);
        } else {
            entry.value = value;
        }
    }

    /**
     * Add delta to the item
     *
     * @param item
     * @param delta
     */
    public void add(T item, long delta) {
        MutableLong entry = map.get(item);
        if (entry == null) {
            entry = MutableLong.valueOf(delta);
            map.put(item, entry);
        } else {
            entry.value += delta;
        }
    }

    /**
     * Reset state of the counter to 0.
     * It will <b>NOT</b> necessary remove all data referenced.
     *
     * Time complexity of this operation is O(n) where n is number of items.
     *
     */
    public void reset() {
        for (MutableLong entry : map.values()) {
            entry.value = 0;
        }
    }

    /**
     * Set counter for item and return previous value
     *
     * @param item
     * @param value
     * @return
     */
    public long getAndSet(T item, long value) {
        MutableLong entry = map.get(item);

        if (entry == null) {
            entry = MutableLong.valueOf(value);
            map.put(item, entry);
            return 0;
        }

        long oldValue = entry.value;
        entry.value = value;
        return oldValue;
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
