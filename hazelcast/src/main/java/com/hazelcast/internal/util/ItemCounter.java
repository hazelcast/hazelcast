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

package com.hazelcast.internal.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.sort;

/**
 * Non Thread-Safe Counter of things. It allows to count items without worrying about nulls.
 *
 * @param <T>
 */
public final class ItemCounter<T> {

    protected final Map<T, MutableLong> map = new HashMap<T, MutableLong>();
    private long total;

    /**
     * Returns the total counts.
     *
     * Complexity is O(1).
     *
     * @return total count.
     */
    public long total() {
        return total;
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
     * Returns a List of keys in descending value order.
     *
     * @return the list of keys
     */
    public List<T> descendingKeys() {
        List<T> list = new ArrayList<T>(map.keySet());

        sort(list, new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                MutableLong l1 = map.get(o1);
                MutableLong l2 = map.get(o2);
                return compare(l2.value, l1.value);
            }

            private int compare(long x, long y) {
                return (x < y) ? -1 : ((x == y) ? 0 : 1);
            }
        });

        return list;
    }

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
     * @param item  to set set the value for
     * @param value a new value
     */
    public void set(T item, long value) {
        MutableLong entry = map.get(item);
        if (entry == null) {
            entry = MutableLong.valueOf(value);
            map.put(item, entry);
            total += value;
        } else {
            total -= entry.value;
            total += value;
            entry.value = value;
        }
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
        MutableLong entry = map.get(item);
        if (entry == null) {
            entry = MutableLong.valueOf(delta);
            map.put(item, entry);
        } else {
            entry.value += delta;
        }
        total += delta;
    }

    /**
     * Reset state of the counter to 0.
     * It will <b>NOT</b> necessary remove all data referenced.
     *
     * Time complexity of this operation is O(n) where n is number of items.
     */
    public void reset() {
        for (MutableLong entry : map.values()) {
            entry.value = 0;
        }
        total = 0;
    }

    /**
     * Clears the counter.
     */
    public void clear() {
        map.clear();
        total = 0;
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
            total += value;
            return 0;
        }

        long oldValue = entry.value;
        total = total - oldValue + value;
        entry.value = value;
        return oldValue;
    }

    public void remove(T item) {
        MutableLong entry = map.remove(item);
        total -= entry == null ? 0 : entry.value;
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

    @Override
    public String toString() {
        return map.toString();
    }
}
