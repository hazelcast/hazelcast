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

package com.hazelcast.jet.datamodel;

import java.util.Objects;

/**
 * Mutable wrapper around an item that adds a timestamp. Useful for LRU
 * caching.
 *
 * @param <T>
 */
public class TimestampedItem<T> {
    private long timestamp;
    private T item;

    /**
     * Creates a new timestamped item.
     */
    public TimestampedItem(long timestamp, T item) {
        this.timestamp = timestamp;
        this.item = item;
    }

    /**
     * Returns the timestamp.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the item.
     */
    public T item() {
        return item;
    }

    /**
     * Sets the timestamp.
     *
     * @return {@code this}
     */
    public TimestampedItem<T> setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * Sets the item.
     *
     * @return {@code this}
     */
    public TimestampedItem<T> setItem(T item) {
        this.item = item;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimestampedItem<T> that = (TimestampedItem<T>) obj;
        return this.timestamp == that.timestamp
                && Objects.equals(this.item, that.item);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 37 * hc + Long.hashCode(timestamp);
        hc = 37 * hc + Objects.hashCode(item);
        return hc;
    }

    @Override
    public String toString() {
        return "TimestampedItem{ts=" + timestamp + ", item=" + item + '}';
    }
}
