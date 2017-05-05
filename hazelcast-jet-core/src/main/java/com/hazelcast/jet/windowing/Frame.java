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

package com.hazelcast.jet.windowing;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Container of items from a fixed-length timestamp interval. Holds
 * intermediate computation results before the final step of forming the
 * user-visible window.
 *
 * @param <K> type of grouping key
 * @param <V> type of the stream item
 */
public final class Frame<K, V> implements Map.Entry<K, V> {
    private final long timestamp;
    private final K key;
    private final V value;

    /**
     * Constructs a frame with the supplied field values.
     * @param timestamp {@link #getTimestamp()}
     * @param key {@link #getKey()}
     * @param value {@link #getValue()}
     */
    public Frame(long timestamp, @Nonnull K key, @Nonnull V value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the timestamp label of this frame (the lowest timestamp
     * not belonging to it).
     */
    public long getTimestamp() {
        return timestamp;
    }

    @Override @Nonnull
    public K getKey() {
        return key;
    }

    @Override @Nonnull
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + Long.hashCode(timestamp);
        hc = 73 * hc + key.hashCode();
        hc = 73 * hc + value.hashCode();
        return hc;
    }

    @Override
    public boolean equals(Object obj) {
        final Frame that;
        return this == obj
                || obj instanceof Frame
                && this.timestamp == (that = (Frame) obj).timestamp
                && this.key.equals(that.key)
                && this.value.equals(that.value);
    }

    @Override
    public String toString() {
        return "Frame{timestamp=" + timestamp + ", key=" + key + ", value=" + value + '}';
    }
}
