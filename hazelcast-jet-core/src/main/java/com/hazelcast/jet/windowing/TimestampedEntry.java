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
 * {@code Map.Entry} extended with a {@code long timestamp}.
 *
 * @param <K> type of grouping key
 * @param <V> type of the stream item
 */
public final class TimestampedEntry<K, V> implements Map.Entry<K, V> {
    private final long timestamp;
    private final K key;
    private final V value;

    /**
     * Constructs a timestamped entry with the supplied field values.
     */
    public TimestampedEntry(long timestamp, @Nonnull K key, @Nonnull V value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the timestamp of this entry.
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
        final TimestampedEntry that;
        return this == obj
                || obj instanceof TimestampedEntry
                && this.timestamp == (that = (TimestampedEntry) obj).timestamp
                && this.key.equals(that.key)
                && this.value.equals(that.value);
    }

    @Override
    public String toString() {
        return "TimestampedEntry{timestamp=" + timestamp + ", key=" + key + ", value=" + value + '}';
    }
}
