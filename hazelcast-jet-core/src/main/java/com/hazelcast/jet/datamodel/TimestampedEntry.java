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

package com.hazelcast.jet.datamodel;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;

/**
 * A {@code Map.Entry} extended with a {@code long timestamp}, used for
 * event time-based data processing.
 *
 * @param <K> type of grouping key
 * @param <V> type of the stream item
 */
public final class TimestampedEntry<K, V> implements Map.Entry<K, V> {
    private final long timestamp;
    @Nonnull
    private final K key;
    @Nonnull
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
     * This constructor exists in order to match the shape of the functional
     * interface {@link com.hazelcast.jet.function.KeyedWindowResultFunction
     * KeyedWindowResultFunction}.
     * <p>
     * Constructs a timestamped entry with the supplied field values. Ignores
     * the first argument.
     */
    public TimestampedEntry(long ignored, long timestamp, @Nonnull K key, @Nonnull V value) {
        this(timestamp, key, value);
    }

    /**
     * Returns the timestamp of this entry.
     */
    public long getTimestamp() {
        return timestamp;
    }

    @Nonnull @Override
    public K getKey() {
        return key;
    }

    @Nonnull @Override
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
        return "TimestampedEntry{ts=" + timestamp
                + ", formattedTs=" + toLocalDateTime(timestamp)
                + ", key=" + key +
                ", value=" + value + '}';
    }
}
