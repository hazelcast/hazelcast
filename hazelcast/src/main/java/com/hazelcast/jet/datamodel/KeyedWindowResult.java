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

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * Holds the result of a group-and-aggregate operation performed over a
 * time window.
 *
 * @param <K> type of the grouping key
 * @param <R> type of the aggregated result
 *
 * @since Jet 3.0
 */
public final class KeyedWindowResult<K, R> extends WindowResult<R> implements Entry<K, R> {
    private final K key;

    /**
     * @param start   start time of the window
     * @param end     end time of the window
     * @param key     grouping key
     * @param result  result of aggregation
     * @param isEarly whether this is an early result, to be followed by the final one
     */
    public KeyedWindowResult(long start, long end, @Nonnull K key, @Nonnull R result, boolean isEarly) {
        super(start, end, result, isEarly);
        this.key = key;
    }

    /**
     * Constructs a keyed window result that is not early.
     *
     * @param start   start time of the window
     * @param end     end time of the window
     * @param key     grouping key
     * @param result  result of aggregation
     */
    public KeyedWindowResult(long start, long end, @Nonnull K key, @Nonnull R result) {
        this(start, end, key, result, false);
    }

    /**
     * Returns the grouping key.
     */
    @Nonnull
    public K key() {
        return key;
    }

    /**
     * Alias for {@link #key}, implements {@code Map.Entry}.
     */
    @Override
    public K getKey() {
        return key;
    }

    /**
     * Alias for {@link #result()}, implements {@code Map.Entry}.
     */
    @Override
    public R getValue() {
        return result();
    }

    /**
     * Implements {@code Map.Entry}, throws {@code
     * UnsupportedOperationException}.
     */
    @Override
    public R setValue(R value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KeyedWindowResult that = (KeyedWindowResult) obj;
        return this.start() == that.start()
                && this.end() == that.end()
                && this.isEarly() == that.isEarly()
                && Objects.equals(this.result(), that.result())
                && Objects.equals(this.key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key);
    }

    @Override
    public String toString() {
        return String.format(
                "KeyedWindowResult{start=%s, end=%s, key='%s', value='%s', isEarly=%s}",
                toLocalTime(start()), toLocalTime(end()), key, result(), isEarly());
    }
}
