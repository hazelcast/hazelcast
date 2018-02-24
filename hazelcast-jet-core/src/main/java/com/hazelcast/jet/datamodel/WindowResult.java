/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Holds the result of a group-and-aggregate operation performed in a time
 * window.
 *
 * @param <K> type of key
 * @param <R> type of aggregated result
 */
public class WindowResult<K, R> implements Map.Entry<K, R> {
    private final long start;
    private final long end;
    private final K key;
    private final R result;

    /**
     * @param start {@link #getStart()}
     * @param end {@link #getEnd()}
     * @param key {@link #getKey()}
     * @param result {@link #getValue()}
     */
    public WindowResult(long start, long end, @Nonnull K key, @Nonnull R result) {
        this.start = start;
        this.end = end;
        this.key = key;
        this.result = result;
    }

    /**
     * Returns the starting timestamp of the window.
     */
    public long getStart() {
        return start;
    }

    /**
     * Returns the ending timestamp of the window.
     */
    public long getEnd() {
        return end;
    }

    /**
     * Returns the key.
     */
    @Nonnull @Override
    public K getKey() {
        return key;
    }

    /**
     * Returns the aggregated result.
     */
    @Nonnull @Override
    public R getValue() {
        return result;
    }

    @Override
    public R setValue(R value) {
        throw new UnsupportedOperationException("setValue called on the immutable WindowResult");
    }

    @Override
    public boolean equals(Object obj) {
        WindowResult that;
        return this == obj
                || obj instanceof WindowResult
                    && this.start == (that = (WindowResult) obj).start
                    && this.end == that.end
                    && this.key.equals(that.key)
                    && this.result.equals(that.result);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + Long.hashCode(start);
        hc = 73 * hc + Long.hashCode(end);
        hc = 73 * hc + key.hashCode();
        hc = 73 * hc + result.hashCode();
        return hc;
    }

    @Override
    public String toString() {
        return "[" + start + ".." + end + "]: " + key + " = " + result;
    }
}
