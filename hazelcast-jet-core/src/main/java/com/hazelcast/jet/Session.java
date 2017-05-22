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

package com.hazelcast.jet;

import javax.annotation.Nonnull;

/**
 * Holds the aggregated result of a session window.
 *
 * @param <K> type of key
 * @param <R> type of aggregated result
 */
public class Session<K, R> {
    private final K key;
    private final long start;
    private final long end;
    private final R result;

    /**
     * @param key {@link #getKey()}
     * @param start {@link #getStart()}
     * @param end {@link #getEnd()}
     * @param result {@link #getResult()}
     */
    public Session(@Nonnull K key, long start, long end, @Nonnull R result) {
        this.key = key;
        this.start = start;
        this.end = end;
        this.result = result;
    }

    /**
     * Returns the session's key.
     */
    @Nonnull
    public K getKey() {
        return key;
    }

    /**
     * Returns the aggregated result for the session.
     */
    @Nonnull
    public R getResult() {
        return result;
    }

    /**
     * Returns the starting timestamp of the session.
     */
    public long getStart() {
        return start;
    }

    /**
     * Returns the ending timestamp of the session.
     */
    public long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object obj) {
        Session that;
        return this == obj
                || obj instanceof Session
                    && this.start == (that = (Session) obj).start
                    && this.end == that.end
                    && this.key.equals(that.key)
                    && this.result.equals(that.result);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + key.hashCode();
        hc = 73 * hc + result.hashCode();
        hc = 73 * hc + Long.hashCode(start);
        hc = 73 * hc + Long.hashCode(end);
        return hc;
    }

    @Override
    public String toString() {
        return key + "[" + start + ".." + end + "]=" + result;
    }
}
