/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;

/**
 * An iterator for a sequence of primitive integers.
 */
@SuppressFBWarnings("EI2")
public class LongIterator implements Iterator<Long> {
    private final long missingValue;
    private final long[] values;

    private int position;

    /**
     * Construct an {@link Iterator} over an array of primitives longs.
     *
     * @param missingValue to indicate the value is missing, i.e. not present or null.
     * @param values       to iterate over.
     */
    public LongIterator(final long missingValue, final long[] values) {
        this.missingValue = missingValue;
        this.values = values;
    }

    public boolean hasNext() {
        final long[] values = this.values;
        while (position < values.length) {
            if (values[position] != missingValue) {
                return true;
            }

            position++;
        }

        return false;
    }

    public Long next() {
        return nextValue();
    }

    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * Strongly typed alternative of {@link Iterator#next()} not to avoid boxing.
     *
     * @return the next long value.
     */
    public long nextValue() {
        final long value = values[position];
        position++;
        return value;
    }

    void reset() {
        position = 0;
    }
}
