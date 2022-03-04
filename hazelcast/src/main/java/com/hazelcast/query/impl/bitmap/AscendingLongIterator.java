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

package com.hazelcast.query.impl.bitmap;

/**
 * Iterates over a set of non-negative {@code long} values in ascending order.
 */
public interface AscendingLongIterator {

    /**
     * Identifies an iterator end.
     * <p>
     * Don't change this value, iteration logic relies on it being exactly -1L.
     */
    long END = -1L;

    /**
     * Denotes an empty ordered long iterator.
     */
    AscendingLongIterator EMPTY = new AscendingLongIterator() {
        @Override
        public long getIndex() {
            return END;
        }

        @Override
        public long advance() {
            return END;
        }

        @Override
        public long advanceAtLeastTo(long member) {
            return END;
        }
    };

    /**
     * Returns a value at which this iterator is positioned currently or {@link
     * #END} if this iterator has reached its end.
     * <p>
     * Just after the creation, iterators are positioned at their first value.
     * <p>
     * "Index" is used instead of "value" since {@link SparseArray.Iterator}
     * interface extends this interface with {@link SparseArray.Iterator#getValue()}
     * method.
     */
    long getIndex();

    /**
     * Advances this iterator to the next index.
     * <p>
     *
     * @return an index at which this iterator was positioned before the
     * advancement or {@link #END} if this iterator already was at its end.
     * Once the end is reached, return value is always {@link #END} on any
     * subsequent advancement attempts.
     */
    long advance();

    /**
     * Advances this iterator to the given member; or, if the member is not
     * present in this iterator, to an index immediately following it and
     * present in the iterator or {@link #END} if no such index exists.
     *
     * @param member the member to advance at least to.
     * @return an index at which this iterator was advanced to or {@link #END}
     * if this iterator reached its end. Once the end is reached, return value
     * is always {@link #END} on any subsequent advancement attempts.
     */
    long advanceAtLeastTo(long member);

}
