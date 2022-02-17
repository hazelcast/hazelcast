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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.flakeidgen.FlakeIdGenerator;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Set of IDs returned from {@link FlakeIdGenerator}.
 * <p>
 * IDs can be iterated using a foreach loop:
 * <pre>{@code
 *    IdBatch idBatch = myFlakeIdGenerator.newIdBatch(100);
 *    for (Long id : idBatch) {
 *        // ... use the id
 *    }
 * }</pre>
 * <p>
 * Object is immutable.
 */
public class IdBatch implements Iterable<Long> {
    private final long base;
    private final long increment;
    private final int batchSize;

    /**
     * Constructor
     *
     * @param base See {@link #base()}
     * @param increment See {@link #increment()}
     * @param batchSize See {@link #batchSize()}
     */
    public IdBatch(long base, long increment, int batchSize) {
        this.base = base;
        this.increment = increment;
        this.batchSize = batchSize;
    }

    /**
     * Returns the first ID in the set.
     */
    public long base() {
        return base;
    }

    /**
     * Returns increment from {@link #base()} for the next ID in the set.
     */
    public long increment() {
        return increment;
    }

    /**
     * Returns number of IDs in the set.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Returns iterator that will iterate contained IDs (boxing the as {@code Long}).
     * {@code remove()} is not supported.
     */
    @Nonnull
    public Iterator<Long> iterator() {
        return new Iterator<Long>() {
            private long base2 = base;
            private int remaining = batchSize;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public Long next() {
                if (remaining == 0) {
                    throw new NoSuchElementException();
                }
                remaining--;
                try {
                    return base2;
                } finally {
                    base2 += increment;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
