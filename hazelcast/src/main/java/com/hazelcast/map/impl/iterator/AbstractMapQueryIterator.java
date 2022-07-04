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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Base class for iterating map entries in the whole cluster with
 * a {@link com.hazelcast.query.Predicate} and a {@link com.hazelcast.projection.Projection}.
 *
 *
 * @param <R> return type of the iterator
 * @see AbstractMapQueryPartitionIterator
 */
public class AbstractMapQueryIterator<R> implements Iterator<R> {

    private final ConstructorFunction<Integer, Iterator<R>> createPartitionIterator;
    private final int partitionCount;
    private Iterator<R> it;
    private int idx;

    public AbstractMapQueryIterator(ConstructorFunction<Integer, Iterator<R>> createPartitionIterator, int partitionCount) {
        this.createPartitionIterator = createPartitionIterator;
        this.partitionCount = partitionCount;
        idx = 0;
        it = createPartitionIterator.createNew(idx);
    }

    @Override
    public R next() {
        if (hasNext()) {
            return it.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
        while (!it.hasNext()) {
            if (idx == partitionCount - 1) {
                return false;
            }
            it = createPartitionIterator.createNew(++idx);
        }
        return true;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing when iterating map with query is not supported");
    }
}
