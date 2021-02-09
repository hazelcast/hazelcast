/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Iterator;
import java.util.List;
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
    protected static final int DEFAULT_FETCH_SIZE = 100;
    protected int size;
    protected Iterator<R> it;
    protected int idx;
    protected List<Iterator<R>> queryPartitionIterators;

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
            if (idx == size - 1) {
                return false;
            }
            it = queryPartitionIterators.get(++idx);
        }
        return true;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing when iterating map with query is not supported");
    }
}
