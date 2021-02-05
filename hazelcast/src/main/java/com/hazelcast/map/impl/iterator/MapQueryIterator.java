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
 * Iterator for iterating the result of the projection on entries
 * in the whole cluster which satisfy the {@code predicate}. The values
 * are fetched in batches. The {@link Iterator#remove()} method is not
 * supported and will throw a {@link UnsupportedOperationException}.
 * It uses {@link MapQueryPartitionIterator} and the provided guarantees
 * are the same with it.
 *
 * @see MapQueryPartitionIterator
 */
public class MapQueryIterator<R> implements Iterator<R> {

    private final int size;
    private Iterator<R> it;
    private int idx;
    private final List<Iterator<R>> partitionIterators;

    public MapQueryIterator(List<Iterator<R>> partitionIterators) {
        this.partitionIterators = partitionIterators;
        this.size = partitionIterators.size();
        idx = 0;
        it = partitionIterators.get(idx);
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
            if (idx == size - 1) {
                return false;
            }
            it = partitionIterators.get(++idx);
        }
        return true;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing when iterating map with query is not supported");
    }
}
