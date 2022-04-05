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

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;

/**
 * Base class for iterating a partition with a {@link Predicate} and a {@link Projection}.
 * When iterating you can control:
 * <ul>
 * <li>the fetch size</li>
 * <li>whether a projection was applied to the entries</li>
 * <li>whether a predicate was applied to the entries</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @param <R> in the case {@link #query} is null, this is {@code Map.Entry<K,V>}, otherwise it is the return type of
 *            the projection
 */
public abstract class AbstractMapQueryPartitionIterator<K, V, R> implements Iterator<R> {
    protected final IMap<K, V> map;
    protected final int fetchSize;
    protected final int partitionId;
    protected final Query query;

    /**
     * The iteration pointers define the iteration state over a backing map.
     * Each array item represents an iteration state for a certain size of the
     * backing map structure (either allocated slot count for HD or table size
     * for on-heap). Each time the table is resized, this array will carry an
     * additional iteration pointer.
     */
    protected IterationPointer[] pointers;

    protected int index;
    protected int currentIndex = -1;

    protected List<Data> segment;

    public AbstractMapQueryPartitionIterator(IMap<K, V> map,
                                             int fetchSize,
                                             int partitionId,
                                             Predicate<K, V> predicate,
                                             Projection<? super Entry<K, V>, R> projection) {
        this.map = map;
        this.fetchSize = fetchSize;
        this.partitionId = partitionId;
        this.query = Query.of()
                          .mapName(map.getName())
                          .iterationType(IterationType.VALUE)
                          .predicate(predicate)
                          .projection(projection)
                          .build();
        resetPointers();
    }

    @Override
    public boolean hasNext() {
        return (segment != null && index < segment.size()) || advance();
    }

    @Override
    public R next() {
        if (hasNext()) {
            currentIndex = index;
            index++;
            return getSerializationService().toObject(getQueryResult(currentIndex));
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing when iterating map with query is not supported");
    }

    protected boolean advance() {
        if (pointers[pointers.length - 1].getIndex() < 0) {
            resetPointers();
            return false;
        }
        segment = fetch();
        if (isNotEmpty(segment)) {
            index = 0;
            return true;
        }
        return false;
    }

    /**
     * Resets the iteration state.
     */
    private void resetPointers() {
        pointers = new IterationPointer[]{new IterationPointer(Integer.MAX_VALUE, -1)};
    }

    /**
     * Sets the iteration state to the state defined by the {@code pointers}
     * if the given response contains items.
     *
     * @param segment  the iteration response
     * @param pointers the pointers defining the state of iteration
     */
    protected void setLastTableIndex(List<Data> segment, IterationPointer[] pointers) {
        if (isNotEmpty(segment)) {
            this.pointers = pointers;
        }
    }

    protected abstract List<Data> fetch();

    protected abstract SerializationService getSerializationService();

    private Data getQueryResult(int index) {
        if (segment != null) {
            return segment.get(index);
        }
        return null;
    }
}
