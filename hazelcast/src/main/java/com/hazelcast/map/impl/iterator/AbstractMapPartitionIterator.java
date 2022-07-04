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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * {@link AbstractMapPartitionIterator} provides the core iterator functionality
 * shared by its descendants.
 * Base class for iterating a partition. When iterating you can control:
 * <ul>
 * <li>the fetch size</li>
 * <li>whether values are prefetched or fetched when iterating</li>
 * </ul>
 * <p>
 * <p>The Hazelcast cluster is made out of partitions which holds a slice of
 * the cluster data. The partition count never increases or decreases in a
 * cluster. In order to implement an iterator over a partitioned data, we use
 * the following parameters.
 * <ul>
 * <li>the {@code partitionId}</li>
 * <li>Each partition may have a lot of entries, so we use a second parameter
 * to track the iteration of the partition.</li>
 * </ul>
 * </p>
 * <p>
 * Iteration steps:
 * <ul>
 * <li>fetching fixed number of keys from the partition defined by {@code partitionId}.</li>
 * <li>iteration on fetched keys.</li>
 * <li>get value of a key when the {@link #next()} method is called.</li>
 * <li>when fetched keys are exhausted by calling {@link #next()}, more keys
 * are fetched from the cluster.</li>
 * </ul>
 * This implementation iterates over partitions and for each partition it
 * iterates over the internal map using the {@link IterationPointer}s.
 * </p>
 * <p>
 * <h2>Fetching data from cluster:</h2>
 * Fetching is getting a fixed size of keys from the internal table of records
 * of a partition defined by {@code partitionId} and the
 * {@link IterationPointer}s. The fetch response is the keys and the
 * {@link IterationPointer}s used for the following fetch operation.
 * </p>
 * <p>
 * <h2>Notes:</h2>
 * <ul>
 * <li>Iterator fetches keys in batch with a fixed size that is configurable.</li>
 * <li>Fetched keys are cached in the iterator to be used in each iteration step.</li>
 * <li>{@link #hasNext()} may return {@code true} for a key already removed.</li>
 * <li>{@link #hasNext()} may only return {@code false} when all known keys
 * are fetched and iterated.</li>
 * <li>{@link #next()} may return {@code null} although the map does not
 * have a {@code null} value. This may happen, for example, when someone
 * removes the entry after the current thread has checked with
 * {@link #hasNext()}.</li>
 * <li>This implementation is not affected by value updates as each value is
 * retrieved from the cluster when {@link #next()} called.</li>
 * </ul>
 * </p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see RecordStore#fetchKeys(com.hazelcast.internal.iteration.IterationPointer[], int)
 * @see MapPartitionIterator
 * @see AbstractCursor
 */
public abstract class AbstractMapPartitionIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    protected IMap<K, V> map;
    protected final int fetchSize;
    protected final int partitionId;
    protected boolean prefetchValues;

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

    protected List result;

    public AbstractMapPartitionIterator(IMap<K, V> map, int fetchSize, int partitionId, boolean prefetchValues) {
        this.map = map;
        this.fetchSize = fetchSize;
        this.partitionId = partitionId;
        this.prefetchValues = prefetchValues;
        resetPointers();
    }

    @Override
    public boolean hasNext() {
        return (result != null && index < result.size()) || advance();
    }

    @Override
    public Map.Entry<K, V> next() {
        if (hasNext()) {
            currentIndex = index;
            index++;
            Data keyData = getKey(currentIndex);
            Object value = getValue(currentIndex, keyData);
            return new LazyMapEntry<>(keyData, value, (InternalSerializationService) getSerializationService());
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        if (result == null || currentIndex < 0) {
            throw new IllegalStateException("Iterator.next() must be called before remove()!");
        }
        Data keyData = getKey(currentIndex);
        map.remove(keyData);
        currentIndex = -1;
    }

    protected boolean advance() {
        if (pointers[pointers.length - 1].getIndex() < 0) {
            resetPointers();
            return false;
        }
        result = fetch();
        if (result != null && result.size() > 0) {
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
     * @param response the iteration response
     * @param pointers the pointers defining the state of iteration
     */
    protected void setIterationPointers(List response, IterationPointer[] pointers) {
        if (response != null && response.size() > 0) {
            this.pointers = pointers;
        }
    }

    protected abstract List fetch();

    protected abstract SerializationService getSerializationService();

    private Data getKey(int index) {
        if (result != null) {
            if (prefetchValues) {
                Map.Entry<Data, Data> entry = (Map.Entry<Data, Data>) result.get(index);
                return entry.getKey();
            } else {
                return (Data) result.get(index);
            }
        }
        return null;
    }

    private Object getValue(int index, Data keyData) {
        if (result != null) {
            if (prefetchValues) {
                Map.Entry<Data, Data> entry = (Map.Entry<Data, Data>) result.get(index);
                return entry.getValue();
            } else {
                return map.get(keyData);
            }
        }
        return null;
    }
}
