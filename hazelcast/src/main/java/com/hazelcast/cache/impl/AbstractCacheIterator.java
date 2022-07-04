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

package com.hazelcast.cache.impl;

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * {@link AbstractCacheIterator} provides the core iterator functionality
 * shared by its descendants.
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
 * <li>{@link #next()} may return {@code null} although the cache does not
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
 * @see com.hazelcast.cache.impl.CacheRecordStore#fetchKeys(IterationPointer[], int)
 * @see CacheIterator
 * @see CacheKeysWithCursor
 */
public abstract class AbstractCacheIterator<K, V> implements Iterator<Cache.Entry<K, V>> {

    protected static final int DEFAULT_FETCH_SIZE = 100;

    protected ICacheInternal<K, V> cache;

    protected List result;
    protected final int partitionCount;
    protected int partitionIndex = -1;

    /**
     * The iteration pointers define the iteration state over a backing map.
     * Each array item represents an iteration state for a certain size of the
     * backing map structure (either allocated slot count for HD or table size
     * for on-heap). Each time the table is resized, this array will carry an
     * additional iteration pointer.
     */
    protected IterationPointer[] pointers;

    protected final int fetchSize;
    protected boolean prefetchValues;

    protected int index;
    protected int currentIndex = -1;

    public AbstractCacheIterator(ICacheInternal<K, V> cache,
                                 int partitionCount,
                                 int fetchSize,
                                 boolean prefetchValues) {
        this.cache = cache;
        this.partitionCount = partitionCount;
        this.fetchSize = fetchSize;
        this.prefetchValues = prefetchValues;
        resetPointers();
    }

    @Override
    public boolean hasNext() {
        ensureOpen();
        if (result != null && index < result.size()) {
            return true;
        }
        return advance();
    }

    @Override
    public Cache.Entry<K, V> next() {
        if (hasNext()) {
            currentIndex = index;
            index++;
            Data keyData = getKey(currentIndex);
            K key = toObject(keyData);
            V value = getValue(currentIndex, key);
            return new CacheEntry<>(key, value);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        ensureOpen();
        if (result == null || currentIndex < 0) {
            throw new IllegalStateException("Iterator.next() must be called before remove()!");
        }
        Data keyData = getKey(currentIndex);
        final K key = toObject(keyData);
        cache.remove(key);
        currentIndex = -1;
    }

    protected boolean advance() {
        while (partitionIndex < getPartitionCount()) {
            if (result == null || result.size() < fetchSize
                    || pointers[pointers.length - 1].getIndex() < 0) {
                partitionIndex++;
                resetPointers();
                result = null;
                if (partitionIndex == getPartitionCount()) {
                    return false;
                }
            }
            result = fetch();
            if (result != null && result.size() > 0) {
                index = 0;
                return true;
            }
        }
        return false;
    }


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

    private V getValue(int index, K key) {
        if (result != null) {
            if (prefetchValues) {
                Map.Entry<Data, Data> entry = (Map.Entry<Data, Data>) result.get(index);
                return (V) toObject(entry.getValue());
            } else {
                return cache.get(key);
            }
        }
        return null;
    }


    protected void ensureOpen() {
        if (cache.isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    /**
     * Resets the iteration state.
     */
    protected void resetPointers() {
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

    protected int getPartitionCount() {
        return partitionCount;
    }

    protected abstract List fetch();

    protected abstract Data toData(Object obj);

    protected abstract <T> T toObject(Object data);
}
