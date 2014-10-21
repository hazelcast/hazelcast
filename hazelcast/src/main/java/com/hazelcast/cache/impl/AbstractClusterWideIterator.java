/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.ICache;
import com.hazelcast.nio.serialization.Data;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@link AbstractClusterWideIterator} provides the core iterator functionality shared by its descendants.
 *
 *<p>Hazelcast cluster is made of partitions which holds a slice of all clusters data. Partition count
 * never increase or decrease in a cluster. In order to implement an iterator over a partitioned data, we use
 * the following parameters.
 *<ul>
 *<li>To iterate over partitioned data, we use partitionId as the first parameter of this iterator.</li>
 *<li>Each partition may have a lot of entries, so we use a second parameter to track the iteration of the
 * partition.</li>
 *</ul>
 *</p>
 * <p>
 *Iteration steps:
 * <ul>
 *     <li>fetching fixed sized of keys from the current partition defined by partitionId.</li>
 *     <li>iteration on fetched keys.</li>
 *     <li>get value of each key with {@link #next()} when method is called.</li>
 *     <li>when fetched keys are all used by calling {@link #next()}, more keys are fetched from the cluster.</li>
 * </ul>
 * This implementation iterates over partitions and for each partition it iterates over the internal map using the
 *     internal table index of the map {@link com.hazelcast.util.CacheConcurrentHashMap}.
 * </p>
 * <p>
 * <h2>Fetching data from cluster:</h2>
 * Fetching is getting a fixed size of keys from the internal table of records of a partition defined by
 * partitionId. Table index is also provided as a table index locator. Fetch response is the keys and
 * last table index. The last table index is included in the result to be used in the next fetch.
 * </p>
 * <p>
 * <h2>Notes:</h2>
 * <ul>
 * <li>Iterator fetches keys in batch with a fixed size that is configurable.</li>
 * <li>Fetched keys are cached in the iterator to be used in each iteration step.</li>
 * <li>{@link #hasNext()} may return true for a key already removed.</li>
 * <li>{@link #hasNext()} only return false when all known keys are fetched and iterated.</li>
 * <li>{@link #next()} may return null although cache never has null value. This may happen when, for example,
 * someone removes the entry after the current thread has checked with {@link #hasNext()}.</li>
 * <li>This implementation does not affected by value updates as each value is got from the cluster
 * when {@link #next()} called.</li>
 * </ul>
 * </p>
 *
 * @param <K> the type of key
 * @param <V> the type of value
 * @see com.hazelcast.cache.impl.CacheRecordStore#iterator(int tableIndex, int size)
 * @see com.hazelcast.cache.impl.ClusterWideIterator
 * @see com.hazelcast.cache.impl.CacheKeyIteratorResult
 */
public abstract class AbstractClusterWideIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    private static final int FETCH_SIZE = 100;

    protected ICache<K, V> cache;

    protected CacheKeyIteratorResult result;
    protected final int partitionCount;
    protected int partitionIndex = -1;

    protected int lastTableIndex;

    protected final int fetchSize;

    protected int index;
    protected int currentIndex = -1;

    public AbstractClusterWideIterator(ICache<K, V> cache, int partitionCount) {
        this.cache = cache;
        this.partitionCount = partitionCount;

        //TODO can be made configurable
        this.fetchSize = FETCH_SIZE;
    }

    @Override
    public boolean hasNext() {
        ensureOpen();
        if (result != null && index < result.getCount()) {
            return true;
        }
        return advance();
    }

    @Override
    public Cache.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        currentIndex = index;
        index++;
        final Data keyData = result.getKey(currentIndex);
        final K key = toObject(keyData);
        final V value = cache.get(key);
        return new CacheEntry<K, V>(key, value);
    }

    @Override
    public void remove() {
        ensureOpen();
        if (result == null || currentIndex < 0) {
            throw new IllegalStateException("Iterator.next() must be called before remove()!");
        }
        Data keyData = result.getKey(currentIndex);
        final K key = toObject(keyData);
        cache.remove(key);
        currentIndex = -1;
    }

    protected boolean advance() {
        while (partitionIndex < getPartitionCount()) {
            if (result == null || result.getCount() < fetchSize || lastTableIndex < 0) {
                partitionIndex++;
                lastTableIndex = Integer.MAX_VALUE;
                result = null;
                if (partitionIndex == getPartitionCount()) {
                    return false;
                }
            }
            result = fetch();
            if (result != null && result.getCount() > 0) {
                index = 0;
                lastTableIndex = result.getTableIndex();
                return true;
            }
        }
        return false;
    }

    protected void ensureOpen() {
        if (cache.isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    protected int getPartitionCount() {
        return partitionCount;
    }

    protected abstract CacheKeyIteratorResult fetch();

    protected abstract Data toData(Object obj);

    protected abstract <T> T toObject(Object data);
}
