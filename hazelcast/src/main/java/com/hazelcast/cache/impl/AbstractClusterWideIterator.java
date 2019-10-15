/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.Data;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * {@link AbstractClusterWideIterator} provides the core iterator functionality shared by its descendants.
 * <p>
 * <p>Hazelcast cluster is made of partitions which holds a slice of all clusters data. Partition count
 * never increase or decrease in a cluster. In order to implement an iterator over a partitioned data, we use
 * the following parameters.
 * <ul>
 * <li>To iterate over partitioned data, we use partitionId as the first parameter of this iterator.</li>
 * <li>Each partition may have a lot of entries, so we use a second parameter to track the iteration of the
 * partition.</li>
 * </ul>
 * <p>
 * Iteration steps:
 * <ul>
 * <li>fetching fixed sized of keys from the current partition defined by partitionId.</li>
 * <li>iteration on fetched keys.</li>
 * <li>get value of each key with {@link #next()} when method is called.</li>
 * <li>when fetched keys are all used by calling {@link #next()}, more keys are fetched from the cluster.</li>
 * </ul>
 * This implementation iterates over partitions and for each partition it iterates over the internal map using the
 * internal table index of the map {@link com.hazelcast.internal.util.SampleableConcurrentHashMap}.
 * <p>
 * <h2>Fetching data from cluster:</h2>
 * Fetching is getting a fixed size of keys from the internal table of records of a partition defined by
 * partitionId. Table index is also provided as a table index locator. Fetch response is the keys and
 * last table index. The last table index is included in the result to be used in the next fetch.
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
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see com.hazelcast.cache.impl.CacheRecordStore#fetchKeys(int tableIndex, int size)
 * @see com.hazelcast.cache.impl.ClusterWideIterator
 * @see CacheKeyIterationResult
 */
public abstract class AbstractClusterWideIterator<K, V> implements Iterator<Cache.Entry<K, V>> {

    protected static final int DEFAULT_FETCH_SIZE = 100;

    protected ICacheInternal<K, V> cache;

    protected List result;
    protected final int partitionCount;
    protected int partitionIndex = -1;

    /**
     * The table is segment table of hash map, which is an array that stores the actual records.
     * This field is used to mark where the latest entry is read.
     * <p>
     * The iteration will start from highest index available to the table.
     * It will be converted to array size on the server side.
     */
    protected int lastTableIndex = Integer.MAX_VALUE;

    protected final int fetchSize;
    protected boolean prefetchValues;

    protected int index;
    protected int currentIndex = -1;

    public AbstractClusterWideIterator(ICacheInternal<K, V> cache, int partitionCount, int fetchSize, boolean prefetchValues) {
        this.cache = cache;
        this.partitionCount = partitionCount;
        this.fetchSize = fetchSize;
        this.prefetchValues = prefetchValues;
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
        while (hasNext()) {
            currentIndex = index;
            index++;
            final Data keyData = getKey(currentIndex);
            final K key = toObject(keyData);
            final V value = getValue(currentIndex, key);
            // Value might be removed or evicted
            if (value != null) {
                return new CacheEntry<K, V>(key, value);
            }
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
            if (result == null || result.size() < fetchSize || lastTableIndex < 0) {
                partitionIndex++;
                lastTableIndex = Integer.MAX_VALUE;
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

    protected void setLastTableIndex(List response, int lastTableIndex) {
        if (response != null && response.size() > 0) {
            this.lastTableIndex = lastTableIndex;
        }
    }

    protected int getPartitionCount() {
        return partitionCount;
    }

    protected abstract List fetch();

    protected abstract Data toData(Object obj);

    protected abstract <T> T toObject(Object data);
}
