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
 * Base cluster-wide iterator
 *
 * @param <K>
 * @param <V>
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
        //        advance();
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
