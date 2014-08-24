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

package com.hazelcast.cache;

import com.hazelcast.cache.operation.CacheKeyIteratorOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ClusterWideIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    private final int partitionCount;
    private int partitionIndex = -1;
    private int lastTableIndex;

    final private int fetchSize;

    private CacheProxy<K, V> cacheProxy;

    private int index;
    private int currentIndex = -1;

    CacheKeyIteratorResult result;

    final SerializationService serializationService;

    public ClusterWideIterator(CacheProxy<K, V> cacheProxy) {
        this.cacheProxy = cacheProxy;
        final NodeEngine engine = cacheProxy.getNodeEngine();
        this.serializationService = engine.getSerializationService();
        this.partitionCount = engine.getPartitionService().getPartitionCount();

        //TODO can be made configurable
        this.fetchSize = 100;
        advance();
    }

    @Override
    public boolean hasNext() {
        cacheProxy.ensureOpen();
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
        final K key = serializationService.toObject(keyData);
        final V value = cacheProxy.get(key);
        return new CacheEntry<K, V>(key, value);
    }

    @Override
    public void remove() {
        cacheProxy.ensureOpen();
        if (result == null || currentIndex < 0) {
            throw new IllegalStateException("Iterator.next() must be called before remove()!");
        }
        Data keyData = result.getKey(currentIndex);
        final K key = serializationService.toObject(keyData);
        cacheProxy.remove(key);
        currentIndex = -1;
    }

    private boolean advance() {
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

    protected int getPartitionCount() {
        return partitionCount;
    }

    protected CacheKeyIteratorResult fetch() {
        final NodeEngine nodeEngine = cacheProxy.getNodeEngine();
        final Operation op = new CacheKeyIteratorOperation(cacheProxy.getDistributedObjectName(), lastTableIndex, fetchSize);
        final InternalCompletableFuture<CacheKeyIteratorResult> f = nodeEngine.getOperationService()
                                                                              .invokeOnPartition(CacheService.SERVICE_NAME, op,
                                                                                      partitionIndex);
        return f.getSafely();
    }
}
