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

import com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import javax.cache.Cache;
import java.util.Iterator;

/**
 * Cluster-wide iterator for the {@link com.hazelcast.cache.ICache}
 *
 * This cluster key iterator, uses to indexes to iterate the cluster which ar PartitionId and tableIndex.
 *
 * Starting from the largest PartitionId it accesses related partition with a tableIndex. Each partition has
 * a concurrentMap with single segment. So each partition data is basically a concurrent hashMap to iterate on.
 *
 * With a configurable batch size of keys are fetch from partition data, i.e CacheRecordStore.
 *
 * Although keys fetch in batches the {@link #next()} returns the entry one by one due to uncertainty of data.
 *
 * @param <K> key
 * @param <V> value
 */
public class ClusterWideIterator<K, V>
        extends AbstractClusterWideIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    private final SerializationService serializationService;
    private final CacheProxy<K, V> cacheProxy;

    public ClusterWideIterator(CacheProxy<K, V> cache) {
        super(cache, cache.getNodeEngine().getPartitionService().getPartitionCount());
        this.cacheProxy = cache;
        this.serializationService = cache.getNodeEngine().getSerializationService();
        advance();
    }

    protected CacheKeyIteratorResult fetch() {
        final Operation op = new CacheKeyIteratorOperation(cacheProxy.nameWithPrefix, lastTableIndex, fetchSize);
        final OperationService operationService = cacheProxy.getNodeEngine().getOperationService();
        final InternalCompletableFuture<CacheKeyIteratorResult> f = operationService
                .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionIndex);
        return f.getSafely();
    }

    @Override
    protected Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    @Override
    protected <T> T toObject(Object data) {
        return serializationService.toObject(data);
    }

}
