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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator for iterating cache entries over multiple partitions.
 * The values are fetched in batches. This implementation is used for embedded mode.
 * <b>NOTE</b>
 * The iteration may be done when the map is being mutated or when there are
 * membership changes. The iterator does not reflect the state when it has
 * been constructed - it may return some entries that were added after the
 * iteration has started and may not return some entries that were removed
 * after iteration has started.
 * The iterator will not, however, skip an entry if it has not been changed
 * and will not return an entry twice.
 * For more information on the iterator details, see
 * {@link AbstractCacheIterator}.
 */
public class CacheIterator<K, V>
        extends AbstractCacheIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    private final SerializationService serializationService;
    private final CacheProxy<K, V> cacheProxy;

    public CacheIterator(CacheProxy<K, V> cache, boolean prefetchValues) {
        this(cache, DEFAULT_FETCH_SIZE, prefetchValues);
    }

    public CacheIterator(CacheProxy<K, V> cache, int fetchSize, boolean prefetchValues) {
        super(cache, cache.getNodeEngine().getPartitionService().getPartitionCount(), fetchSize, prefetchValues);
        this.cacheProxy = cache;
        this.serializationService = cache.getNodeEngine().getSerializationService();
        advance();
    }

    public CacheIterator(CacheProxy<K, V> cache, int fetchSize, int partitionId, boolean prefetchValues) {
        super(cache, cache.getNodeEngine().getPartitionService().getPartitionCount(), fetchSize, prefetchValues);
        this.cacheProxy = cache;
        this.serializationService = cache.getNodeEngine().getSerializationService();
        this.partitionIndex = partitionId;
        advance();
    }

    protected List fetch() {
        final OperationService operationService = cacheProxy.getNodeEngine().getOperationService();
        if (prefetchValues) {
            Operation operation = cacheProxy.operationProvider.createFetchEntriesOperation(pointers, fetchSize);
            CacheEntriesWithCursor iteratorResult = invoke(operationService, operation);
            setIterationPointers(iteratorResult.getEntries(), iteratorResult.getPointers());
            return iteratorResult.getEntries();
        } else {
            Operation operation = cacheProxy.operationProvider.createFetchKeysOperation(pointers, fetchSize);
            CacheKeysWithCursor iteratorResult = invoke(operationService, operation);
            setIterationPointers(iteratorResult.getKeys(), iteratorResult.getPointers());
            return iteratorResult.getKeys();
        }

    }

    private <T> T invoke(OperationService operationService, Operation operation) {
        InternalCompletableFuture<T> f = operationService
                .invokeOnPartition(CacheService.SERVICE_NAME, operation, partitionIndex);
        return f.joinInternal();
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
