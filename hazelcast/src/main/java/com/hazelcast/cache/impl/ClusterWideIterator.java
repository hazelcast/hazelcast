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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import javax.cache.Cache;
import java.util.Iterator;

/**
 * Cluster-wide iterator for {@link com.hazelcast.cache.ICache}.
 * <p/>
 * <p>
 * This implementation is used for server or embedded mode.
 * </p>
 * Note: For more information on the iterator details, see {@link AbstractClusterWideIterator}.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see AbstractClusterWideIterator
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
        Operation operation = cacheProxy.operationProvider.createKeyIteratorOperation(lastTableIndex, fetchSize);
        final OperationService operationService = cacheProxy.getNodeEngine().getOperationService();
        final InternalCompletableFuture<CacheKeyIteratorResult> f = operationService
                .invokeOnPartition(CacheService.SERVICE_NAME, operation, partitionIndex);
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
