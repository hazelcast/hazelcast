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
import com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import javax.cache.Cache;
import java.util.Iterator;

/**
 * Cluster-wide iterator for the {@link com.hazelcast.cache.ICache}
 *
 * @param <K> key
 * @param <V> value
 */
public class ClusterWideIterator<K, V>
        extends AbstractClusterWideIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    final CacheDistributedObject cacheDistributedObject;

    public ClusterWideIterator(ICache<K, V> cache, CacheDistributedObject cacheDistributedObject) {
        super(cache, cacheDistributedObject.getNodeEngine().getPartitionService().getPartitionCount());
        this.cacheDistributedObject = cacheDistributedObject;
        advance();
    }

    protected CacheKeyIteratorResult fetch() {
        final Operation op = new CacheKeyIteratorOperation(getDistributedObject().getName(), lastTableIndex, fetchSize);
        final OperationService operationService = getDistributedObject().getNodeEngine().getOperationService();
        final InternalCompletableFuture<CacheKeyIteratorResult> f = operationService
                .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionIndex);
        return f.getSafely();
    }

    @Override
    protected Data toData(Object obj) {
        return getDistributedObject().getNodeEngine().getSerializationService().toData(obj);
    }

    @Override
    protected <T> T toObject(Object data) {
        return getDistributedObject().getNodeEngine().getSerializationService().toObject(data);
    }

    private CacheDistributedObject getDistributedObject() {
        return cacheDistributedObject;
    }
}
