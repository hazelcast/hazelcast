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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.AbstractClusterWideIterator;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.client.CacheIterateRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.Cache;
import java.util.Iterator;

/**
 * client side cluster-wide iterator for the {@link com.hazelcast.cache.ICache}
 *
 * @param <K> key
 * @param <V> value
 */
public class ClientClusterWideIterator<K, V>
        extends AbstractClusterWideIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    private ClientCacheProxy<K, V> cacheProxy;
    private ClientContext context;

    public ClientClusterWideIterator(ClientCacheProxy<K, V> cacheProxy, ClientContext context) {
        super(cacheProxy, context.getPartitionService().getPartitionCount());
        this.cacheProxy = cacheProxy;
        this.context = context;
        advance();
    }

    protected CacheKeyIteratorResult fetch() {
        CacheIterateRequest request = new CacheIterateRequest(cacheProxy.getNameWithPrefix(), partitionIndex, lastTableIndex,
                fetchSize);
        try {
            final ICompletableFuture<Object> f = context.getInvocationService().invokeOnRandomTarget(request);
            return toObject(f.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected Data toData(Object obj) {
        return context.getSerializationService().toData(obj);
    }

    @Override
    protected <T> T toObject(Object data) {
        return context.getSerializationService().toObject(data);
    }

}
