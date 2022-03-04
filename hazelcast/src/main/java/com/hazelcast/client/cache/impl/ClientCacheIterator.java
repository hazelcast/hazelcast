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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.AbstractCacheIterator;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheIterateCodec;
import com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.internal.iteration.IterationPointer.decodePointers;
import static com.hazelcast.internal.iteration.IterationPointer.encodePointers;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Client side cluster-wide iterator for {@link com.hazelcast.cache.ICache}.
 * <p>
 * This implementation is used by client implementation of JCache.
 * <p>
 * Note: For more information on the iterator details, see {@link AbstractCacheIterator}.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
public class ClientCacheIterator<K, V> extends AbstractCacheIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    private ICacheInternal<K, V> cacheProxy;
    private ClientContext context;

    public ClientCacheIterator(ICacheInternal<K, V> cacheProxy, ClientContext context, boolean prefetchValues) {
        this(cacheProxy, context, DEFAULT_FETCH_SIZE, prefetchValues);
    }

    public ClientCacheIterator(ICacheInternal<K, V> cacheProxy, ClientContext context,
                               int fetchSize, boolean prefetchValues) {
        super(cacheProxy, context.getPartitionService().getPartitionCount(), fetchSize, prefetchValues);
        this.cacheProxy = cacheProxy;
        this.context = context;
        advance();
    }

    public ClientCacheIterator(ICacheInternal<K, V> cacheProxy, ClientContext context, int fetchSize,
                               int partitionId, boolean prefetchValues) {
        super(cacheProxy, context.getPartitionService().getPartitionCount(), fetchSize, prefetchValues);
        this.cacheProxy = cacheProxy;
        this.context = context;
        this.partitionIndex = partitionId;
        advance();
    }

    protected List fetch() {
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        String name = cacheProxy.getPrefixedName();
        if (prefetchValues) {
            ClientMessage request = CacheIterateEntriesCodec.encodeRequest(
                    name, encodePointers(pointers), fetchSize);
            try {
                ClientInvocation clientInvocation = new ClientInvocation(client, request, name, partitionIndex);
                ClientInvocationFuture future = clientInvocation.invoke();
                CacheIterateEntriesCodec.ResponseParameters responseParameters = CacheIterateEntriesCodec.decodeResponse(
                        future.get());
                IterationPointer[] pointers = decodePointers(responseParameters.iterationPointers);
                setIterationPointers(responseParameters.entries, pointers);
                return responseParameters.entries;
            } catch (Exception e) {
                throw rethrow(e);
            }
        } else {
            ClientMessage request = CacheIterateCodec.encodeRequest(name, encodePointers(pointers), fetchSize);
            try {
                ClientInvocation clientInvocation = new ClientInvocation(client, request, name, partitionIndex);
                ClientInvocationFuture future = clientInvocation.invoke();
                CacheIterateCodec.ResponseParameters responseParameters = CacheIterateCodec.decodeResponse(future.get());
                IterationPointer[] pointers = decodePointers(responseParameters.iterationPointers);
                setIterationPointers(responseParameters.keys, pointers);
                return responseParameters.keys;
            } catch (Exception e) {
                throw rethrow(e);
            }
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
