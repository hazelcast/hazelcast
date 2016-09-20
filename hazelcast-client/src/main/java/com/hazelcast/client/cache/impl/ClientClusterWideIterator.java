/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheIterateCodec;
import com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;

/**
 * Client side cluster-wide iterator for {@link com.hazelcast.cache.ICache}.
 * <p>
 * This implementation is used by client implementation of jcache.
 * </p>
 * Note: For more information on the iterator details, see {@link AbstractClusterWideIterator}.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
public class ClientClusterWideIterator<K, V>
        extends AbstractClusterWideIterator<K, V>
        implements Iterator<Cache.Entry<K, V>> {

    private ClientCacheProxy<K, V> cacheProxy;
    private ClientContext context;

    public ClientClusterWideIterator(ClientCacheProxy<K, V> cacheProxy, ClientContext context, boolean prefetchValues) {
        this(cacheProxy, context, DEFAULT_FETCH_SIZE, prefetchValues);
    }

    public ClientClusterWideIterator(ClientCacheProxy<K, V> cacheProxy, ClientContext context,
                                     int fetchSize, boolean prefetchValues) {
        super(cacheProxy, context.getPartitionService().getPartitionCount(), fetchSize, prefetchValues);
        this.cacheProxy = cacheProxy;
        this.context = context;
        advance();
    }

    public ClientClusterWideIterator(ClientCacheProxy<K, V> cacheProxy, ClientContext context, int fetchSize,
                                     int partitionId, boolean prefetchValues) {
        super(cacheProxy, context.getPartitionService().getPartitionCount(), fetchSize, prefetchValues);
        this.cacheProxy = cacheProxy;
        this.context = context;
        this.partitionIndex = partitionId;
        advance();
    }

    protected List fetch() {
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        if (prefetchValues) {
            ClientMessage request = CacheIterateEntriesCodec
                    .encodeRequest(cacheProxy.getPrefixedName(), partitionIndex, lastTableIndex, fetchSize);
            try {
                ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionIndex);
                ClientInvocationFuture<ClientMessage> f = clientInvocation.invoke();
                CacheIterateEntriesCodec.ResponseParameters responseParameters = CacheIterateEntriesCodec.
                        decodeResponse(f.get());
                setLastTableIndex(responseParameters.entries, responseParameters.tableIndex);
                return responseParameters.entries;
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        } else {
            ClientMessage request = CacheIterateCodec
                    .encodeRequest(cacheProxy.getPrefixedName(), partitionIndex, lastTableIndex, fetchSize);
            try {
                ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionIndex);
                ClientInvocationFuture<ClientMessage> f = clientInvocation.invoke();
                CacheIterateCodec.ResponseParameters responseParameters = CacheIterateCodec.decodeResponse(f.get());
                setLastTableIndex(responseParameters.keys, responseParameters.tableIndex);
                return responseParameters.keys;
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
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
