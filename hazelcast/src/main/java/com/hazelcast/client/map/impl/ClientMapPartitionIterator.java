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

package com.hazelcast.client.map.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.iterator.AbstractMapPartitionIterator;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.List;

/**
 * Iterator for iterating map entries in the {@code partitionId}. The values are not fetched one-by-one but rather in batches.
 * <b>NOTE</b>
 * Iterating the map should be done only when the {@link IMap} is not being
 * mutated and the cluster is stable (there are no migrations or membership changes).
 * In other cases, the iterator may not return some entries or may return an entry twice.
 */
public class ClientMapPartitionIterator<K, V> extends AbstractMapPartitionIterator<K, V> {

    private final ClientMapProxy<K, V> mapProxy;
    private final ClientContext context;

    public ClientMapPartitionIterator(ClientMapProxy<K, V> mapProxy, ClientContext context, int fetchSize,
                                      int partitionId, boolean prefetchValues) {
        super(mapProxy, fetchSize, partitionId, prefetchValues);
        this.mapProxy = mapProxy;
        this.context = context;
    }

    @Override
    protected List fetch() {
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        if (prefetchValues) {
            return fetchWithPrefetchValues(client);
        } else {
            return fetchWithoutPrefetchValues(client);
        }
    }

    private List fetchWithoutPrefetchValues(HazelcastClientInstanceImpl client) {
        ClientMessage request = MapFetchKeysCodec.encodeRequest(mapProxy.getName(), lastTableIndex, fetchSize);
        ClientInvocation clientInvocation = new ClientInvocation(client, request, mapProxy.getName(), partitionId);
        try {
            ClientInvocationFuture f = clientInvocation.invoke();
            MapFetchKeysCodec.ResponseParameters responseParameters = MapFetchKeysCodec.decodeResponse(f.get());
            setLastTableIndex(responseParameters.keys, responseParameters.tableIndex);
            return responseParameters.keys;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private List fetchWithPrefetchValues(HazelcastClientInstanceImpl client) {
        ClientMessage request = MapFetchEntriesCodec.encodeRequest(mapProxy.getName(), lastTableIndex, fetchSize);
        ClientInvocation clientInvocation = new ClientInvocation(client, request, mapProxy.getName(), partitionId);
        try {
            ClientInvocationFuture f = clientInvocation.invoke();
            MapFetchEntriesCodec.ResponseParameters responseParameters = MapFetchEntriesCodec.decodeResponse(f.get());
            setLastTableIndex(responseParameters.entries, responseParameters.tableIndex);
            return responseParameters.entries;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected SerializationService getSerializationService() {
        return context.getSerializationService();
    }
}
