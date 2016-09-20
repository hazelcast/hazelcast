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

package com.hazelcast.client.map.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.map.impl.iterator.AbstractMapPartitionIterator;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.List;

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
        ClientMessage request = MapFetchKeysCodec.encodeRequest(mapProxy.getName(), partitionId,
                lastTableIndex, fetchSize);
        try {
            InternalCompletableFuture<ClientMessage> f = client.getInvocationService().invokeOnPartition(partitionId, request);
            MapFetchKeysCodec.ResponseParameters responseParameters = MapFetchKeysCodec.decodeResponse(f.get());
            setLastTableIndex(responseParameters.keys, responseParameters.tableIndex);
            return responseParameters.keys;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private List fetchWithPrefetchValues(HazelcastClientInstanceImpl client) {
        ClientMessage request = MapFetchEntriesCodec.encodeRequest(mapProxy.getName(), partitionId, lastTableIndex,
                fetchSize);
        try {
            InternalCompletableFuture<ClientMessage> f = client.getInvocationService().invokeOnPartition(partitionId, request);
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
