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
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.iterator.AbstractMapQueryPartitionIterator;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * Iterator for iterating map entries in the {@code partitionId}. The values are not fetched one-by-one but rather in batches.
 * The {@link Iterator#remove()} method is not supported and will throw an {@link UnsupportedOperationException}.
 * <b>NOTE</b>
 * Iterating the map should be done only when the {@link IMap} is not being
 * mutated and the cluster is stable (there are no migrations or membership changes).
 * In other cases, the iterator may not return some entries or may return an entry twice.
 */
public class ClientMapQueryPartitionIterator<K, V, R> extends AbstractMapQueryPartitionIterator<K, V, R> {

    private final ClientMapProxy<K, V> mapProxy;
    private final ClientContext context;

    public ClientMapQueryPartitionIterator(ClientMapProxy<K, V> mapProxy,
                                           ClientContext context,
                                           int fetchSize,
                                           int partitionId,
                                           Predicate<K, V> predicate,
                                           Projection<? super Entry<K, V>, R> projection) {
        super(mapProxy, fetchSize, partitionId, predicate, projection);
        this.mapProxy = mapProxy;
        this.context = context;
    }

    @Override
    protected List<Data> fetch() {
        final HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        final ClientMessage request = MapFetchWithQueryCodec.encodeRequest(mapProxy.getName(), lastTableIndex, fetchSize,
                getSerializationService().toData(query.getProjection()),
                getSerializationService().toData(query.getPredicate()));
        final ClientInvocation clientInvocation = new ClientInvocation(client, request, mapProxy.getName(), partitionId);
        try {
            final ClientInvocationFuture f = clientInvocation.invoke();
            final MapFetchWithQueryCodec.ResponseParameters responseParameters = MapFetchWithQueryCodec.decodeResponse(f.get());

            final List<Data> results = responseParameters.results;

            setLastTableIndex(results, responseParameters.nextTableIndexToReadFrom);
            return results;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected SerializationService getSerializationService() {
        return context.getSerializationService();
    }
}
