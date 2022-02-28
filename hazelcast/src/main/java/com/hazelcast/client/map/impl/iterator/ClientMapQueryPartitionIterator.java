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

package com.hazelcast.client.map.impl.iterator;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.impl.iterator.AbstractMapQueryPartitionIterator;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.internal.iteration.IterationPointer.decodePointers;
import static com.hazelcast.internal.iteration.IterationPointer.encodePointers;

/**
 * Iterator for iterating map entries in the {@code partitionId}. The values
 * are not fetched one-by-one but rather in batches.
 * The {@link Iterator#remove()} method is not supported and will throw a
 * {@link UnsupportedOperationException}.
 * <b>NOTE</b>
 * The iteration may be done when the map is being mutated or when there are
 * membership changes. The iterator does not reflect the state when it has
 * been constructed - it may return some entries that were added after the
 * iteration has started and may not return some entries that were removed
 * after iteration has started.
 * The iterator will not, however, skip an entry if it has not been changed
 * and will not return an entry twice.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 * @param <R> the return type of iterator after the projection
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
        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) context.getHazelcastInstance();
        ClientMessage request = MapFetchWithQueryCodec.encodeRequest(
                mapProxy.getName(),
                encodePointers(pointers),
                fetchSize,
                getSerializationService().toData(query.getProjection()),
                getSerializationService().toData(query.getPredicate()));
        ClientInvocation clientInvocation = new ClientInvocation(client, request, mapProxy.getName(), partitionId);
        try {
            ClientInvocationFuture f = clientInvocation.invoke();
            MapFetchWithQueryCodec.ResponseParameters responseParameters = MapFetchWithQueryCodec.decodeResponse(f.get());
            List<Data> results = responseParameters.results;
            IterationPointer[] pointers = decodePointers(responseParameters.iterationPointers);
            setLastTableIndex(results, pointers);
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
