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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.ArrayList;
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
public class MapQueryPartitionIterator<K, V, R> extends AbstractMapQueryPartitionIterator<K, V, R> {

    private final MapProxyImpl<K, V> mapProxy;

    public MapQueryPartitionIterator(MapProxyImpl<K, V> mapProxy, int fetchSize, int partitionId,
                                     Predicate<K, V> predicate,
                                     Projection<? super Entry<K, V>, R> projection) {
        super(mapProxy, fetchSize, partitionId, predicate, projection);
        this.mapProxy = mapProxy;
        advance();
    }

    protected List<Data> fetch() {
        final MapOperation op = mapProxy.getOperationProvider()
                                        .createFetchWithQueryOperation(mapProxy.getName(), lastTableIndex, fetchSize, query);

        final ResultSegment segment = invoke(op);
        final QueryResult queryResult = (QueryResult) segment.getResult();

        final List<Data> serialized = new ArrayList<>(queryResult.size());
        for (QueryResultRow row : queryResult) {
            serialized.add(row.getValue());
        }

        setLastTableIndex(serialized, segment.getNextTableIndexToReadFrom());
        return serialized;
    }

    private ResultSegment invoke(Operation operation) {
        final InternalCompletableFuture<ResultSegment> future =
                mapProxy.getOperationService().invokeOnPartition(mapProxy.getServiceName(), operation, partitionId);
        return future.joinInternal();
    }

    @Override
    protected SerializationService getSerializationService() {
        return mapProxy.getNodeEngine().getSerializationService();
    }

}
