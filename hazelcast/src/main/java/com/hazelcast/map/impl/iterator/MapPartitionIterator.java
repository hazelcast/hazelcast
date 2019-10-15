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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.List;

/**
 * Iterator for iterating map entries in the {@code partitionId}. The values are not fetched one-by-one but rather in batches.
 * <b>NOTE</b>
 * Iterating the map should be done only when the {@link IMap} is not being
 * mutated and the cluster is stable (there are no migrations or membership changes).
 * In other cases, the iterator may not return some entries or may return an entry twice.
 */
public class MapPartitionIterator<K, V> extends AbstractMapPartitionIterator<K, V> {

    private final MapProxyImpl<K, V> mapProxy;

    public MapPartitionIterator(MapProxyImpl<K, V> mapProxy, int fetchSize, int partitionId, boolean prefetchValues) {
        super(mapProxy, fetchSize, partitionId, prefetchValues);
        this.mapProxy = mapProxy;
        advance();
    }

    protected List fetch() {
        final String name = mapProxy.getName();
        final MapOperationProvider operationProvider = mapProxy.getOperationProvider();
        final MapOperation operation = prefetchValues
                    ? operationProvider.createFetchEntriesOperation(name, lastTableIndex, fetchSize)
                    : operationProvider.createFetchKeysOperation(name, lastTableIndex, fetchSize);

        final AbstractCursor cursor = invoke(operation);
        setLastTableIndex(cursor.getBatch(), cursor.getNextTableIndexToReadFrom());
        return cursor.getBatch();
    }

    private <T extends AbstractCursor> T invoke(Operation operation) {
        final InvocationFuture<T> future =
                mapProxy.getOperationService().invokeOnPartition(mapProxy.getServiceName(), operation, partitionId);
        return future.joinInternal();
    }

    @Override
    protected SerializationService getSerializationService() {
        return mapProxy.getNodeEngine().getSerializationService();
    }

}
