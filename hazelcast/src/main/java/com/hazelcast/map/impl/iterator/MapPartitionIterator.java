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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.List;

/**
 * Iterator for iterating map entries in a single partition.
 * The values are fetched in batches.
 * <p>
 * <b>NOTE</b>
 * The iteration may be done when the map is being mutated or when there are
 * membership changes. The iterator does not reflect the state when it has
 * been constructed - it may return some entries that were added after the
 * iteration has started and may not return some entries that were removed
 * after iteration has started.
 * The iterator will not, however, skip an entry if it has not been changed
 * and will not return an entry twice.
 */
public class MapPartitionIterator<K, V> extends AbstractMapPartitionIterator<K, V> {

    private final MapProxyImpl<K, V> mapProxy;

    public MapPartitionIterator(MapProxyImpl<K, V> mapProxy, int fetchSize, int partitionId, boolean prefetchValues) {
        super(mapProxy, fetchSize, partitionId, prefetchValues);
        this.mapProxy = mapProxy;
        advance();
    }

    protected List fetch() {
        String name = mapProxy.getName();
        MapOperationProvider operationProvider = mapProxy.getOperationProvider();
        MapOperation operation = prefetchValues
                ? operationProvider.createFetchEntriesOperation(name, pointers, fetchSize)
                : operationProvider.createFetchKeysOperation(name, pointers, fetchSize);

        AbstractCursor cursor = invoke(operation);
        setIterationPointers(cursor.getBatch(), cursor.getIterationPointers());
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
