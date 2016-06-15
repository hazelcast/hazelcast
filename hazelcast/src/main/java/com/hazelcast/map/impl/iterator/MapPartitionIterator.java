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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.serialization.SerializationService;
import java.util.List;

public class MapPartitionIterator<K, V> extends AbstractMapPartitionIterator<K, V> {

    private final MapProxyImpl<K, V> mapProxy;

    public MapPartitionIterator(MapProxyImpl<K, V> mapProxy, int fetchSize, int partitionId, boolean prefetchValues) {
        super(mapProxy, fetchSize, partitionId, prefetchValues);
        this.mapProxy = mapProxy;
        advance();
    }

    protected List fetch() {
        String name = mapProxy.getName();
        String serviceName = mapProxy.getServiceName();
        MapOperationProvider operationProvider = mapProxy.getOperationProvider();
        OperationService operationService = mapProxy.getOperationService();
        if (prefetchValues) {
            MapOperation operation = operationProvider.createFetchEntriesOperation(name, lastTableIndex, fetchSize);
            InternalCompletableFuture<MapEntriesWithCursor> future = operationService
                    .invokeOnPartition(serviceName, operation, partitionId);
            MapEntriesWithCursor mapEntriesWithCursor = future.join();
            setLastTableIndex(mapEntriesWithCursor.getEntries(), mapEntriesWithCursor.getNextTableIndexToReadFrom());
            return mapEntriesWithCursor.getEntries();
        } else {
            MapOperation operation = operationProvider.createFetchKeysOperation(name, lastTableIndex, fetchSize);
            InternalCompletableFuture<MapKeysWithCursor> future = operationService
                    .invokeOnPartition(serviceName, operation, partitionId);
            MapKeysWithCursor mapKeysWithCursor = future.join();
            setLastTableIndex(mapKeysWithCursor.getKeys(), mapKeysWithCursor.getNextTableIndexToReadFrom());
            return mapKeysWithCursor.getKeys();
        }
    }

    @Override
    protected SerializationService getSerializationService() {
        return mapProxy.getNodeEngine().getSerializationService();
    }

}
