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

package com.hazelcast.map.impl;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.wan.WanMapAddOrUpdateEvent;
import com.hazelcast.map.impl.wan.WanMapRemoveEvent;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

class WanMapSupportingService implements WanSupportingService {
    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final WanEventCounters wanEventTypeCounters;

    WanMapSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.wanEventTypeCounters = nodeEngine.getWanReplicationService()
                .getReceivedEventCounters(MapService.SERVICE_NAME);
    }

    @Override
    public void onReplicationEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType) {
        if (event instanceof WanMapAddOrUpdateEvent) {
            handleAddOrUpdate((WanMapAddOrUpdateEvent) event);
        } else if (event instanceof WanMapRemoveEvent) {
            handleRemove((WanMapRemoveEvent) event);
        }
    }

    private void handleRemove(WanMapRemoveEvent replicationRemove) {
        String mapName = replicationRemove.getObjectName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        MapOperation operation = operationProvider.createDeleteOperation(replicationRemove.getObjectName(),
                replicationRemove.getKey(), true);

        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationRemove.getKey());
            Future future = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            future.get();
            wanEventTypeCounters.incrementRemove(mapName);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private void handleAddOrUpdate(WanMapAddOrUpdateEvent replicationUpdate) {
        SplitBrainMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
        String mapName = replicationUpdate.getObjectName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        SerializationService serializationService = nodeEngine.getSerializationService();
        MapMergeTypes<Object, Object> mergingEntry = createMergingEntry(serializationService, replicationUpdate.getEntryView());
        //noinspection unchecked
        MapOperation operation = operationProvider.createMergeOperation(mapName, mergingEntry,
                (SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object>) mergePolicy, true);

        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationUpdate.getEntryView().getKey());
            Future future = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            future.get();
            wanEventTypeCounters.incrementUpdate(mapName);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
