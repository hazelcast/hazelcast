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

package com.hazelcast.map.impl;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.internal.services.ReplicationSupportingService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.wan.MapReplicationRemove;
import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.wan.DistributedServiceWanEventCounters;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

class MapReplicationSupportingService implements ReplicationSupportingService {
    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final DistributedServiceWanEventCounters wanEventTypeCounters;

    MapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.wanEventTypeCounters = nodeEngine.getWanReplicationService()
                .getReceivedEventCounters(MapService.SERVICE_NAME);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent event, WanAcknowledgeType acknowledgeType) {
        if (event instanceof MapReplicationUpdate) {
            handleUpdate((MapReplicationUpdate) event);
        } else if (event instanceof MapReplicationRemove) {
            handleRemove((MapReplicationRemove) event);
        }
    }

    private void handleRemove(MapReplicationRemove replicationRemove) {
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

    private void handleUpdate(MapReplicationUpdate replicationUpdate) {
        SplitBrainMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
        String mapName = replicationUpdate.getObjectName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        SerializationService serializationService = nodeEngine.getSerializationService();
        MapMergeTypes mergingEntry = createMergingEntry(serializationService, replicationUpdate.getEntryView());
        //noinspection unchecked
        MapOperation operation = operationProvider.createMergeOperation(mapName, mergingEntry,
                (SplitBrainMergePolicy<Data, MapMergeTypes>) mergePolicy, true);

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
