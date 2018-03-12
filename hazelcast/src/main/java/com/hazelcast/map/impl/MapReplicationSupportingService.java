/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.wan.MapReplicationRemove;
import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationService;

import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.util.ExceptionUtil.rethrow;

class MapReplicationSupportingService implements ReplicationSupportingService {
    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final WanReplicationService wanService;

    MapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.wanService = nodeEngine.getWanReplicationService();
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();
        if (eventObject instanceof MapReplicationUpdate) {
            handleUpdate((MapReplicationUpdate) eventObject);
        } else if (eventObject instanceof MapReplicationRemove) {
            handleRemove((MapReplicationRemove) eventObject);
        }
    }

    private void handleRemove(MapReplicationRemove replicationRemove) {
        String mapName = replicationRemove.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        MapOperation operation = operationProvider.createRemoveOperation(replicationRemove.getMapName(),
                replicationRemove.getKey(), true);

        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationRemove.getKey());
            Future future = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            future.get();
            wanService.getReceivedEventCounter(MapService.SERVICE_NAME).incrementRemove(mapName);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private void handleUpdate(MapReplicationUpdate replicationUpdate) {
        Object mergePolicy = replicationUpdate.getMergePolicy();
        String mapName = replicationUpdate.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        MapOperation operation;
        if (mergePolicy instanceof SplitBrainMergePolicy) {
            SerializationService serializationService = nodeEngine.getSerializationService();
            MapMergeTypes mergingEntry = createMergingEntry(serializationService, replicationUpdate.getEntryView());
            //noinspection unchecked
            operation = operationProvider.createMergeOperation(mapName, mergingEntry,
                    (SplitBrainMergePolicy<Data, MapMergeTypes>) mergePolicy, true);
        } else {
            EntryView<Data, Data> entryView = replicationUpdate.getEntryView();
            operation = operationProvider.createLegacyMergeOperation(mapName, entryView, (MapMergePolicy) mergePolicy, true);
        }
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationUpdate.getEntryView().getKey());
            Future future = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            future.get();
            wanService.getReceivedEventCounter(MapService.SERVICE_NAME).incrementUpdate(mapName);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
