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
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

class MapReplicationSupportingService implements ReplicationSupportingService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    MapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
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
            Future f = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private void handleUpdate(MapReplicationUpdate replicationUpdate) {
        EntryView entryView = replicationUpdate.getEntryView();
        MapMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
        String mapName = replicationUpdate.getMapName();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        Data dataKey = mapServiceContext.toData(entryView.getKey(), mapContainer.getPartitioningStrategy());
        MapOperation operation = operationProvider.createMergeOperation(mapName, dataKey, entryView, mergePolicy, true);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(entryView.getKey());
            Future f = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }
}
