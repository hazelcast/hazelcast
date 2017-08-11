/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.SynchronizeIndexesForPartitionTask;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// RU_COMPAT_V38

/**
 * This class is responsible for tracking cluster version changes and synchronized indexes if needed.
 * The synchronization will happen only on 3.8 to 3.9 cluster version change.
 *
 * It is a last-chance anit-entropy that guards the situation where the index definitions arrive after the map data.
 * In this case the MapContainer.indexesToAdd indexes will be send to each partition and the indexes will be populated.
 *
 * IMPORTANT: The synchronization applies to runtime partitioned indexes only.
 * It is impossible to apply this fix for global indexes to the their global nature. It would require re-adding all data.
 *
 * @see com.hazelcast.map.impl.operation.SynchronizeIndexesForPartitionTask
 * @see com.hazelcast.map.impl.operation.PostJoinMapOperation
 * @see com.hazelcast.map.impl.operation.MapReplicationStateHolder
 */
class MapIndexSynchronizer {

    protected MapServiceContext mapServiceContext;
    protected OperationService operationService;
    protected IPartitionService partitionService;
    protected SerializationService serializationService;
    protected NodeEngine nodeEngine;

    private ILogger logger;
    private Version currentVersion;

    public MapIndexSynchronizer(MapServiceContext mapServiceContext, OperationService operationService,
                                IPartitionService partitionService, SerializationService serializationService,
                                NodeEngine nodeEngine) {
        this.mapServiceContext = mapServiceContext;
        this.operationService = operationService;
        this.partitionService = partitionService;
        this.serializationService = serializationService;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    public void onClusterVersionChange(Version newVersion) {
        if (isV38toV39transition(newVersion)) {
            synchronizeIndexes();
        }
        currentVersion = newVersion;
    }

    private void synchronizeIndexes() {
        logger.info("Running MapIndexSynchronizer");
        List<MapIndexInfo> mapIndexInfos = getIndexesToSynchronize();
        if (!mapIndexInfos.isEmpty()) {
            executeIndexSync(mapIndexInfos);
        }
    }

    private List<MapIndexInfo> getIndexesToSynchronize() {
        List<MapIndexInfo> mapIndexInfos = new ArrayList<MapIndexInfo>();
        for (Map.Entry<String, MapContainer> entry : mapServiceContext.getMapContainers().entrySet()) {
            String mapName = entry.getKey();
            MapContainer mapContainer = entry.getValue();
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapName);
            int indexesToSynchronize = mapContainer.getIndexesToAdd().size();
            if (indexesToSynchronize > 0) {
                logger.info("Scheduling " + indexesToSynchronize + " indexes sync for map " + entry.getKey());
            }
            for (IndexInfo indexInfo : mapContainer.getIndexesToAdd()) {
                mapIndexInfo.addIndexInfo(indexInfo.getAttributeName(), indexInfo.isOrdered());
            }
            mapIndexInfos.add(mapIndexInfo);
            mapContainer.clearIndexesToAdd();
        }
        return mapIndexInfos;
    }

    private void executeIndexSync(List<MapIndexInfo> mapIndexInfos) {
        InternalPartitionServiceImpl internalPartitionService = ((InternalPartitionServiceImpl) partitionService);
        OperationServiceImpl operationServiceImpl = (OperationServiceImpl) operationService;
        for (int partitionId : internalPartitionService.getMemberPartitions(nodeEngine.getThisAddress())) {
            SynchronizeIndexesForPartitionTask task = new SynchronizeIndexesForPartitionTask(partitionId, mapIndexInfos,
                    mapServiceContext.getService(), serializationService);
            operationServiceImpl.execute(task);
        }
    }

    private boolean isV38toV39transition(Version newVersion) {
        return currentVersion != null && currentVersion.equals(Versions.V3_8) && newVersion.equals(Versions.V3_9);
    }
}
