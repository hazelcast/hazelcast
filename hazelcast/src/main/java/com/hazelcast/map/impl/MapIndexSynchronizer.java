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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.SynchronizeIndexesForPartitionTask;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// RU_COMPAT_V3_9

/**
 * This class is responsible for tracking cluster version changes and synchronizes indexes if needed.
 * The synchronization will happen only on 3.9 to 3.10 cluster version change.
 *
 * It is a last-chance anti-entropy that guards the situation where the index definitions arrive after the map data.
 * In this case the MapContainer.indexesToAdd indexes will be send to each partition and the indexes will be populated.
 *
 * IMPORTANT: The synchronization applies to runtime partitioned indexes only.
 * It is impossible to apply this fix for global indexes due to the their global nature.
 * It would require re-adding all data, since you don't know if the data from this partition has been added
 * to the index just by checking if the index exists or not.
 *
 * @see com.hazelcast.map.impl.operation.SynchronizeIndexesForPartitionTask
 * @see com.hazelcast.map.impl.operation.PostJoinMapOperation
 * @see com.hazelcast.map.impl.operation.MapReplicationStateHolder
 */
class MapIndexSynchronizer {

    protected MapServiceContext mapServiceContext;
    protected OperationService operationService;
    protected SerializationService serializationService;
    protected NodeEngine nodeEngine;

    private ILogger logger;
    private Version currentVersion;

    public MapIndexSynchronizer(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        this.mapServiceContext = mapServiceContext;
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = nodeEngine.getSerializationService();
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    public void onClusterVersionChange(Version newVersion) {
        if (isV39toV310transition(newVersion)) {
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
            MapContainer mapContainer = entry.getValue();
            int indexesToSynchronize = mapContainer.getPartitionIndexesToAdd().size();
            if (indexesToSynchronize > 0) {
                String mapName = entry.getKey();
                logger.info("Scheduling " + indexesToSynchronize + " indexes sync for map " + mapName);
                MapIndexInfo mapIndexInfo = new MapIndexInfo(mapName);
                for (IndexInfo indexInfo : mapContainer.getPartitionIndexesToAdd()) {
                    mapIndexInfo.addIndexInfo(indexInfo.getAttributeName(), indexInfo.isOrdered());
                }
                mapIndexInfos.add(mapIndexInfo);
            }
            mapContainer.clearPartitionIndexesToAdd();
        }
        return mapIndexInfos;
    }

    private void executeIndexSync(List<MapIndexInfo> mapIndexInfos) {
        OperationServiceImpl operationServiceImpl = (OperationServiceImpl) operationService;
        for (int partitionId : mapServiceContext.getOwnedPartitions()) {
            SynchronizeIndexesForPartitionTask task = new SynchronizeIndexesForPartitionTask(partitionId, mapIndexInfos,
                    mapServiceContext.getService(), serializationService,
                    (InternalPartitionService) nodeEngine.getPartitionService());
            operationServiceImpl.execute(task);
        }
    }

    private boolean isV39toV310transition(Version newVersion) {
        return currentVersion != null && currentVersion.equals(Versions.V3_9) && newVersion.equals(Versions.V3_10);
    }
}
