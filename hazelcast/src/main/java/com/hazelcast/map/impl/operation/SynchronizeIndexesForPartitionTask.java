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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// RU_COMPAT_3_9

/**
 * A task that is sent to all local partitions in order to add the indexes that have been specified in the task.
 * Each task is executed for a separate partition.
 *
 * It is a compatibility mode between 3.9 and 3.10 nodes. It is only necessary due to race between
 * PostJoinMapOperations and MapReplicationOperations, so the map data may arrive before the index definitions.
 *
 * This task is responsible to put the index instances back in sync with the definitions.
 *
 * @see com.hazelcast.map.impl.MapIndexSynchronizer
 * @see com.hazelcast.map.impl.operation.PostJoinMapOperation
 * @see com.hazelcast.map.impl.operation.MapReplicationStateHolder
 */
public class SynchronizeIndexesForPartitionTask implements PartitionSpecificRunnable {

    private final int partitionId;
    // list is reused by multiple tasks, should not be modified.
    private final List<MapIndexInfo> mapIndexInfos;

    private final MapService mapService;
    private final SerializationService serializationService;
    private final InternalPartitionService partitionService;

    public SynchronizeIndexesForPartitionTask(int partitionId, List<MapIndexInfo> mapIndexInfos, MapService mapService,
                                              SerializationService serializationService,
                                              InternalPartitionService partitionService) {
        this.partitionId = partitionId;
        this.mapIndexInfos = mapIndexInfos;
        this.mapService = mapService;
        this.serializationService = serializationService;
        this.partitionService = partitionService;
    }

    @Override
    public void run() {
        InternalPartition partition = partitionService.getPartition(partitionId, false);
        if (!partition.isLocal() || partition.isMigrating()) {
            return;
        }

        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        long now = Clock.currentTimeMillis();

        for (MapIndexInfo mapIndexInfo : mapIndexInfos) {
            MapContainer mapContainer = mapServiceContext.getMapContainer(mapIndexInfo.getMapName());
            Indexes indexes = mapContainer.getIndexes(partitionId);

            // identify missing indexes
            List<IndexInfo> missingIndexes = new ArrayList<IndexInfo>();
            for (IndexInfo indexInfo : mapIndexInfo.getIndexInfos()) {
                if (indexes.getIndex(indexInfo.getAttributeName()) == null) {
                    indexes.addOrGetIndex(indexInfo.getAttributeName(), indexInfo.isOrdered());
                    missingIndexes.add(indexInfo);
                }
            }

            // recreate missing indexes
            RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), mapIndexInfo.getMapName());
            Iterator<Record> iterator = recordStore.iterator(now, false);
            while (iterator.hasNext()) {
                Record record = iterator.next();
                Data key = record.getKey();
                Object value = Records.getValueOrCachedValue(record, serializationService);
                QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
                for (IndexInfo missingIndex : missingIndexes) {
                    indexes.getIndex(missingIndex.getAttributeName())
                           .saveEntryIndex(queryEntry, null, Index.OperationSource.SYSTEM);
                }
            }
        }
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
