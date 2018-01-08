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

import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;

class MapSplitBrainHandlerService implements SplitBrainHandlerService {

    private final int partitionCount;
    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final OperationService operationService;
    private final IPartitionService partitionService;
    private final MapServiceContext mapServiceContext;
    private final MergePolicyProvider mergePolicyProvider;

    MapSplitBrainHandlerService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();
        this.mergePolicyProvider = mapServiceContext.getMergePolicyProvider();
        this.operationService = nodeEngine.getOperationService();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        long now = Clock.currentTimeMillis();

        Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        Map<MapContainer, Collection<Record>> recordMap = createHashMap(mapContainers.size());

        for (MapContainer mapContainer : mapContainers.values()) {
            if (NATIVE.equals(mapContainer.getMapConfig().getInMemoryFormat())) {
                logger.warning("Split-brain recovery can not be applied NATIVE in-memory-formatted map ["
                        + mapContainer.name + ']');
                continue;
            }

            MapMergePolicy mergePolicy = getMapMergePolicy(mapContainer);
            boolean mergePartitionData = !(mergePolicy instanceof IgnoreMergingEntryMapMergePolicy);

            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
                //noinspection unchecked
                RecordStore<Record> recordStore = partitionContainer.getRecordStore(mapContainer.getName());
                // add your owned entries to the map so they will be merged
                if (mergePartitionData && isLocalPartition(partitionId)) {
                    Collection<Record> records = recordMap.get(mapContainer);
                    if (records == null) {
                        records = new LinkedList<Record>();
                        recordMap.put(mapContainer, records);
                    }
                    Iterator<Record> iterator = recordStore.iterator(now, false);
                    while (iterator.hasNext()) {
                        records.add(iterator.next());
                    }
                }
                // clear all records either owned or backup
                recordStore.reset();
                mapContainer.getIndexes(partitionId).clearIndexes();
            }
        }
        return new Merger(recordMap);
    }

    /**
     * @see IPartition#isLocal()
     */
    private boolean isLocalPartition(int partitionId) {
        IPartition partition = partitionService.getPartition(partitionId, false);
        return partition.isLocal();
    }

    private MapMergePolicy getMapMergePolicy(MapContainer mapContainer) {
        String mergePolicyName = mapContainer.getMapConfig().getMergePolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    private class Merger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private final ILogger logger;
        private final Map<MapContainer, Collection<Record>> recordMap;

        Merger(Map<MapContainer, Collection<Record>> recordMap) {
            this.recordMap = recordMap;
            this.logger = nodeEngine.getLogger(MapSplitBrainHandlerService.class);
        }

        @Override
        public void run() {
            final Semaphore semaphore = new Semaphore(0);

            ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Error while running merge operation: " + t.getMessage());
                    semaphore.release(1);
                }
            };

            int recordCount = 0;
            for (Map.Entry<MapContainer, Collection<Record>> recordMapEntry : recordMap.entrySet()) {
                MapContainer mapContainer = recordMapEntry.getKey();
                Collection<Record> recordList = recordMapEntry.getValue();

                String mapName = mapContainer.getName();
                MapMergePolicy mergePolicy = getMapMergePolicy(mapContainer);
                MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
                for (Record record : recordList) {
                    recordCount++;
                    Data key = record.getKey();
                    Data value = mapServiceContext.toData(record.getValue());
                    EntryView<Data, Data> entryView = createSimpleEntryView(key, value, record);

                    Operation operation = operationProvider.createMergeOperation(mapName, entryView, mergePolicy, false);
                    try {
                        int partitionId = partitionService.getPartitionId(key);
                        operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId)
                                .andThen(mergeCallback);
                    } catch (Throwable t) {
                        throw rethrow(t);
                    }
                }
            }
            try {
                semaphore.tryAcquire(recordCount, recordCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting merge operation...");
            }
        }
    }
}
