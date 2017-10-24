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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;

class MapSplitBrainHandlerService implements SplitBrainHandlerService {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;

    MapSplitBrainHandlerService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        long now = Clock.currentTimeMillis();
        Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        Map<MapContainer, Collection<Record>> recordMap = new HashMap<MapContainer, Collection<Record>>(mapContainers.size());
        ILogger logger = nodeEngine.getLogger(getClass());
        IPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Address thisAddress = nodeEngine.getClusterService().getThisAddress();

        for (MapContainer mapContainer : mapContainers.values()) {
            if (NATIVE.equals(mapContainer.getMapConfig().getInMemoryFormat())) {
                logger.warning("Split-brain recovery can not be applied NATIVE in-memory-formatted map ["
                        + mapContainer.name + ']');
                continue;
            }

            for (int i = 0; i < partitionCount; i++) {
                PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(i);
                //noinspection unchecked
                RecordStore<Record> recordStore = partitionContainer.getRecordStore(mapContainer.getName());

                // add your owned entries to the map so they will be merged
                if (thisAddress.equals(partitionService.getPartitionOwner(i))) {
                    MapMergePolicy mergePolicy = getMapMergePolicy(mapContainer);
                    if (!(mergePolicy instanceof IgnoreMergingEntryMapMergePolicy)) {
                        Collection<Record> records = recordMap.get(mapContainer);
                        if (records == null) {
                            records = new ArrayList<Record>();
                            recordMap.put(mapContainer, records);
                        }
                        Iterator<Record> iterator = recordStore.iterator(now, false);
                        while (iterator.hasNext()) {
                            records.add(iterator.next());
                        }
                    }
                }
                // clear all records either owned or backup
                recordStore.reset();
                mapContainer.getIndexes(i).clearIndexes();
            }
        }
        return new Merger(recordMap);
    }

    private MapMergePolicy getMapMergePolicy(MapContainer mapContainer) {
        String mergePolicyName = mapContainer.getMapConfig().getMergePolicy();
        return mapServiceContext.getMergePolicyProvider().getMergePolicy(mergePolicyName);
    }

    private class Merger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private final Map<MapContainer, Collection<Record>> recordMap;

        Merger(Map<MapContainer, Collection<Record>> recordMap) {
            this.recordMap = recordMap;
        }

        @Override
        public void run() {
            final ILogger logger = nodeEngine.getLogger(MapSplitBrainHandlerService.class);
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

                // TODO: number of records may be high
                // TODO: below can be optimized a many records can be sent in a single invocation
                MapMergePolicy mergePolicy = getMapMergePolicy(mapContainer);

                MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
                for (Record record : recordList) {
                    recordCount++;
                    EntryView<Data, Data> entryView = EntryViews.createSimpleEntryView(record.getKey(),
                            mapServiceContext.toData(record.getValue()), record);

                    MapOperation operation = operationProvider.createMergeOperation(mapName, record.getKey(), entryView,
                            mergePolicy, false);
                    try {
                        int partitionId = nodeEngine.getPartitionService().getPartitionId(record.getKey());
                        ICompletableFuture<Object> future = nodeEngine.getOperationService()
                                .invokeOnPartition(SERVICE_NAME, operation, partitionId);
                        future.andThen(mergeCallback);
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
