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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.MutableLong;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;
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
            MapConfig mapConfig = mapContainer.getMapConfig();
            if (NATIVE.equals(mapConfig.getInMemoryFormat())) {
                logger.warning("Split-brain recovery can not be applied to NATIVE in-memory formatted map ["
                        + mapContainer.name + ']');
                continue;
            }

            Object mergePolicy = getMergePolicy(mapConfig.getMergePolicyConfig());
            boolean mergePartitionData = !(mergePolicy instanceof IgnoreMergingEntryMapMergePolicy)
                    && !(mergePolicy instanceof DiscardMergePolicy);

            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
                //noinspection unchecked
                RecordStore<Record> recordStore = partitionContainer.getRecordStore(mapContainer.getName());
                // add your owned entries to the map so they will be merged
                if (mergePartitionData && partitionService.isPartitionOwner(partitionId)) {
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

    private Object getMergePolicy(MergePolicyConfig config) {
        return mergePolicyProvider.getMergePolicy(config.getPolicy());
    }

    private class Merger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private final Semaphore semaphore = new Semaphore(0);
        private final Map<MapContainer, Collection<Record>> recordMap;
        private final ILogger logger;

        Merger(Map<MapContainer, Collection<Record>> recordMap) {
            this.recordMap = recordMap;
            this.logger = nodeEngine.getLogger(MapSplitBrainHandlerService.class);
        }

        @Override
        public void run() {
            int recordCount = 0;
            for (Map.Entry<MapContainer, Collection<Record>> recordMapEntry : recordMap.entrySet()) {
                MapContainer mapContainer = recordMapEntry.getKey();
                Collection<Record> recordList = recordMapEntry.getValue();

                String mapName = mapContainer.getName();
                MergePolicyConfig mergePolicyConfig = mapContainer.getMapConfig().getMergePolicyConfig();
                Object mergePolicy = getMergePolicy(mergePolicyConfig);
                if (mergePolicy instanceof SplitBrainMergePolicy) {
                    // we cannot merge into a 3.9 cluster, since not all members may understand the MergeOperationFactory
                    // RU_COMPAT_3_9
                    if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                        logger.info("Cannot merge map '" + mapName + "' with merge policy '" + mergePolicyConfig.getPolicy()
                                + "' until cluster is running version " + Versions.V3_10);
                        continue;
                    }
                    int batchSize = mergePolicyConfig.getBatchSize();
                    recordCount += handleMerge(mapName, recordList, (SplitBrainMergePolicy) mergePolicy, batchSize);
                } else {
                    recordCount += handleMerge(mapName, recordList, (MapMergePolicy) mergePolicy);
                }
            }
            recordMap.clear();

            try {
                semaphore.tryAcquire(recordCount, recordCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for merge operation...");
            }
        }

        private int handleMerge(String name, Collection<Record> recordList, SplitBrainMergePolicy mergePolicy, int batchSize) {
            Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();

            // create a mapping between partition IDs and
            // a) an entry counter per member (a batch operation is sent out once this counter matches the batch size)
            // b) the member address (so we can retrieve the target address from the current partition ID)
            MutableLong[] counterPerMember = new MutableLong[partitionCount];
            Address[] addresses = new Address[partitionCount];
            for (Map.Entry<Address, List<Integer>> addressListEntry : memberPartitionsMap.entrySet()) {
                MutableLong counter = new MutableLong();
                Address address = addressListEntry.getKey();
                for (int partitionId : addressListEntry.getValue()) {
                    counterPerMember[partitionId] = counter;
                    addresses[partitionId] = address;
                }
            }

            // sort the entries per partition and send out batch operations (multiple partitions per member)
            //noinspection unchecked
            List<SplitBrainMergeEntryView<Data, Data>>[] entriesPerPartition = new List[partitionCount];
            int recordCount = 0;
            for (Record record : recordList) {
                recordCount++;
                int partitionId = partitionService.getPartitionId(record.getKey());
                List<SplitBrainMergeEntryView<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries == null) {
                    entries = new LinkedList<SplitBrainMergeEntryView<Data, Data>>();
                    entriesPerPartition[partitionId] = entries;
                }

                Data dataValue = mapServiceContext.toData(record.getValue());
                SplitBrainMergeEntryView<Data, Data> entryView = createSplitBrainMergeEntryView(record, dataValue);
                entries.add(entryView);

                long currentSize = ++counterPerMember[partitionId].value;
                if (currentSize % batchSize == 0) {
                    List<Integer> partitions = memberPartitionsMap.get(addresses[partitionId]);
                    sendBatch(name, partitions, entriesPerPartition, mergePolicy);
                }
            }
            // invoke operations for remaining entriesPerPartition
            for (Map.Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
                sendBatch(name, entry.getValue(), entriesPerPartition, mergePolicy);
            }
            return recordCount;
        }

        private int handleMerge(String name, Collection<Record> recordList, MapMergePolicy mergePolicy) {
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
            MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(name);

            int recordCount = 0;
            for (Record record : recordList) {
                recordCount++;
                Data key = record.getKey();
                Data value = mapServiceContext.toData(record.getValue());
                EntryView<Data, Data> entryView = createSimpleEntryView(key, value, record);

                Operation operation = operationProvider.createLegacyMergeOperation(name, entryView, mergePolicy, false);
                try {
                    int partitionId = partitionService.getPartitionId(key);
                    operationService
                            .invokeOnPartition(SERVICE_NAME, operation, partitionId)
                            .andThen(mergeCallback);
                } catch (Throwable t) {
                    throw rethrow(t);
                }
            }
            return recordCount;
        }

        private void sendBatch(String name, List<Integer> memberPartitions,
                               List<SplitBrainMergeEntryView<Data, Data>>[] entriesPerPartition,
                               SplitBrainMergePolicy mergePolicy) {
            int size = memberPartitions.size();
            int[] partitions = new int[size];
            int index = 0;
            for (Integer partitionId : memberPartitions) {
                if (entriesPerPartition[partitionId] != null) {
                    partitions[index++] = partitionId;
                }
            }
            if (index == 0) {
                return;
            }
            // trim partition array to real size
            if (index < size) {
                partitions = Arrays.copyOf(partitions, index);
                size = index;
            }

            //noinspection unchecked
            List<SplitBrainMergeEntryView<Data, Data>>[] entries = new List[size];
            index = 0;
            int totalSize = 0;
            for (int partitionId : partitions) {
                int batchSize = entriesPerPartition[partitionId].size();
                entries[index++] = entriesPerPartition[partitionId];
                totalSize += batchSize;
                entriesPerPartition[partitionId] = null;
            }
            if (totalSize == 0) {
                return;
            }

            invokeMergeOperationFactory(name, mergePolicy, partitions, entries, totalSize);
        }

        private void invokeMergeOperationFactory(String name, SplitBrainMergePolicy mergePolicy, int[] partitions,
                                                 List<SplitBrainMergeEntryView<Data, Data>>[] entries, int totalSize) {
            try {
                MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(name);
                OperationFactory factory = operationProvider.createMergeOperationFactory(name, partitions, entries, mergePolicy);
                operationService.invokeOnPartitions(SERVICE_NAME, factory, partitions);
            } catch (Throwable t) {
                logger.warning("Error while running merge operation: " + t.getMessage());
                throw rethrow(t);
            } finally {
                semaphore.release(totalSize);
            }
        }
    }
}
