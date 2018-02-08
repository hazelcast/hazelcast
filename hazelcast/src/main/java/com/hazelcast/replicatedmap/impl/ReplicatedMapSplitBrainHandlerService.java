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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.replicatedmap.impl.operation.LegacyMergeOperation;
import com.hazelcast.replicatedmap.impl.operation.MergeOperationFactory;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.merge.MergePolicyProvider;
import com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.MutableLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.spi.impl.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Contains split-brain handling logic for {@link com.hazelcast.core.ReplicatedMap}.
 */
class ReplicatedMapSplitBrainHandlerService implements SplitBrainHandlerService {

    private final ReplicatedMapService service;
    private final MergePolicyProvider mergePolicyProvider;
    private final NodeEngine nodeEngine;
    private final IPartitionService partitionService;
    private final SerializationService serializationService;

    ReplicatedMapSplitBrainHandlerService(ReplicatedMapService service, MergePolicyProvider mergePolicyProvider) {
        this.service = service;
        this.mergePolicyProvider = mergePolicyProvider;
        this.nodeEngine = service.getNodeEngine();
        this.partitionService = nodeEngine.getPartitionService();
        this.serializationService = nodeEngine.getSerializationService();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        HashMap<String, Collection<ReplicatedRecord>> recordMap = new HashMap<String, Collection<ReplicatedRecord>>();
        for (Integer partition : partitionService.getMemberPartitions(nodeEngine.getThisAddress())) {
            PartitionContainer partitionContainer = service.getPartitionContainer(partition);
            ConcurrentMap<String, ReplicatedRecordStore> stores = partitionContainer.getStores();
            for (ReplicatedRecordStore store : stores.values()) {
                String name = store.getName();
                Object mergePolicy = getMergePolicy(service.getReplicatedMapConfig(name).getMergePolicyConfig());
                if (mergePolicy instanceof DiscardMergePolicy) {
                    continue;
                }

                Collection<ReplicatedRecord> records = recordMap.get(name);
                if (records == null) {
                    records = new ArrayList<ReplicatedRecord>();
                }
                Iterator<ReplicatedRecord> iterator = store.recordIterator();
                while (iterator.hasNext()) {
                    ReplicatedRecord record = iterator.next();
                    records.add(record);
                }
                recordMap.put(name, records);
                store.reset();
            }
        }
        return new Merger(recordMap);
    }

    private class Merger implements Runnable {

        private static final long TIMEOUT_FACTOR = 500;

        private final ILogger logger = nodeEngine.getLogger(ReplicatedMapSplitBrainHandlerService.class);
        private final Semaphore semaphore = new Semaphore(0);

        private final Map<String, Collection<ReplicatedRecord>> recordMap;

        Merger(Map<String, Collection<ReplicatedRecord>> recordMap) {
            this.recordMap = recordMap;
        }

        @Override
        public void run() {
            int recordCount = 0;
            for (Map.Entry<String, Collection<ReplicatedRecord>> entry : recordMap.entrySet()) {
                String name = entry.getKey();
                MergePolicyConfig mergePolicyConfig = service.getReplicatedMapConfig(name).getMergePolicyConfig();
                Object mergePolicy = getMergePolicy(mergePolicyConfig);
                if (mergePolicy instanceof SplitBrainMergePolicy) {
                    // we cannot merge into a 3.9 cluster, since not all members may understand the MergeOperationFactory
                    // RU_COMPAT_3_9
                    if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                        logger.info("Cannot merge replicated map '" + name
                                + "' with merge policy '" + mergePolicyConfig.getPolicy()
                                + "' until cluster is running version " + Versions.V3_10);
                        continue;
                    }
                    int batchSize = mergePolicyConfig.getBatchSize();
                    recordCount += handleMerge(name, entry.getValue(), (SplitBrainMergePolicy) mergePolicy, batchSize);
                } else {
                    recordCount += handleMerge(name, entry.getValue(), (ReplicatedMapMergePolicy) mergePolicy);
                }
            }

            try {
                if (!semaphore.tryAcquire(recordCount, recordCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS)) {
                    logger.warning("Split-brain healing for replicated maps didn't finish within the timeout...");
                }
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for split-brain healing of replicated maps...");
                Thread.currentThread().interrupt();
            }
        }

        private int handleMerge(String name, Collection<ReplicatedRecord> recordList, SplitBrainMergePolicy mergePolicy,
                                int batchSize) {
            int partitionCount = partitionService.getPartitionCount();
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
            List<SplitBrainMergeEntryView<Object, Object>>[] entriesPerPartition = new List[partitionCount];
            int recordCount = 0;
            for (ReplicatedRecord record : recordList) {
                recordCount++;
                int partitionId = partitionService.getPartitionId(record.getKeyInternal());
                List<SplitBrainMergeEntryView<Object, Object>> entries = entriesPerPartition[partitionId];
                if (entries == null) {
                    entries = new LinkedList<SplitBrainMergeEntryView<Object, Object>>();
                    entriesPerPartition[partitionId] = entries;
                }

                SplitBrainMergeEntryView<Object, Object> entryView = createSplitBrainMergeEntryView(record);
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

        private void sendBatch(String name, List<Integer> memberPartitions,
                               List<SplitBrainMergeEntryView<Object, Object>>[] entriesPerPartition,
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
            List<SplitBrainMergeEntryView<Object, Object>>[] entries = new List[size];
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
                                                 List<SplitBrainMergeEntryView<Object, Object>>[] entries, int totalSize) {
            try {
                OperationFactory factory = new MergeOperationFactory(name, partitions, entries, mergePolicy);
                nodeEngine.getOperationService()
                        .invokeOnPartitions(ReplicatedMapService.SERVICE_NAME, factory, partitions);
            } catch (Throwable t) {
                logger.warning("Error while running replicated map merge operation: " + t.getMessage());
                throw rethrow(t);
            } finally {
                semaphore.release(totalSize);
            }
        }

        private int handleMerge(String name, Collection<ReplicatedRecord> recordList, ReplicatedMapMergePolicy mergePolicy) {
            ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Error while running replicated map merge operation: " + t.getMessage());
                    semaphore.release(1);
                }
            };

            int recordCount = 0;
            for (ReplicatedRecord record : recordList) {
                recordCount++;

                ReplicatedMapEntryView entryView = createEntryView(record);
                LegacyMergeOperation operation = new LegacyMergeOperation(name, record.getKeyInternal(), entryView, mergePolicy);
                try {
                    int partitionId = partitionService.getPartitionId(record.getKeyInternal());
                    nodeEngine.getOperationService()
                            .invokeOnPartition(SERVICE_NAME, operation, partitionId)
                            .andThen(mergeCallback);
                } catch (Throwable t) {
                    throw rethrow(t);
                }
            }
            return recordCount;
        }

        private ReplicatedMapEntryView createEntryView(ReplicatedRecord record) {
            return new ReplicatedMapEntryView<Object, Object>()
                    .setKey(serializationService.toObject(record.getKeyInternal()))
                    .setValue(serializationService.toObject(record.getValueInternal()))
                    .setHits(record.getHits())
                    .setTtl(record.getTtlMillis())
                    .setLastAccessTime(record.getLastAccessTime())
                    .setCreationTime(record.getCreationTime())
                    .setLastUpdateTime(record.getUpdateTime());
        }
    }

    private Object getMergePolicy(MergePolicyConfig mergePolicyConfig) {
        return mergePolicyProvider.getMergePolicy(mergePolicyConfig.getPolicy());
    }
}
