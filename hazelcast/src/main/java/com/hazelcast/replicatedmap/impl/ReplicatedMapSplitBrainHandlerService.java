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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.replicatedmap.impl.operation.MergeOperation;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.merge.MergePolicyProvider;
import com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Contains split-brain handling logic for {@link com.hazelcast.core.ReplicatedMap}
 */
public class ReplicatedMapSplitBrainHandlerService implements SplitBrainHandlerService {
    private final ReplicatedMapService service;
    private final MergePolicyProvider mergePolicyProvider;
    private final NodeEngine nodeEngine;
    private final SerializationService serializationService;

    public ReplicatedMapSplitBrainHandlerService(ReplicatedMapService service,
                                                 MergePolicyProvider mergePolicyProvider) {
        this.service = service;
        this.mergePolicyProvider = mergePolicyProvider;
        this.nodeEngine = service.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        HashMap<String, Collection<ReplicatedRecord>> recordMap = new HashMap<String, Collection<ReplicatedRecord>>();
        Address thisAddress = service.getNodeEngine().getThisAddress();
        List<Integer> partitions = nodeEngine.getPartitionService().getMemberPartitions(thisAddress);
        for (Integer partition : partitions) {
            PartitionContainer partitionContainer = service.getPartitionContainer(partition);
            ConcurrentMap<String, ReplicatedRecordStore> stores = partitionContainer.getStores();
            for (ReplicatedRecordStore store : stores.values()) {
                String name = store.getName();
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

        private static final int TIMEOUT_FACTOR = 500;

        Map<String, Collection<ReplicatedRecord>> recordMap;

        public Merger(Map<String, Collection<ReplicatedRecord>> recordMap) {
            this.recordMap = recordMap;
        }

        @Override
        public void run() {
            final Semaphore semaphore = new Semaphore(0);
            int recordCount = 0;
            final ILogger logger = nodeEngine.getLogger(ReplicatedMapService.class);

            ExecutionCallback mergeCallback = new ExecutionCallback() {
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

            for (Map.Entry<String, Collection<ReplicatedRecord>> entry : recordMap.entrySet()) {
                recordCount++;
                String name = entry.getKey();
                Collection<ReplicatedRecord> records = entry.getValue();
                ReplicatedMapConfig replicatedMapConfig = service.getReplicatedMapConfig(name);
                String mergePolicy = replicatedMapConfig.getMergePolicy();
                ReplicatedMapMergePolicy policy = mergePolicyProvider.getMergePolicy(mergePolicy);
                for (ReplicatedRecord record : records) {
                    ReplicatedMapEntryView entryView = createEntryView(record);
                    MergeOperation mergeOperation = new MergeOperation(name, record.getKeyInternal(), entryView, policy);
                    try {
                        int partitionId = nodeEngine.getPartitionService().getPartitionId(record.getKeyInternal());
                        ICompletableFuture f = nodeEngine.getOperationService()
                                .invokeOnPartition(SERVICE_NAME, mergeOperation, partitionId);
                        f.andThen(mergeCallback);
                    } catch (Throwable t) {
                        throw ExceptionUtil.rethrow(t);
                    }
                }
            }
            try {
                semaphore.tryAcquire(recordCount, recordCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting replicated map merge operation...");
            }

        }

        private ReplicatedMapEntryView createEntryView(ReplicatedRecord record) {
            Object key = serializationService.toObject(record.getKeyInternal());
            Object value = serializationService.toObject(record.getValueInternal());
            ReplicatedMapEntryView entryView = new ReplicatedMapEntryView(key, value);
            entryView.setHits(record.getHits());
            entryView.setTtl(record.getTtlMillis());
            entryView.setLastAccessTime(record.getLastAccessTime());
            entryView.setCreationTime(record.getCreationTime());
            entryView.setLastUpdateTime(record.getUpdateTime());
            return entryView;
        }
    }
}
