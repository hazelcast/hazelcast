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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.cache.impl.operation.CacheMergeOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Handles split-brain functionality for cache.
 */
class CacheSplitBrainHandlerService implements SplitBrainHandlerService {

    private final int partitionCount;
    private final NodeEngine nodeEngine;
    private final CacheService cacheService;
    private final Map<String, CacheConfig> configs;
    private final CachePartitionSegment[] segments;
    private final OperationService operationService;
    private final IPartitionService partitionService;
    private final CacheMergePolicyProvider mergePolicyProvider;
    private final SerializationService serializationService;

    CacheSplitBrainHandlerService(NodeEngine nodeEngine, Map<String, CacheConfig> configs, CachePartitionSegment[] segments) {
        this.nodeEngine = nodeEngine;
        this.configs = configs;
        this.segments = segments;
        this.mergePolicyProvider = new CacheMergePolicyProvider(nodeEngine);
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();
        this.cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);
        this.serializationService = nodeEngine.getSerializationService();
        this.operationService = nodeEngine.getOperationService();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        Map<String, Map<Data, CacheRecord>> recordMap = createHashMap(configs.size());

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            // add your owned entries so they will be merged
            if (isLocalPartition(partitionId)) {
                CachePartitionSegment segment = segments[partitionId];
                Iterator<ICacheRecordStore> iterator = segment.recordStoreIterator();
                while (iterator.hasNext()) {
                    ICacheRecordStore cacheRecordStore = iterator.next();
                    if (!(cacheRecordStore instanceof SplitBrainAwareCacheRecordStore)) {
                        continue;
                    }
                    String cacheName = cacheRecordStore.getName();
                    Map<Data, CacheRecord> records = recordMap.get(cacheName);
                    if (records == null) {
                        records = createHashMap(cacheRecordStore.size());
                        recordMap.put(cacheName, records);
                    }
                    for (Map.Entry<Data, CacheRecord> cacheRecordEntry : cacheRecordStore.getReadOnlyRecords().entrySet()) {
                        Data key = cacheRecordEntry.getKey();
                        CacheRecord cacheRecord = cacheRecordEntry.getValue();
                        records.put(key, cacheRecord);
                    }
                    // clear all records either owned or backup
                    cacheRecordStore.clear();
                }
            }
        }

        invalidateNearCaches(recordMap);

        return new CacheMerger(recordMap);
    }

    /**
     * Sends cache invalidation event regardless if any actually cleared or not
     * (no need to know how many actually cleared)
     */
    private void invalidateNearCaches(Map<String, Map<Data, CacheRecord>> recordMap) {
        for (String cacheName : recordMap.keySet()) {
            cacheService.sendInvalidationEvent(cacheName, null, SOURCE_NOT_AVAILABLE);
        }
    }

    /**
     * @see IPartition#isLocal()
     */
    private boolean isLocalPartition(int partitionId) {
        IPartition partition = partitionService.getPartition(partitionId, false);
        return partition.isLocal();
    }

    private class CacheMerger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private final ILogger logger;
        private final Map<String, Map<Data, CacheRecord>> recordMap;

        CacheMerger(Map<String, Map<Data, CacheRecord>> recordMap) {
            this.recordMap = recordMap;
            this.logger = nodeEngine.getLogger(CacheService.class);
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
            for (Map.Entry<String, Map<Data, CacheRecord>> recordMapEntry : recordMap.entrySet()) {
                String cacheName = recordMapEntry.getKey();
                Map<Data, CacheRecord> records = recordMapEntry.getValue();
                CacheMergePolicy cacheMergePolicy = getCacheMergePolicy(cacheName);

                for (Map.Entry<Data, CacheRecord> entry : records.entrySet()) {
                    recordCount++;
                    Data key = entry.getKey();
                    CacheRecord record = entry.getValue();
                    CacheEntryView<Data, Data> entryView = new DefaultCacheEntryView(
                            key,
                            serializationService.toData(record.getValue()),
                            record.getCreationTime(),
                            record.getExpirationTime(),
                            record.getLastAccessTime(),
                            record.getAccessHit());

                    Operation operation = new CacheMergeOperation(cacheName, key, entryView, cacheMergePolicy);
                    try {
                        int partitionId = partitionService.getPartitionId(key);
                        operationService.invokeOnPartition(ICacheService.SERVICE_NAME, operation, partitionId)
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

    protected CacheMergePolicy getCacheMergePolicy(String cacheName) {
        CacheConfig cacheConfig = configs.get(cacheName);
        String mergePolicyName = cacheConfig.getMergePolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }
}
