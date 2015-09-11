/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.merge.entry.CacheEntryView;
import com.hazelcast.cache.impl.merge.entry.HeapDataCacheEntryView;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.cache.impl.operation.CacheMergeOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.util.ExceptionUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Cache Service is the main access point of JCache implementation.
 * <p>
 * This service is responsible for:
 * <ul>
 * <li>Creating and/or accessing the named {@link com.hazelcast.cache.impl.CacheRecordStore}.</li>
 * <li>Creating/Deleting the cache configuration of the named {@link com.hazelcast.cache.ICache}.</li>
 * <li>Registering/Deregistering of cache listeners.</li>
 * <li>Publish/dispatch cache events.</li>
 * <li>Enabling/Disabling statistic and management.</li>
 * <li>Data migration commit/rollback through {@link com.hazelcast.spi.MigrationAwareService}.</li>
 * </ul>
 * </p>
 * <p><b>WARNING:</b>This service is an optionally registered service which is enabled when {@link javax.cache.Caching}
 * class is found on the classpath.</p>
 * <p>
 * If registered, it will provide all the above cache operations for all partitions of the node which it
 * is registered on.
 * </p>
 * <p><b>Distributed Cache Name</b> is used for providing a unique name to a cache object to overcome cache manager
 * scoping which depends on URI and class loader parameters. It's a simple concatenation of CacheNamePrefix and
 * cache name where CacheNamePrefix is calculated by each cache manager
 * using {@link AbstractHazelcastCacheManager#cacheNamePrefix()}.
 * </p>
 */
public class CacheService
        extends BaseCacheService
        implements SplitBrainHandlerService {

    private CacheMergePolicyProvider mergePolicyProvider;

    @Override
    protected void postInit(NodeEngine nodeEngine, Properties properties) {
        super.postInit(nodeEngine, properties);
        mergePolicyProvider = new CacheMergePolicyProvider(nodeEngine);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        final Map<String, Map<Data, CacheRecord>> recordMap = new HashMap<String, Map<Data, CacheRecord>>(configs.size());
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitionCount = partitionService.getPartitionCount();
        final Address thisAddress = nodeEngine.getClusterService().getThisAddress();

        for (int i = 0; i < partitionCount; i++) {
            // Add your owned entries so they will be merged
            if (thisAddress.equals(partitionService.getPartitionOwner(i))) {
                CachePartitionSegment segment = segments[i];
                Iterator<ICacheRecordStore> iter = segment.recordStoreIterator();
                while (iter.hasNext()) {
                    ICacheRecordStore cacheRecordStore = iter.next();
                    String cacheName = cacheRecordStore.getName();
                    Map<Data, CacheRecord> records = recordMap.get(cacheName);
                    if (records == null) {
                        records = new HashMap<Data, CacheRecord>(cacheRecordStore.size());
                        recordMap.put(cacheName, records);
                    }
                    for (Map.Entry<Data, CacheRecord> cacheRecordEntry : cacheRecordStore.getReadOnlyRecords().entrySet()) {
                        Data key = cacheRecordEntry.getKey();
                        CacheRecord cacheRecord = cacheRecordEntry.getValue();
                        records.put(key, cacheRecord);
                    }
                    // Clear all records either owned or backup
                    cacheRecordStore.clear();
                }
            }
        }
        return new CacheMerger(nodeEngine, configs, recordMap, mergePolicyProvider);
    }

    private static class CacheMerger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private final NodeEngine nodeEngine;
        private final Map<String, CacheConfig> configs;
        private final Map<String, Map<Data, CacheRecord>> recordMap;
        private final CacheMergePolicyProvider mergePolicyProvider;
        private final ILogger logger;

        public CacheMerger(NodeEngine nodeEngine,
                           Map<String, CacheConfig> configs,
                           Map<String, Map<Data, CacheRecord>> recordMap,
                           CacheMergePolicyProvider mergePolicyProvider) {
            this.nodeEngine = nodeEngine;
            this.configs = configs;
            this.recordMap = recordMap;
            this.mergePolicyProvider = mergePolicyProvider;
            this.logger = nodeEngine.getLogger(CacheService.class);
        }

        @Override
        public void run() {
            final Semaphore semaphore = new Semaphore(0);
            int recordCount = 0;

            ExecutionCallback mergeCallback = new ExecutionCallback() {
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

            SerializationService serializationService = nodeEngine.getSerializationService();

            for (Map.Entry<String, Map<Data, CacheRecord>> recordMapEntry : recordMap.entrySet()) {
                String cacheName = recordMapEntry.getKey();
                CacheConfig cacheConfig = configs.get(cacheName);
                Map<Data, CacheRecord> records = recordMapEntry.getValue();
                String mergePolicyName = cacheConfig.getMergePolicy();
                final CacheMergePolicy cacheMergePolicy = mergePolicyProvider.getMergePolicy(mergePolicyName);
                for (Map.Entry<Data, CacheRecord> recordEntry : records.entrySet()) {
                    Data key = recordEntry.getKey();
                    CacheRecord record = recordEntry.getValue();
                    recordCount++;
                    CacheEntryView entryView =
                            new HeapDataCacheEntryView(
                                    key,
                                    serializationService.toData(record.getValue()),
                                    record.getExpirationTime(),
                                    record.getAccessHit());
                    CacheMergeOperation operation =
                            new CacheMergeOperation(
                                    cacheName,
                                    key,
                                    entryView,
                                    cacheMergePolicy);
                    try {
                        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
                        ICompletableFuture f =
                                nodeEngine.getOperationService()
                                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);

                        f.andThen(mergeCallback);
                    } catch (Throwable t) {
                        throw ExceptionUtil.rethrow(t);
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
