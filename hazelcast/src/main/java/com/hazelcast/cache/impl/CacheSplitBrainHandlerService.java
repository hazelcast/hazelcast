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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.cache.impl.operation.CacheLegacyMergeOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Disposable;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.MergingEntryHolder;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.MutableLong;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MergePolicyConfig.DEFAULT_BATCH_SIZE;
import static com.hazelcast.spi.impl.merge.MergingHolders.createMergeHolder;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static java.util.Collections.singletonList;

/**
 * Handles split-brain functionality for cache.
 */
class CacheSplitBrainHandlerService implements SplitBrainHandlerService {

    protected static final long TIMEOUT_FACTOR = 500;

    protected final int partitionCount;
    protected final ILogger logger;
    protected final NodeEngine nodeEngine;
    protected final CacheService cacheService;
    protected final Map<String, CacheConfig> configs;
    protected final CachePartitionSegment[] segments;
    protected final OperationService operationService;
    protected final IPartitionService partitionService;
    protected final SerializationService serializationService;
    protected final CacheMergePolicyProvider mergePolicyProvider;

    CacheSplitBrainHandlerService(NodeEngine nodeEngine, Map<String, CacheConfig> configs, CachePartitionSegment[] segments) {
        this.nodeEngine = nodeEngine;
        this.configs = configs;
        this.segments = segments;
        this.mergePolicyProvider = new CacheMergePolicyProvider(nodeEngine);
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();
        this.cacheService = nodeEngine.getService(SERVICE_NAME);
        this.serializationService = nodeEngine.getSerializationService();
        this.operationService = nodeEngine.getOperationService();
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public Runnable prepareMergeRunnable() {
        Map<String, Map<Data, CacheRecord>> recordMap = createHashMap(configs.size());

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            // add your owned entries so they will be merged
            if (partitionService.isPartitionOwner(partitionId)) {
                CachePartitionSegment segment = segments[partitionId];
                List<Iterator<ICacheRecordStore>> iterators = iteratorsOf(segment);
                for (Iterator<ICacheRecordStore> iterator : iterators) {
                    while (iterator.hasNext()) {
                        ICacheRecordStore cacheRecordStore = iterator.next();
                        String cacheName = cacheRecordStore.getName();
                        if (cacheRecordStore.getConfig().getInMemoryFormat() == NATIVE
                                && nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                            logger.warning("Split-brain recovery can not be applied NATIVE in-memory-formatted cache ["
                                    + cacheName + ']');
                            continue;
                        }

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
                    }
                }
            }
        }

        invalidateNearCaches(recordMap);

        return new CacheMerger(recordMap);
    }

    // overridden on ee
    protected List<Iterator<ICacheRecordStore>> iteratorsOf(CachePartitionSegment segment) {
        return singletonList(segment.recordStoreIterator());
    }

    // overridden on ee
    protected void destroySegment(CachePartitionSegment segment) {
        // don't use iteratorsOf here
        Collection<ICacheRecordStore> recordStores = segment.recordStores.values();

        Iterator<ICacheRecordStore> iterator = recordStores.iterator();
        while (iterator.hasNext()) {
            try {
                ICacheRecordStore recordStore = iterator.next();
                recordStore.destroy();
            } finally {
                iterator.remove();
            }
        }
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

    private Object getCacheMergePolicy(String cacheName) {
        CacheConfig cacheConfig = configs.get(cacheName);
        String mergePolicyName = cacheConfig.getMergePolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    // TODO traverse over recordstores not copy to heap eagerly
    private class CacheMerger implements Runnable, Disposable {

        private final Semaphore semaphore = new Semaphore(0);
        private final ILogger logger = nodeEngine.getLogger(CacheService.class);

        private final Map<String, Map<Data, CacheRecord>> recordMap;

        CacheMerger(Map<String, Map<Data, CacheRecord>> recordMap) {
            this.recordMap = recordMap;
        }

        @Override
        public void run() {
            int recordCount = 0;
            for (Map.Entry<String, Map<Data, CacheRecord>> recordMapEntry : recordMap.entrySet()) {
                String cacheName = recordMapEntry.getKey();
                Map<Data, CacheRecord> records = recordMapEntry.getValue();
                Object mergePolicy = getCacheMergePolicy(cacheName);
                if (mergePolicy instanceof SplitBrainMergePolicy) {
                    // we cannot merge into a 3.9 cluster, since not all members may understand the MergeOperationFactory
                    // RU_COMPAT_3_9
                    if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                        logger.info("Cannot merge cache '" + cacheName
                                + "' with merge policy '" + mergePolicy.getClass().getName()
                                + "' until cluster is running version " + Versions.V3_10);
                        continue;
                    }
                    recordCount += handleMerge(cacheName, records, (SplitBrainMergePolicy) mergePolicy, DEFAULT_BATCH_SIZE);
                } else {
                    recordCount += handleMerge(cacheName, records, (CacheMergePolicy) mergePolicy);
                }
            }
            recordMap.clear();

            try {
                if (!semaphore.tryAcquire(recordCount, recordCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS)) {
                    logger.warning("Split-brain healing for caches didn't finish within the timeout...");
                }
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for split-brain healing of caches...");
                Thread.currentThread().interrupt();
            }
        }

        private int handleMerge(String name, Map<Data, CacheRecord> recordMap, SplitBrainMergePolicy mergePolicy, int batchSize) {
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
            List<MergingEntryHolder<Data, Data>>[] entriesPerPartition = new List[partitionCount];
            int recordCount = 0;
            for (Map.Entry<Data, CacheRecord> entry : recordMap.entrySet()) {
                recordCount++;
                Data key = entry.getKey();
                CacheRecord record = entry.getValue();
                int partitionId = partitionService.getPartitionId(key);
                List<MergingEntryHolder<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries == null) {
                    entries = new LinkedList<MergingEntryHolder<Data, Data>>();
                    entriesPerPartition[partitionId] = entries;
                }

                Data dataValue = serializationService.toData(record.getValue());
                MergingEntryHolder<Data, Data> mergingEntry = createMergeHolder(key, dataValue, record);
                entries.add(mergingEntry);

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

        private int handleMerge(String cacheName, Map<Data, CacheRecord> recordMap, CacheMergePolicy mergePolicy) {
            ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Error while running cache merge operation: " + t.getMessage());
                    semaphore.release(1);
                }
            };

            int recordCount = 0;
            for (Map.Entry<Data, CacheRecord> entry : recordMap.entrySet()) {
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

                Operation operation = new CacheLegacyMergeOperation(cacheName, key, entryView, mergePolicy);
                try {
                    int partitionId = partitionService.getPartitionId(key);
                    operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId)
                            .andThen(mergeCallback);
                } catch (Throwable t) {
                    throw rethrow(t);
                }
            }
            return recordCount;
        }

        private void sendBatch(String name, List<Integer> memberPartitions,
                               List<MergingEntryHolder<Data, Data>>[] entriesPerPartition,
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
            List<MergingEntryHolder<Data, Data>>[] entries = new List[size];
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
                                                 List<MergingEntryHolder<Data, Data>>[] entries, int totalSize) {
            try {
                CacheConfig cacheConfig = cacheService.getCacheConfig(name);
                CacheOperationProvider operationProvider = cacheService.getCacheOperationProvider(name,
                        cacheConfig.getInMemoryFormat());
                OperationFactory factory = operationProvider.createMergeOperationFactory(name, partitions, entries, mergePolicy);
                operationService.invokeOnPartitions(SERVICE_NAME, factory, partitions);
            } catch (Throwable t) {
                logger.warning("Error while running cache merge operation: " + t.getMessage());
                throw rethrow(t);
            } finally {
                semaphore.release(totalSize);
            }
        }

        @Override
        public void dispose() {
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                destroySegment(segments[partitionId]);
            }
        }
    }
}
