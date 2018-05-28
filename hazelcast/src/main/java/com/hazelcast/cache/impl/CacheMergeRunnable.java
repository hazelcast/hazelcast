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
import com.hazelcast.cache.impl.operation.CacheLegacyMergeOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.merge.AbstractMergeRunnable;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.util.function.BiConsumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.config.MergePolicyConfig.DEFAULT_BATCH_SIZE;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

class CacheMergeRunnable extends AbstractMergeRunnable<Data, Data, ICacheRecordStore, CacheMergeTypes> {

    private final CacheService cacheService;
    private final ConcurrentMap<String, CacheConfig> configs;

    CacheMergeRunnable(Collection<ICacheRecordStore> mergingStores,
                       CacheSplitBrainHandlerService splitBrainHandlerService,
                       NodeEngine nodeEngine) {
        super(CacheService.SERVICE_NAME, mergingStores, splitBrainHandlerService, nodeEngine);

        this.cacheService = nodeEngine.getService(SERVICE_NAME);
        this.configs = new ConcurrentHashMap<String, CacheConfig>(cacheService.getConfigs());
    }

    @Override
    protected void onRunStart() {
        super.onRunStart();

        for (CacheConfig cacheConfig : configs.values()) {
            cacheService.putCacheConfigIfAbsent(cacheConfig);
        }
    }

    @Override
    protected void onMerge(String cacheName) {
        cacheService.sendInvalidationEvent(cacheName, null, SOURCE_NOT_AVAILABLE);
    }

    @Override
    protected void mergeStore(ICacheRecordStore store, BiConsumer<Integer, CacheMergeTypes> consumer) {
        int partitionId = store.getPartitionId();

        for (Map.Entry<Data, CacheRecord> entry : store.getReadOnlyRecords().entrySet()) {
            Data key = toHeapData(entry.getKey());
            CacheRecord record = entry.getValue();
            Data dataValue = toHeapData(record.getValue());

            consumer.accept(partitionId, createMergingEntry(getSerializationService(), key, dataValue, record));
        }
    }

    @Override
    protected void mergeStoreLegacy(ICacheRecordStore recordStore, BiConsumer<Integer, Operation> consumer) {
        int partitionId = recordStore.getPartitionId();
        String name = recordStore.getName();
        CacheMergePolicy mergePolicy = ((CacheMergePolicy) getMergePolicy(name));

        for (Map.Entry<Data, CacheRecord> entry : recordStore.getReadOnlyRecords().entrySet()) {
            Data key = entry.getKey();
            CacheRecord record = entry.getValue();
            CacheEntryView<Data, Data> entryView = new DefaultCacheEntryView(
                    key,
                    toData(record.getValue()),
                    record.getCreationTime(),
                    record.getExpirationTime(),
                    record.getLastAccessTime(),
                    record.getAccessHit());

            consumer.accept(partitionId, new CacheLegacyMergeOperation(name, key, entryView, mergePolicy));
        }
    }

    @Override
    protected InMemoryFormat getInMemoryFormat(String dataStructureName) {
        return cacheService.getConfigs().get(dataStructureName).getInMemoryFormat();
    }

    @Override
    protected int getBatchSize(String dataStructureName) {
        // the batch size cannot be retrieved from the MergePolicyConfig,
        // because there is no MergePolicyConfig in CacheConfig
        // (adding it breaks backward compatibility)
        return DEFAULT_BATCH_SIZE;
    }

    @Override
    protected Object getMergePolicy(String dataStructureName) {
        return cacheService.getMergePolicy(dataStructureName);
    }

    @Override
    protected String getDataStructureName(ICacheRecordStore iCacheRecordStore) {
        return iCacheRecordStore.getName();
    }

    @Override
    protected int getPartitionId(ICacheRecordStore store) {
        return store.getPartitionId();
    }

    @Override
    protected OperationFactory createMergeOperationFactory(String dataStructureName,
                                                           SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy,
                                                           int[] partitions, List<CacheMergeTypes>[] entries) {
        CacheConfig cacheConfig = cacheService.getCacheConfig(dataStructureName);
        CacheOperationProvider operationProvider
                = cacheService.getCacheOperationProvider(dataStructureName, cacheConfig.getInMemoryFormat());
        return operationProvider.createMergeOperationFactory(dataStructureName, partitions, entries, mergePolicy);
    }
}
