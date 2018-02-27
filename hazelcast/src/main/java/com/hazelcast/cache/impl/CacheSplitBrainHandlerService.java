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

import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractSplitBrainHandlerService;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Collections.singletonList;

/**
 * Handles split-brain functionality for cache.
 */
class CacheSplitBrainHandlerService extends AbstractSplitBrainHandlerService<ICacheRecordStore> {

    private final CacheService cacheService;
    private final CachePartitionSegment[] segments;
    private final Map<String, CacheConfig> configs;
    private final CacheMergePolicyProvider mergePolicyProvider;

    CacheSplitBrainHandlerService(NodeEngine nodeEngine,
                                  Map<String, CacheConfig> configs,
                                  CachePartitionSegment[] segments) {
        super(nodeEngine);
        this.configs = configs;
        this.segments = segments;
        this.mergePolicyProvider = new CacheMergePolicyProvider(nodeEngine);
        this.cacheService = nodeEngine.getService(SERVICE_NAME);
    }

    @Override
    protected Runnable newMergeRunnable(Map<String, Collection<ICacheRecordStore>> collectedStores,
                                        Map<String, Collection<ICacheRecordStore>> collectedStoresWithLegacyPolicies,
                                        Collection<ICacheRecordStore> backupStores,
                                        NodeEngine nodeEngine) {
        return new CacheMergeRunnable(collectedStores, collectedStoresWithLegacyPolicies,
                backupStores, this, nodeEngine);
    }

    @Override
    public String getDataStructureName(ICacheRecordStore recordStore) {
        return recordStore.getName();
    }

    @Override
    protected Object getMergePolicy(String dataStructureName) {
        CacheConfig cacheConfig = configs.get(dataStructureName);
        String mergePolicyName = cacheConfig.getMergePolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    @Override
    protected void onPrepareMergeRunnableEnd(Collection<String> dataStructureNames) {
        for (String cacheName : dataStructureNames) {
            cacheService.sendInvalidationEvent(cacheName, null, SOURCE_NOT_AVAILABLE);
        }
    }

    @Override
    protected Collection<Iterator<ICacheRecordStore>> iteratorsOf(int partitionId) {
        return singletonList(segments[partitionId].recordStoreIterator());
    }

    @Override
    protected void destroyStore(ICacheRecordStore store) {
        assert store.getConfig().getInMemoryFormat() != NATIVE;

        store.destroy();
    }

    public Map<String, CacheConfig> getConfigs() {
        return configs;
    }
}
