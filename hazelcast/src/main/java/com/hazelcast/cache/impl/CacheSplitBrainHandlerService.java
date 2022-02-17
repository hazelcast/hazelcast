/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainHandlerService;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Handles split-brain functionality for cache.
 */
class CacheSplitBrainHandlerService extends AbstractSplitBrainHandlerService<ICacheRecordStore> {

    private final CacheService cacheService;
    private final CachePartitionSegment[] segments;

    CacheSplitBrainHandlerService(NodeEngine nodeEngine, CachePartitionSegment[] segments) {
        super(nodeEngine);
        this.segments = segments;
        this.cacheService = nodeEngine.getService(SERVICE_NAME);
    }

    @Override
    protected Runnable newMergeRunnable(Collection<ICacheRecordStore> mergingStores) {
        return new CacheMergeRunnable(mergingStores, this, cacheService.nodeEngine);
    }

    @Override
    protected Iterator<ICacheRecordStore> storeIterator(int partitionId) {
        return segments[partitionId].recordStoreIterator();
    }

    @Override
    protected void destroyStore(ICacheRecordStore store) {
        assertRunningOnPartitionThread();

        store.destroyInternals();
    }

    @Override
    protected boolean hasEntries(ICacheRecordStore store) {
        assertRunningOnPartitionThread();

        return store.size() > 0;
    }

    @Override
    protected boolean hasMergeablePolicy(ICacheRecordStore store) {
        SplitBrainMergePolicy mergePolicy = cacheService.getMergePolicy(store.getName());
        return !(mergePolicy instanceof DiscardMergePolicy);
    }
}
