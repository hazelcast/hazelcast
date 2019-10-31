/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainHandlerService;
import com.hazelcast.spi.merge.DiscardMergePolicy;

import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

class MapSplitBrainHandlerService extends AbstractSplitBrainHandlerService<RecordStore> {

    private final MapServiceContext mapServiceContext;

    MapSplitBrainHandlerService(MapServiceContext mapServiceContext) {
        super(mapServiceContext.getNodeEngine());
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    protected Runnable newMergeRunnable(Collection<RecordStore> mergingStores) {
        return new MapMergeRunnable(mergingStores, this, mapServiceContext);
    }

    @Override
    protected Iterator<RecordStore> storeIterator(int partitionId) {
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        Collection<RecordStore> recordStores = partitionContainer.getAllRecordStores();
        return recordStores.iterator();
    }

    /**
     * Clears indexes inside partition thread while collecting merge
     * tasks. Otherwise, if we do this cleanup upon join of merging node,
     * concurrently running merge and migration operations can cause
     * inconsistency over shared index objects between record stores.
     */
    @Override
    protected void onStoreCollection(RecordStore recordStore) {
        assertRunningOnPartitionThread();

        DefaultRecordStore defaultRecordStore = (DefaultRecordStore) recordStore;
        defaultRecordStore.clearOtherDataThanStorage(false, true);
        defaultRecordStore.getIndexingObserver().onDestroy(false, true);
    }

    @Override
    protected void destroyStore(RecordStore store) {
        assertRunningOnPartitionThread();

        ((DefaultRecordStore) store).destroyStorageAfterClear(false, true);
    }

    @Override
    protected boolean hasEntries(RecordStore store) {
        assertRunningOnPartitionThread();

        return !store.isEmpty();
    }

    @Override
    protected boolean hasMergeablePolicy(RecordStore store) {
        String policy = store.getMapContainer().getMapConfig().getMergePolicyConfig().getPolicy();
        Object mergePolicy = mapServiceContext.getNodeEngine().getSplitBrainMergePolicyProvider().getMergePolicy(policy);
        return !(mergePolicy instanceof DiscardMergePolicy);
    }
}
