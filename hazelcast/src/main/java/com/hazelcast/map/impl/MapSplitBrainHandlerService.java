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

package com.hazelcast.map.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainHandlerService;
import com.hazelcast.spi.merge.DiscardMergePolicy;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

class MapSplitBrainHandlerService extends AbstractSplitBrainHandlerService<RecordStore> {

    private final MapServiceContext mapServiceContext;
    private final ILogger logger;

    MapSplitBrainHandlerService(MapServiceContext mapServiceContext) {
        super(mapServiceContext.getNodeEngine());
        this.mapServiceContext = mapServiceContext;
        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
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
        defaultRecordStore.getMapDataStore().reset();
        defaultRecordStore.getIndexingObserver().onDestroy(false, true);

        // Removal of old mapContainer is required not to leak old
        // state into merged cluster. An example old state that we
        // don't want to leak into merged cluster is index state. In
        // merged cluster, there will be a fresh mapContainer object.
        PartitionContainer partitionContainer
                = mapServiceContext.getPartitionContainer(recordStore.getPartitionId());
        MapContainer mapContainer = recordStore.getMapContainer();

        // `onStoreCollection` method is called after collection
        // of each record-store. Since same map-container is shared
        // between all record-stores of a map, to destroy it only
        // one time, we use `destroyMapContainer` method. It only
        // destroys map-container once and other calls have no
        // effect.
        //
        // This one-time logic also helps us to add shared
        // global indexes to new-map-container only once.
        if (partitionContainer.destroyMapContainer(mapContainer)) {
            if (mapContainer.shouldUseGlobalIndex()) {
                // remove global indexes from old-map-container and add them to new one
                addIndexConfigToNewMapContainer(mapContainer.name, -1,
                        mapContainer.getIndexes());
            }
        }

        // remove partitioned indexes from old-map-container and add them to new one
        if (!mapContainer.shouldUseGlobalIndex()) {
            Indexes indexes = partitionContainer.getIndexes().remove(mapContainer.name);
            addIndexConfigToNewMapContainer(mapContainer.name, recordStore.getPartitionId(),
                    indexes);
        }
    }

    private void addIndexConfigToNewMapContainer(String mapName, int partitionId,
                                                 Indexes indexes) {
        if (indexes == null) {
            return;
        }

        LinkedList<IndexConfig> indexConfigs = new LinkedList<>();
        InternalIndex[] internalIndexes = indexes.getIndexes();
        for (int i = 0; i < internalIndexes.length; i++) {
            indexConfigs.add(internalIndexes[i].getConfig());
        }

        // create new map-container here.
        MapContainer newMapContainer = mapServiceContext.getMapContainer(mapName);
        for (IndexConfig indexConfig : indexConfigs) {
            newMapContainer.getIndexes(partitionId).addOrGetIndex(indexConfig);
        }
    }

    @Override
    protected void destroyStore(RecordStore store) {
        assertRunningOnPartitionThread();

        if (logger.isFineEnabled()) {
            logger.fine(String.format("Destroyed store [mapName:%s, partitionId:%d, partitionSize:%d]",
                    store.getName(), store.getPartitionId(), store.size()));
        }

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
