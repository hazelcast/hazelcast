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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainHandlerService;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Collections.singletonList;

class MapSplitBrainHandlerService extends AbstractSplitBrainHandlerService<RecordStore> {

    private final MapServiceContext mapServiceContext;
    private final MergePolicyProvider mergePolicyProvider;

    MapSplitBrainHandlerService(MapServiceContext mapServiceContext) {
        super(mapServiceContext.getNodeEngine());
        this.mapServiceContext = mapServiceContext;
        this.mergePolicyProvider = mapServiceContext.getMergePolicyProvider();
    }

    @Override
    protected Runnable newMergeRunnable(Map<String, Collection<RecordStore>> collectedStores,
                                        Map<String, Collection<RecordStore>> collectedStoresWithLegacyPolicies,
                                        Collection<RecordStore> backupStores,
                                        NodeEngine nodeEngine) {
        return new MapMergeRunnable(collectedStores, collectedStoresWithLegacyPolicies,
                backupStores, mapServiceContext, this);
    }

    @Override
    protected String getDataStructureName(RecordStore recordStore) {
        return recordStore.getName();
    }

    @Override
    protected Object getMergePolicy(String dataStructureName) {
        MapConfig mapConfig = getMapConfig(dataStructureName);
        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        return mergePolicyProvider.getMergePolicy(mergePolicyConfig.getPolicy());
    }

    @Override
    protected boolean isDiscardPolicy(Object mergePolicy) {
        return mergePolicy instanceof IgnoreMergingEntryMapMergePolicy
                || super.isDiscardPolicy(mergePolicy);
    }

    @Override
    protected Collection<Iterator<RecordStore>> iteratorsOf(int partitionId) {
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        Collection<RecordStore> recordStores = partitionContainer.getAllRecordStores();
        return singletonList(recordStores.iterator());
    }

    @Override
    protected void destroyStore(RecordStore store) {
        assert store.getMapContainer().getMapConfig().getInMemoryFormat() != NATIVE;

        store.getMapContainer().getIndexes(store.getPartitionId()).clearIndexes();
        store.destroy();
    }

    public MapConfig getMapConfig(String dataStructureName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(dataStructureName);
        return mapContainer.getMapConfig();
    }
}
