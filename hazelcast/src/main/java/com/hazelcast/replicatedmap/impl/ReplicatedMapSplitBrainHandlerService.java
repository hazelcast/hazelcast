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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.merge.MergePolicyProvider;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainHandlerService;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Contains split-brain handling logic for {@link com.hazelcast.core.ReplicatedMap}.
 */
class ReplicatedMapSplitBrainHandlerService extends AbstractSplitBrainHandlerService<ReplicatedRecordStore> {

    private final ReplicatedMapService service;
    private final MergePolicyProvider mergePolicyProvider;

    ReplicatedMapSplitBrainHandlerService(ReplicatedMapService service, MergePolicyProvider mergePolicyProvider) {
        super(service.getNodeEngine());
        this.service = service;
        this.mergePolicyProvider = mergePolicyProvider;
    }

    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return service.getReplicatedMapConfig(name);
    }

    public MergePolicyProvider getMergePolicyProvider() {
        return mergePolicyProvider;
    }

    @Override
    protected Runnable newMergeRunnable(Map<String, Collection<ReplicatedRecordStore>> collectedStores,
                                        Map<String, Collection<ReplicatedRecordStore>> collectedStoresWithLegacyPolicies,
                                        Collection<ReplicatedRecordStore> backupStores,
                                        NodeEngine nodeEngine) {
        return new ReplicatedMapMergeRunnable(collectedStores, collectedStoresWithLegacyPolicies, backupStores,
                service.getNodeEngine(), this);
    }

    @Override
    protected String getDataStructureName(ReplicatedRecordStore recordStore) {
        return recordStore.getName();
    }

    @Override
    protected Object getMergePolicy(String dataStructureName) {
        MergePolicyConfig mergePolicyConfig = getReplicatedMapConfig(dataStructureName).getMergePolicyConfig();
        return mergePolicyProvider.getMergePolicy(mergePolicyConfig.getPolicy());
    }

    @Override
    protected Collection<Iterator<ReplicatedRecordStore>> iteratorsOf(int partitionId) {
        PartitionContainer partitionContainer = service.getPartitionContainer(partitionId);
        if (partitionContainer == null) {
            return emptyList();
        }
        ConcurrentMap<String, ReplicatedRecordStore> stores = partitionContainer.getStores();
        return singletonList(stores.values().iterator());
    }

    @Override
    protected void destroyStore(ReplicatedRecordStore store) {
        store.destroy();
    }
}
