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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainHandlerService;
import com.hazelcast.spi.merge.DiscardMergePolicy;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Contains split-brain handling logic for {@link ReplicatedMap}.
 */
class ReplicatedMapSplitBrainHandlerService extends AbstractSplitBrainHandlerService<ReplicatedRecordStore> {

    private final ReplicatedMapService service;

    ReplicatedMapSplitBrainHandlerService(ReplicatedMapService service) {
        super(service.getNodeEngine());
        this.service = service;
    }

    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return service.getReplicatedMapConfig(name);
    }

    @Override
    protected Runnable newMergeRunnable(Collection<ReplicatedRecordStore> mergingStores) {
        return new ReplicatedMapMergeRunnable(mergingStores, this, service.getNodeEngine());
    }

    @Override
    protected Iterator<ReplicatedRecordStore> storeIterator(int partitionId) {
        PartitionContainer partitionContainer = service.getPartitionContainer(partitionId);
        if (partitionContainer == null) {
            return Collections.emptyIterator();
        }
        ConcurrentMap<String, ReplicatedRecordStore> stores = partitionContainer.getStores();
        return stores.values().iterator();
    }

    @Override
    protected void destroyStore(ReplicatedRecordStore replicatedRecordStore) {
        assertRunningOnPartitionThread();

        replicatedRecordStore.destroy();
    }

    @Override
    protected boolean hasEntries(ReplicatedRecordStore store) {
        assertRunningOnPartitionThread();

        return !store.isEmpty();
    }

    @Override
    protected boolean hasMergeablePolicy(ReplicatedRecordStore store) {
        Object mergePolicy = service.getMergePolicy(store.getName());
        return !(mergePolicy instanceof DiscardMergePolicy);
    }
}
