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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.replicatedmap.impl.operation.LegacyMergeOperation;
import com.hazelcast.replicatedmap.impl.operation.MergeOperationFactory;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.merge.AbstractMergeRunnable;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.util.function.BiConsumer;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

class ReplicatedMapMergeRunnable extends AbstractMergeRunnable<ReplicatedRecordStore, MergingEntry<Object, Object>> {

    private final ReplicatedMapSplitBrainHandlerService replicatedMapSplitBrainHandlerService;

    ReplicatedMapMergeRunnable(Map<String, Collection<ReplicatedRecordStore>> collectedStores,
                               Map<String, Collection<ReplicatedRecordStore>> collectedStoresWithLegacyPolicies,
                               Collection<ReplicatedRecordStore> backupStores,
                               NodeEngine nodeEngine,
                               ReplicatedMapSplitBrainHandlerService replicatedMapSplitBrainHandlerService) {
        super(SERVICE_NAME, collectedStores, collectedStoresWithLegacyPolicies, backupStores, nodeEngine);
        this.replicatedMapSplitBrainHandlerService = replicatedMapSplitBrainHandlerService;
    }

    @Override
    protected void consumeStore(ReplicatedRecordStore store, BiConsumer<Integer, MergingEntry<Object, Object>> consumer) {
        int partitionId = store.getPartitionId();

        Iterator<ReplicatedRecord> iterator = store.recordIterator();
        while (iterator.hasNext()) {
            ReplicatedRecord record = iterator.next();

            MergingEntry<Object, Object> mergingEntry = createMergingEntry(getSerializationService(), record);
            consumer.accept(partitionId, mergingEntry);
        }
    }

    @Override
    protected void consumeStoreLegacy(ReplicatedRecordStore store, BiConsumer<Integer, Operation> consumer) {
        int partitionId = store.getPartitionId();
        String name = store.getName();
        ReplicatedMapMergePolicy mergePolicy = ((ReplicatedMapMergePolicy) getMergePolicy(name));

        Iterator<ReplicatedRecord> iterator = store.recordIterator();
        while (iterator.hasNext()) {
            ReplicatedRecord record = iterator.next();

            ReplicatedMapEntryView entryView = createEntryView(record);
            LegacyMergeOperation operation = new LegacyMergeOperation(name, record.getKeyInternal(), entryView, mergePolicy);

            consumer.accept(partitionId, operation);
        }
    }

    private ReplicatedMapEntryView createEntryView(ReplicatedRecord record) {
        return new ReplicatedMapEntryView<Object, Object>()
                .setKey(getSerializationService().toObject(record.getKeyInternal()))
                .setValue(getSerializationService().toObject(record.getValueInternal()))
                .setHits(record.getHits())
                .setTtl(record.getTtlMillis())
                .setLastAccessTime(record.getLastAccessTime())
                .setCreationTime(record.getCreationTime())
                .setLastUpdateTime(record.getUpdateTime());
    }

    @Override
    protected int getBatchSize(String dataStructureName) {
        ReplicatedMapConfig replicatedMapConfig = replicatedMapSplitBrainHandlerService.getReplicatedMapConfig(dataStructureName);
        MergePolicyConfig mergePolicyConfig = replicatedMapConfig.getMergePolicyConfig();
        return mergePolicyConfig.getBatchSize();
    }

    @Override
    protected InMemoryFormat getInMemoryFormat(String dataStructureName) {
        ReplicatedMapConfig replicatedMapConfig = replicatedMapSplitBrainHandlerService.getReplicatedMapConfig(dataStructureName);
        return replicatedMapConfig.getInMemoryFormat();
    }

    @Override
    protected Object getMergePolicy(String dataStructureName) {
        return replicatedMapSplitBrainHandlerService.getMergePolicy(dataStructureName);
    }

    @Override
    protected void destroyStores(Collection<ReplicatedRecordStore> stores) {
        replicatedMapSplitBrainHandlerService.destroyStores(stores);
    }

    @Override
    protected OperationFactory createMergeOperationFactory(String dataStructureName,
                                                           SplitBrainMergePolicy mergePolicy,
                                                           int[] partitions,
                                                           List<MergingEntry<Object, Object>>[] entries) {
        return new MergeOperationFactory(dataStructureName, partitions, entries, mergePolicy);
    }
}
