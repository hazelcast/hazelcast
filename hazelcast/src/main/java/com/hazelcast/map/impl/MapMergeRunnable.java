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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.merge.AbstractMergeRunnable;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.util.Clock;
import com.hazelcast.util.function.BiConsumer;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

class MapMergeRunnable extends AbstractMergeRunnable<RecordStore, MergingEntry<Data, Data>> {

    private final MapServiceContext mapServiceContext;
    private final MapSplitBrainHandlerService mapSplitBrainHandlerService;

    MapMergeRunnable(Map<String, Collection<RecordStore>> collectedStores,
                     Map<String, Collection<RecordStore>> collectedStoresWithLegacyPolicies,
                     Collection<RecordStore> backupStores, MapServiceContext mapServiceContext,
                     MapSplitBrainHandlerService mapSplitBrainHandlerService) {
        super(MapService.SERVICE_NAME, collectedStores, collectedStoresWithLegacyPolicies,
                backupStores, mapServiceContext.getNodeEngine());

        this.mapServiceContext = mapServiceContext;
        this.mapSplitBrainHandlerService = mapSplitBrainHandlerService;
    }

    @Override
    protected void consumeStore(RecordStore store, BiConsumer<Integer, MergingEntry<Data, Data>> consumer) {
        long now = Clock.currentTimeMillis();
        int partitionId = store.getPartitionId();

        Iterator<Record> iterator = store.iterator(now, false);
        while (iterator.hasNext()) {
            Record record = iterator.next();

            Data dataValue = toData(record.getValue());
            MergingEntry<Data, Data> mergingEntry = createMergingEntry(getSerializationService(), record, dataValue);
            consumer.accept(partitionId, mergingEntry);
        }
    }

    @Override
    protected void consumeStoreLegacy(RecordStore store, BiConsumer<Integer, Operation> consumer) {
        long now = Clock.currentTimeMillis();
        int partitionId = store.getPartitionId();
        String name = store.getName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(name);
        MapMergePolicy mergePolicy = ((MapMergePolicy) getMergePolicy(name));

        Iterator<Record> iterator = store.iterator(now, false);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            Data value = toData(record.getValue());
            EntryView<Data, Data> entryView = createSimpleEntryView(key, value, record);

            Operation operation = operationProvider.createLegacyMergeOperation(name, entryView, mergePolicy, false);

            consumer.accept(partitionId, operation);
        }
    }

    @Override
    protected int getBatchSize(String dataStructureName) {
        MapConfig mapConfig = mapSplitBrainHandlerService.getMapConfig(dataStructureName);
        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        return mergePolicyConfig.getBatchSize();
    }

    @Override
    protected InMemoryFormat getInMemoryFormat(String dataStructureName) {
        MapConfig mapConfig = mapSplitBrainHandlerService.getMapConfig(dataStructureName);
        return mapConfig.getInMemoryFormat();
    }

    @Override
    protected Object getMergePolicy(String dataStructureName) {
        return mapSplitBrainHandlerService.getMergePolicy(dataStructureName);
    }

    @Override
    protected void destroyStores(Collection<RecordStore> stores) {
        mapSplitBrainHandlerService.destroyStores(stores);
    }

    @Override
    protected OperationFactory createMergeOperationFactory(String dataStructureName,
                                                           SplitBrainMergePolicy mergePolicy,
                                                           int[] partitions,
                                                           List<MergingEntry<Data, Data>>[] entries) {
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(dataStructureName);
        return operationProvider.createMergeOperationFactory(dataStructureName, partitions, entries, mergePolicy);
    }
}
