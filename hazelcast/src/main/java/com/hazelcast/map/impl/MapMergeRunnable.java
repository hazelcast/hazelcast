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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.merge.AbstractMergeRunnable;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.internal.util.Clock;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

class MapMergeRunnable extends AbstractMergeRunnable<Data, Data, RecordStore, MapMergeTypes> {

    private final MapServiceContext mapServiceContext;

    MapMergeRunnable(Collection<RecordStore> mergingStores,
                     MapSplitBrainHandlerService splitBrainHandlerService,
                     MapServiceContext mapServiceContext) {
        super(MapService.SERVICE_NAME, mergingStores, splitBrainHandlerService, mapServiceContext.getNodeEngine());

        this.mapServiceContext = mapServiceContext;
    }

    @Override
    protected void mergeStore(RecordStore store, BiConsumer<Integer, MapMergeTypes> consumer) {
        long now = Clock.currentTimeMillis();
        int partitionId = store.getPartitionId();

        //noinspection unchecked
        Iterator<Record> iterator = store.iterator(now, false);
        while (iterator.hasNext()) {
            Record record = iterator.next();

            Data dataKey = toHeapData(record.getKey());
            Data dataValue = toHeapData(record.getValue());

            consumer.accept(partitionId, createMergingEntry(getSerializationService(), dataKey, dataValue, record));
        }
    }

    @Override
    protected int getBatchSize(String dataStructureName) {
        MapConfig mapConfig = getMapConfig(dataStructureName);
        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        return mergePolicyConfig.getBatchSize();
    }

    @Override
    protected InMemoryFormat getInMemoryFormat(String dataStructureName) {
        MapConfig mapConfig = getMapConfig(dataStructureName);
        return mapConfig.getInMemoryFormat();
    }

    @Override
    protected SplitBrainMergePolicy getMergePolicy(String dataStructureName) {
        MapConfig mapConfig = getMapConfig(dataStructureName);
        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        return mergePolicyProvider.getMergePolicy(mergePolicyConfig.getPolicy());
    }

    @Override
    protected String getDataStructureName(RecordStore recordStore) {
        return recordStore.getName();
    }

    @Override
    protected int getPartitionId(RecordStore store) {
        return store.getPartitionId();
    }

    @Override
    protected OperationFactory createMergeOperationFactory(String dataStructureName,
                                                           SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy,
                                                           int[] partitions, List<MapMergeTypes>[] entries) {
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(dataStructureName);
        return operationProvider.createMergeOperationFactory(dataStructureName, partitions, entries, mergePolicy);
    }

    private MapConfig getMapConfig(String dataStructureName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(dataStructureName);
        return mapContainer.getMapConfig();
    }
}
