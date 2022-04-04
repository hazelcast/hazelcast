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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.spi.impl.merge.AbstractMergeRunnable;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

class MapMergeRunnable extends AbstractMergeRunnable<Object, Object, RecordStore, MapMergeTypes<Object, Object>> {

    private final MapServiceContext mapServiceContext;

    MapMergeRunnable(Collection<RecordStore> mergingStores,
                     MapSplitBrainHandlerService splitBrainHandlerService,
                     MapServiceContext mapServiceContext) {
        super(MapService.SERVICE_NAME, mergingStores, splitBrainHandlerService, mapServiceContext.getNodeEngine());

        this.mapServiceContext = mapServiceContext;
    }

    @Override
    protected void mergeStore(RecordStore store, BiConsumer<Integer, MapMergeTypes<Object, Object>> consumer) {
        int partitionId = store.getPartitionId();

        store.forEach((BiConsumer<Data, Record>) (key, record) -> {
            Data dataKey = toHeapData(key);
            Data dataValue = toHeapData(record.getValue());
            ExpiryMetadata expiryMetadata = store.getExpirySystem().getExpiryMetadata(dataKey);
            consumer.accept(partitionId,
                    createMergingEntry(getSerializationService(), dataKey, dataValue,
                            record, expiryMetadata));
        }, false);
    }

    @Override
    protected int getBatchSize(String dataStructureName) {
        MapConfig mapConfig = getMapConfig(dataStructureName);
        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        return mergePolicyConfig.getBatchSize();
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
                                                           SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>,
                                                                   Object> mergePolicy,
                                                           int[] partitions, List<MapMergeTypes<Object, Object>>[] entries) {
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(dataStructureName);
        return operationProvider.createMergeOperationFactory(dataStructureName, partitions, entries, mergePolicy);
    }

    private MapConfig getMapConfig(String dataStructureName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(dataStructureName);
        return mapContainer.getMapConfig();
    }
}
