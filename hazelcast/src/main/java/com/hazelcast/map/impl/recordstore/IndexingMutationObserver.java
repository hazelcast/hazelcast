/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;

import javax.annotation.Nonnull;
import java.util.function.BiConsumer;

import static com.hazelcast.map.impl.record.Records.getValueOrCachedValue;

public class IndexingMutationObserver<R extends Record> implements MutationObserver<R> {

    private final int partitionId;
    private final MapContainer mapContainer;
    private final SerializationService ss;
    private final RecordStore recordStore;

    public IndexingMutationObserver(RecordStore recordStore, SerializationService ss) {
        this.partitionId = recordStore.getPartitionId();
        this.mapContainer = recordStore.getMapContainer();
        this.recordStore = recordStore;
        this.ss = ss;
    }

    @Override
    public void onPutRecord(@Nonnull Data key, @Nonnull R record,
                            Object oldValue, boolean backup) {
        if (!backup) {
            saveIndex(key, record, oldValue, Index.OperationSource.USER);
        }
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull R record, boolean populateIndex) {
        if (populateIndex) {
            saveIndex(key, record, null, Index.OperationSource.SYSTEM);
        }
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull R record,
                               Object oldValue, Object newValue, boolean backup) {
        if (!backup) {
            saveIndex(key, record, oldValue, Index.OperationSource.USER);
        }
    }

    @Override
    public void onRemoveRecord(@Nonnull Data key, R record, boolean backup) {
        if (!backup) {
            removeIndex(key, record, Index.OperationSource.USER);
        }
    }

    @Override
    public void onEvictRecord(@Nonnull Data key, @Nonnull R record, boolean backup) {
        if (!backup) {
            removeIndex(key, record, Index.OperationSource.USER);
        }
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull R record, boolean backup) {
        if (!backup) {
            saveIndex(key, record, null, Index.OperationSource.USER);
        }
    }

    @Override
    public void onReset() {
        clearGlobalIndexes();
        // Partitioned indexes are cleared in MapReplicationStateHolder
    }

    @Override
    public void onClear() {
        onReset();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        // global indexes are destroyed on map-container
        // destroy(see MapServiceContextImpl#destroyMap)
        clearGlobalIndexes();
        clearPartitionedIndexes(true);
    }

    /**
     * Only indexed data will be removed, index info will stay.
     */
    private void clearGlobalIndexes() {
        if (!mapContainer.shouldUseGlobalIndex()) {
            return;
        }
        IndexRegistry indexRegistry = mapContainer.getGlobalIndexRegistry();
        if (indexRegistry.haveAtLeastOneIndex()) {
            // clears indexed data of this partition
            // from shared global index.
            fullScanLocalDataToClear(indexRegistry);
        }
    }

    /**
     * Only indexed data will be removed, index info will stay.
     */
    private void clearPartitionedIndexes(boolean destroy) {
        IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(partitionId);
        if (indexRegistry.isGlobal()) {
            return;
        }

        if (destroy) {
            indexRegistry.destroyIndexes();
            return;
        }

        indexRegistry.clearAll();
    }

    /**
     * Clears local data of this partition from global index by doing
     * partition full-scan.
     */
    private void fullScanLocalDataToClear(IndexRegistry indexRegistry) {
        InternalIndex[] indexesSnapshot = indexRegistry.getIndexes();

        IndexRegistry.beginPartitionUpdate(indexesSnapshot);

        CachedQueryEntry<?, ?> entry = new CachedQueryEntry<>(ss, mapContainer.getExtractors());
        recordStore.forEach((BiConsumer<Data, Record>) (dataKey, record) -> {
            Object value = getValueOrCachedValue(record, ss);
            entry.init(dataKey, value);
            indexRegistry.removeEntry(entry, Index.OperationSource.SYSTEM);
        }, false);

        IndexRegistry.markPartitionAsUnindexed(partitionId, indexesSnapshot);
    }

    private void saveIndex(Data dataKey, Record record, Object oldValue,
                           Index.OperationSource operationSource) {
        IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(partitionId);
        if (!indexRegistry.haveAtLeastOneIndex()) {
            return;
        }

        QueryableEntry queryableEntry = mapContainer.newQueryEntry(toBackingKeyFormat(dataKey),
                getValueOrCachedValue(record, ss));
        queryableEntry.setRecord(record);

        indexRegistry.putEntry(queryableEntry, oldValue, operationSource);
    }

    private void removeIndex(Data dataKey, Record record,
                             Index.OperationSource operationSource) {
        IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(partitionId);
        if (!indexRegistry.haveAtLeastOneIndex()) {
            return;
        }

        indexRegistry.removeEntry(toBackingKeyFormat(dataKey), getValueOrCachedValue(record, ss), operationSource);
    }

    private Data toBackingKeyFormat(Data key) {
        return recordStore.getStorage().toBackingDataKeyFormat(key);
    }
}
