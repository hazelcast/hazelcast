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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockService;
import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.monitor.LocalRecordStoreStats;
import com.hazelcast.monitor.impl.LocalRecordStoreStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTimes;

/**
 * Contains record store common parts.
 */
abstract class AbstractRecordStore implements RecordStore<Record> {

    protected final int partitionId;
    protected final String name;
    protected final LockStore lockStore;
    protected final MapContainer mapContainer;
    protected final RecordFactory recordFactory;
    protected final InMemoryFormat inMemoryFormat;
    protected final MapStoreContext mapStoreContext;
    protected final ValueComparator valueComparator;
    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;
    protected final MapDataStore<Data, Object> mapDataStore;
    protected final LocalRecordStoreStatsImpl stats = new LocalRecordStoreStatsImpl();
    protected final RecordStoreMutationObserver<Record> mutationObserver;

    protected Storage<Data, Record> storage;
    private final StoreAdapter storeAdapter;

    protected AbstractRecordStore(MapContainer mapContainer, int partitionId) {
        this.name = mapContainer.getName();
        this.mapContainer = mapContainer;
        this.partitionId = partitionId;
        this.mapServiceContext = mapContainer.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.valueComparator = mapServiceContext.getValueComparatorOf(inMemoryFormat);
        this.mapStoreContext = mapContainer.getMapStoreContext();
        this.mapDataStore = mapStoreContext.getMapStoreManager().getMapDataStore(name, partitionId);
        this.lockStore = createLockStore();
        Collection<RecordStoreMutationObserver<Record>> mutationObservers = mapServiceContext
                .createRecordStoreMutationObservers(getName(), partitionId);
        this.mutationObserver = new CompositeRecordStoreMutationObserver<>(mutationObservers);
        this.storeAdapter = new RecordStoreAdapter(this);
    }

    protected boolean persistenceEnabledFor(@Nonnull CallerProvenance provenance) {
        switch (provenance) {
            case WAN:
                return mapContainer.isPersistWanReplicatedData();
            case NOT_WAN:
                return true;
            default:
                throw new IllegalArgumentException("Unexpected provenance: `" + provenance + "`");
        }
    }

    @Override
    public LocalRecordStoreStats getLocalRecordStoreStats() {
        return stats;
    }

    @Override
    public void init() {
        this.storage = createStorage(recordFactory, inMemoryFormat);
    }

    @Override
    public Record createRecord(Data key, Object value, long ttlMillis, long maxIdle, long now) {
        Record record = recordFactory.newRecord(key, value);
        record.setCreationTime(now);
        record.setLastUpdateTime(now);

        setExpirationTimes(ttlMillis, maxIdle, record, mapContainer.getMapConfig(), true);
        updateStatsOnPut(false, now);
        return record;
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new StorageImpl(recordFactory, memoryFormat, serializationService);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MapContainer getMapContainer() {
        return mapContainer;
    }

    @Override
    public long getOwnedEntryCost() {
        return storage.getEntryCostEstimator().getEstimate();
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected void updateRecord(Data key, Record record, Object value,
                                long now, boolean countAsAccess, long ttl, long maxIdle, boolean mapStoreOperation) {
        updateStatsOnPut(countAsAccess, now);
        record.onUpdate(now);
        if (countAsAccess) {
            record.onAccess(now);
        }
        setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), true);
        if (mapStoreOperation) {
            value = runMapStore(record, key, value, now);
        }
        mutationObserver.onUpdateRecord(key, record, value);
        storage.updateRecordValue(key, record, value);
    }

    protected Record putNewRecord(Data key, Object value, long ttlMillis, long maxIdleMillis, long now) {
        Record record = createRecord(key, value, ttlMillis, maxIdleMillis, now);
        runMapStore(record, key, value, now);
        storage.put(key, record);
        mutationObserver.onPutRecord(key, record);
        return record;
    }

    protected Object runMapStore(Record record, Data key, Object value, long now) {
        long expirationTime = record.getExpirationTime();
        value = mapDataStore.add(key, value, expirationTime, now);
        if (mapDataStore.isPostProcessingMapStore()) {
            recordFactory.setValue(record, value);
        }
        return value;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    protected void saveIndex(Record record, Object oldValue) {
        Data dataKey = record.getKey();
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.haveAtLeastOneIndex()) {
            Object value = Records.getValueOrCachedValue(record, serializationService);
            QueryableEntry queryableEntry = mapContainer.newQueryEntry(dataKey, value);
            queryableEntry.setRecord(record);
            queryableEntry.setStoreAdapter(storeAdapter);
            indexes.putEntry(queryableEntry, oldValue, Index.OperationSource.USER);
        }
    }

    protected void removeIndex(Record record) {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.haveAtLeastOneIndex()) {
            Data key = record.getKey();
            Object value = Records.getValueOrCachedValue(record, serializationService);
            indexes.removeEntry(key, value, Index.OperationSource.USER);
        }
    }

    protected void removeIndex(Collection<Record> records) {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (!indexes.haveAtLeastOneIndex()) {
            return;
        }

        for (Record record : records) {
            removeIndex(record);
        }
    }

    protected LockStore createLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        LockService lockService = nodeEngine.getServiceOrNull(LockService.SERVICE_NAME);
        if (lockService == null) {
            return null;
        }
        return lockService.createLockStore(partitionId, MapService.getObjectNamespace(name));
    }

    public int getLockedEntryCount() {
        return lockStore.getLockedEntryCount();
    }

    protected RecordStoreLoader createRecordStoreLoader(MapStoreContext mapStoreContext) {
        MapStoreWrapper wrapper = mapStoreContext.getMapStoreWrapper();
        if (wrapper == null) {
            return RecordStoreLoader.EMPTY_LOADER;
        } else if (wrapper.isWithExpirationTime()) {
            return new EntryRecordStoreLoader(this);
        } else {
            return new BasicRecordStoreLoader(this);
        }
    }

    protected Data toData(Object value) {
        return mapServiceContext.toData(value);
    }

    public void setSizeEstimator(EntryCostEstimator entryCostEstimator) {
        this.storage.setEntryCostEstimator(entryCostEstimator);
    }

    @Override
    public void disposeDeferredBlocks() {
        storage.disposeDeferredBlocks();
    }

    public Storage<Data, ? extends Record> getStorage() {
        return storage;
    }

    protected void updateStatsOnPut(boolean countAsAccess, long now) {
        stats.setLastUpdateTime(now);

        if (countAsAccess) {
            updateStatsOnGet(now);
        }
    }

    protected void updateStatsOnPut(long hits) {
        stats.increaseHits(hits);
    }

    protected void updateStatsOnGet(long now) {
        stats.setLastAccessTime(now);
        stats.increaseHits();
    }

}
