/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.internal.locksupport.LockStore;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.monitor.LocalRecordStoreStats;
import com.hazelcast.internal.monitor.impl.LocalRecordStoreStatsImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.JsonMetadataInitializer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;
import java.util.UUID;

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
    protected final MapDataStore<Data, Object> mapDataStore;
    protected final SerializationService serializationService;
    protected final CompositeMutationObserver<Record> mutationObserver;
    protected final LocalRecordStoreStatsImpl stats = new LocalRecordStoreStatsImpl();

    protected Storage<Data, Record> storage;
    protected IndexingMutationObserver<Record> indexingObserver;

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
        this.mutationObserver = new CompositeMutationObserver<>();
    }

    @Override
    public void init() {
        this.storage = createStorage(recordFactory, inMemoryFormat);
        addMutationObservers();
    }

    // Overridden in EE.
    protected void addMutationObservers() {
        // Add observer for event journal
        EventJournalConfig eventJournalConfig = mapContainer.getEventJournalConfig();
        if (eventJournalConfig != null && eventJournalConfig.isEnabled()) {
            mutationObserver.add(new EventJournalWriterMutationObserver(mapServiceContext.getEventJournal(),
                    mapContainer, partitionId));
        }

        // Add observer for json metadata
        if (mapContainer.getMapConfig().getMetadataPolicy() == MetadataPolicy.CREATE_ON_UPDATE) {
            addJsonMetadataMutationObserver();
        }

        // Add observer for indexing
        indexingObserver = new IndexingMutationObserver<>(this, serializationService);
        mutationObserver.add(indexingObserver);
    }

    // Overridden in EE.
    protected void addJsonMetadataMutationObserver() {
        mutationObserver.add(new JsonMetadataMutationObserver(serializationService,
                JsonMetadataInitializer.INSTANCE));
    }

    public IndexingMutationObserver<Record> getIndexingObserver() {
        return indexingObserver;
    }

    @Override
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    @Override
    public EvictionPolicy getEvictionPolicy() {
        return getMapContainer().getMapConfig().getEvictionConfig().getEvictionPolicy();
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
    public Record createRecord(Data key, Object value, long ttlMillis, long maxIdle, long now) {
        Record record = recordFactory.newRecord(value);
        record.setCreationTime(now);
        record.setLastUpdateTime(now);

        setExpirationTimes(ttlMillis, maxIdle, record, mapContainer.getMapConfig(), true);
        updateStatsOnPut(false, now);
        return record;
    }

    @Override
    public Record createRecord(Data key, Record fromRecord, long nowInMillis) {
        Record newRecord = recordFactory.newRecord(fromRecord == null ? null : fromRecord.getValue());
        if (fromRecord != null) {
            Records.copyMetadataFrom(fromRecord, newRecord);
        }
        updateStatsOnPut(false, nowInMillis);
        return newRecord;
    }

    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new StorageImpl(memoryFormat, serializationService);
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

    protected static long getNow() {
        return Clock.currentTimeMillis();
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected void updateRecord(Data key, Record record, Object oldValue, Object newValue,
                                long now, boolean countAsAccess,
                                long ttl, long maxIdle, boolean mapStoreOperation,
                                UUID transactionId, boolean backup) {
        updateStatsOnPut(countAsAccess, now);
        record.onUpdate(now);
        if (countAsAccess) {
            record.onAccess(now);
        }
        setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), true);
        if (mapStoreOperation) {
            newValue = putIntoMapStore(record, key, newValue, now, transactionId);
        }
        storage.updateRecordValue(key, record, newValue);
        mutationObserver.onUpdateRecord(key, record, oldValue, newValue, backup);
    }

    protected Record putNewRecord(Data key, Object oldValue, Object newValue, long ttlMillis,
                                  long maxIdleMillis, long now, UUID transactionId) {
        Record record = createRecord(key, newValue, ttlMillis, maxIdleMillis, now);
        putIntoMapStore(record, key, newValue, now, transactionId);
        storage.put(key, record);
        mutationObserver.onPutRecord(key, record, oldValue, false);
        return record;
    }

    protected Object putIntoMapStore(Record record, Data key, Object newValue, long now, UUID transactionId) {
        newValue = mapDataStore.add(key, newValue, record.getExpirationTime(), now, transactionId);
        if (mapDataStore.isPostProcessingMapStore()) {
            storage.updateRecordValue(key, record, newValue);
        }
        return newValue;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    protected LockStore createLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        LockSupportService lockService = nodeEngine.getServiceOrNull(LockSupportService.SERVICE_NAME);
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

    protected void updateStatsOnPut(long hits, long now) {
        stats.setLastUpdateTime(now);
        stats.increaseHits(hits);
    }

    protected void updateStatsOnGet(long now) {
        stats.setLastAccessTime(now);
        stats.increaseHits();
    }
}
