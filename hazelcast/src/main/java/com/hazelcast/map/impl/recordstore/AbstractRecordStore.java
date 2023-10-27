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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
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
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.ObjectRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.record.RecordReaderWriter;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;

/**
 * Contains record store common parts.
 */
abstract class AbstractRecordStore implements RecordStore<Record> {

    protected final int partitionId;
    protected final String name;
    protected final LockStore lockStore;
    protected final MapContainer mapContainer;
    protected final InMemoryFormat inMemoryFormat;
    protected final MapStoreContext mapStoreContext;
    protected final ValueComparator valueComparator;
    protected final MapServiceContext mapServiceContext;
    protected final MapDataStore<Data, Object> mapDataStore;
    protected final SerializationService serializationService;
    protected final CompositeMutationObserver<Record> mutationObserver;
    protected final LocalRecordStoreStatsImpl stats = new LocalRecordStoreStatsImpl();

    protected RecordFactory recordFactory;
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
        this.valueComparator = mapServiceContext.getValueComparatorOf(inMemoryFormat);
        this.mapStoreContext = mapContainer.getMapStoreContext();
        this.mapDataStore = mapStoreContext.getMapStoreManager().getMapDataStore(name, partitionId);
        this.lockStore = createLockStore();
        this.mutationObserver = new CompositeMutationObserver<>();
    }

    // overridden in different context
    RecordFactory createRecordFactory() {
        MapConfig mapConfig = mapContainer.getMapConfig();
        switch (mapConfig.getInMemoryFormat()) {
            case BINARY:
                return new DataRecordFactory(mapContainer, serializationService);
            case OBJECT:
                return new ObjectRecordFactory(mapContainer, serializationService);
            default:
                throw new IllegalArgumentException("Invalid storage format: " + mapConfig.getInMemoryFormat());
        }
    }

    @Override
    public void init() {
        this.recordFactory = createRecordFactory();
        this.storage = createStorage(recordFactory, inMemoryFormat);
        addMutationObservers();
    }

    public ValueComparator getValueComparator() {
        return valueComparator;
    }

    public LockStore getLockStore() {
        return lockStore;
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
            mutationObserver.add(new JsonMetadataMutationObserver(serializationService,
                    JsonMetadataInitializer.INSTANCE, getOrCreateMetadataStore()));
        }

        // Add observer for indexing
        indexingObserver = new IndexingMutationObserver<>(this, serializationService);
        mutationObserver.add(indexingObserver);
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

    public boolean persistenceEnabledFor(@Nonnull CallerProvenance provenance) {
        switch (provenance) {
            case WAN:
                return mapContainer.getWanContext().isPersistWanReplicatedData();
            case NOT_WAN:
                return true;
            default:
                throw new IllegalArgumentException("Unexpected provenance: `" + provenance + "`");
        }
    }

    @Override
    public Record createRecord(Data key, Object value, long now) {
        Record record = recordFactory.newRecord(key, value);
        record.setCreationTime(now);
        record.setLastUpdateTime(now);
        if (record.getMatchingRecordReaderWriter()
                == RecordReaderWriter.SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_READER_WRITER) {
            // To distinguish last-access-time from creation-time we
            // set last-access-time for only LRU records. A LRU record
            // has no creation-time field but last-access-time field.
            record.setLastAccessTime(now);
        }

        updateStatsOnPut(false, now);
        return record;
    }

    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new StorageImpl(memoryFormat, getExpirySystem(), serializationService);
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

    public void updateStatsOnRemove(long now) {
        stats.setLastUpdateTime(now);
    }

    protected void updateStatsOnGet(long now) {
        stats.setLastAccessTime(now);
        stats.increaseHits();
    }

    @Override
    public LocalRecordStoreStatsImpl getLocalRecordStoreStats() {
        return stats;
    }

    @Override
    public void setLocalRecordStoreStats(LocalRecordStoreStats stats) {
        this.stats.copyFrom(stats);
    }
}
