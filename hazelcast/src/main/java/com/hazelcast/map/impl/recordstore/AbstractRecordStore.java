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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.journal.MapEventJournal;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordComparator;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.monitor.LocalRecordStoreStats;
import com.hazelcast.monitor.impl.LocalRecordStoreStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.Collection;

import static com.hazelcast.map.impl.ExpirationTimeSetter.setTTLAndUpdateExpiryTime;


/**
 * Contains record store common parts.
 */
abstract class AbstractRecordStore implements RecordStore<Record> {

    protected final int partitionId;
    protected final String name;
    protected final LockStore lockStore;
    protected final MapContainer mapContainer;
    protected final RecordFactory recordFactory;
    protected final MapEventJournal eventJournal;
    protected final InMemoryFormat inMemoryFormat;
    protected final MapStoreContext mapStoreContext;
    protected final RecordComparator recordComparator;
    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;
    protected final MapDataStore<Data, Object> mapDataStore;
    protected final LocalRecordStoreStatsImpl stats = new LocalRecordStoreStatsImpl();

    protected Storage<Data, Record> storage;

    protected AbstractRecordStore(MapContainer mapContainer, int partitionId) {
        this.name = mapContainer.getName();
        this.mapContainer = mapContainer;
        this.partitionId = partitionId;
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        this.inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.recordComparator = mapServiceContext.getRecordComparator(inMemoryFormat);
        this.mapStoreContext = mapContainer.getMapStoreContext();
        this.mapDataStore = mapStoreContext.getMapStoreManager().getMapDataStore(name, partitionId);
        this.lockStore = createLockStore();
        this.eventJournal = mapServiceContext.getEventJournal();
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
    public Record createRecord(Object value, long ttlMillis, long now) {
        Record record = recordFactory.newRecord(value);
        record.setCreationTime(now);
        record.setLastUpdateTime(now);

        setTTLAndUpdateExpiryTime(ttlMillis, record, mapContainer.getMapConfig(), true);
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

    protected void updateRecord(Data key, Record record, Object value, long now, boolean countAsAccess) {
        updateStatsOnPut(countAsAccess, now);
        if (countAsAccess) {
            record.onAccess(now);
        }
        record.onUpdate(now);
        eventJournal.writeUpdateEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                record.getKey(), record.getValue(), value);
        storage.updateRecordValue(key, record, value);
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    protected void saveIndex(Record record, Object oldValue) {
        Data dataKey = record.getKey();
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.hasIndex()) {
            Object value = Records.getValueOrCachedValue(record, serializationService);
            QueryableEntry queryableEntry = mapContainer.newQueryEntry(dataKey, value);
            indexes.saveEntryIndex(queryableEntry, oldValue);
        }
    }


    protected void removeIndex(Record record) {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.hasIndex()) {
            Data key = record.getKey();
            Object value = Records.getValueOrCachedValue(record, serializationService);
            indexes.removeEntryIndex(key, value);
        }
    }

    protected void removeIndex(Collection<Record> records) {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (!indexes.hasIndex()) {
            return;
        }

        for (Record record : records) {
            removeIndex(record);
        }
    }

    protected LockStore createLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        ObjectNamespace objectNamespace = MapService.getObjectNamespace(name);
        return nodeEngine.getLockService().createLockStore(partitionId, objectNamespace);
    }

    public int getLockedEntryCount() {
        return lockStore.getLockedEntryCount();
    }

    protected RecordStoreLoader createRecordStoreLoader(MapStoreContext mapStoreContext) {
        return mapStoreContext.getMapStoreWrapper() == null
                ? RecordStoreLoader.EMPTY_LOADER : new BasicRecordStoreLoader(this);
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
