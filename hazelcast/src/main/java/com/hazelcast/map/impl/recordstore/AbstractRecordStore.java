/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.MapStoreManager;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.nearcache.impl.invalidation.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateTTLMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickTTL;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTime;


/**
 * Contains record store common parts.
 */
abstract class AbstractRecordStore implements RecordStore<Record> {

    protected final String name;
    protected final LockStore lockStore;
    protected final RecordFactory recordFactory;
    protected final MapContainer mapContainer;
    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;
    protected final MapDataStore<Data, Object> mapDataStore;
    protected final MapStoreContext mapStoreContext;
    protected final InMemoryFormat inMemoryFormat;
    protected final int partitionId;

    protected Storage<Data, Record> storage;

    private long hits;
    private long lastAccess;
    private long lastUpdate;

    protected AbstractRecordStore(MapContainer mapContainer, int partitionId) {
        this.mapContainer = mapContainer;
        this.partitionId = partitionId;
        this.mapServiceContext = mapContainer.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.name = mapContainer.getName();
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        this.mapStoreContext = mapContainer.getMapStoreContext();
        MapStoreManager mapStoreManager = mapStoreContext.getMapStoreManager();
        this.mapDataStore = mapStoreManager.getMapDataStore(name, partitionId);
        this.lockStore = createLockStore();
    }

    @Override
    public void init() {
        this.storage = createStorage(recordFactory, inMemoryFormat);
    }

    @Override
    public Record createRecord(Object value, long ttlMillis, long now) {
        MapConfig mapConfig = mapContainer.getMapConfig();
        Record record = recordFactory.newRecord(value);
        record.setCreationTime(now);
        record.setLastUpdateTime(now);
        final long ttlMillisFromConfig = calculateTTLMillis(mapConfig);
        final long ttl = pickTTL(ttlMillis, ttlMillisFromConfig);
        record.setTtl(ttl);

        final long maxIdleMillis = calculateMaxIdleMillis(mapConfig);
        setExpirationTime(record, maxIdleMillis);
        updateStatsOnPut(true, now);
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

    protected void updateRecord(Data key, Record record, Object value, long now) {
        updateStatsOnPut(false, now);
        record.onUpdate(now);
        storage.updateRecordValue(key, record, value);
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    protected void saveIndex(Record record, Object oldValue) {
        Data dataKey = record.getKey();
        final Indexes indexes = mapContainer.getIndexes();
        if (indexes.hasIndex()) {
            Object value = Records.getValueOrCachedValue(record, serializationService);
            // When using format InMemoryFormat.NATIVE, just copy key & value to heap.
            if (NATIVE == inMemoryFormat) {
                dataKey = (Data) copyToHeap(dataKey);
                value = copyToHeap(value);
                oldValue = copyToHeap(oldValue);
            }
            QueryableEntry queryableEntry = mapContainer.newQueryEntry(dataKey, value);
            indexes.saveEntryIndex(queryableEntry, oldValue);
        }
    }


    protected void removeIndex(Record record) {
        Indexes indexes = mapContainer.getIndexes();
        if (indexes.hasIndex()) {
            Data key = record.getKey();
            Object value = Records.getValueOrCachedValue(record, serializationService);
            if (NATIVE == inMemoryFormat) {
                key = (Data) copyToHeap(key);
                value = copyToHeap(value);
            }
            indexes.removeEntryIndex(key, value);
        }
    }

    protected Object copyToHeap(Object object) {
        if (object instanceof Data) {
            return toHeapData(((Data) object));
        } else {
            return object;
        }
    }

    protected void removeIndex(Collection<Record> records) {
        Indexes indexes = mapContainer.getIndexes();
        if (!indexes.hasIndex()) {
            return;
        }

        for (Record record : records) {
            removeIndex(record);
        }
    }

    protected LockStore createLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService == null) {
            return null;
        }
        return lockService.createLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
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

    @Override
    public long getHits() {
        return hits;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccess;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdate;
    }

    @Override
    public void increaseHits() {
        this.hits++;
    }

    @Override
    public void increaseHits(long hits) {
        this.hits += hits;
    }

    @Override
    public void decreaseHits(long hits) {
        this.hits -= hits;
    }

    @Override
    public void setLastAccessTime(long time) {
        this.lastAccess = Math.max(this.lastAccess, time);
    }

    @Override
    public void setLastUpdateTime(long time) {
        this.lastUpdate = Math.max(this.lastUpdate, time);
    }


    protected void updateStatsOnPut(boolean newRecord, long now) {
        setLastUpdateTime(now);

        if (!newRecord) {
            updateStatsOnGet(now);
        }
    }

    protected void updateStatsOnPut(long hits) {
        increaseHits(hits);
    }

    protected void updateStatsOnGet(long now) {
        setLastAccessTime(now);
        increaseHits();
    }

    protected void updateStatsOnRemove(long hits) {
        decreaseHits(hits);
    }

    protected void resetStats() {
        this.hits = 0;
        this.lastAccess = 0;
        this.lastUpdate = 0;
    }
}
