/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SizeEstimator;
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
import com.hazelcast.util.Clock;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateTTLMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.pickTTL;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTime;


/**
 * Contains record store common parts.
 */
abstract class AbstractRecordStore implements RecordStore<Record> {

    protected final RecordFactory recordFactory;
    protected final String name;
    protected final MapContainer mapContainer;
    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;

    protected final MapDataStore<Data, Object> mapDataStore;

    protected final MapStoreContext mapStoreContext;

    protected final int partitionId;
    protected final InMemoryFormat inMemoryFormat;

    protected Storage<Data, Record> storage;

    protected AbstractRecordStore(MapContainer mapContainer, int partitionId) {
        this.mapContainer = mapContainer;
        this.partitionId = partitionId;
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        this.name = mapContainer.getName();
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        this.mapStoreContext = mapContainer.getMapStoreContext();
        MapStoreManager mapStoreManager = mapStoreContext.getMapStoreManager();
        this.mapDataStore = mapStoreManager.getMapDataStore(partitionId);
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
        return record;
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new StorageImpl(recordFactory, memoryFormat);
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
    public long getHeapCost() {
        return storage.getSizeEstimator().getSize();
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected void accessRecord(Record record, long now) {
        record.setLastAccessTime(now);
        record.onAccess();
    }

    protected void accessRecord(Record record) {
        final long now = getNow();
        accessRecord(record, now);
    }

    protected void updateRecord(Data key, Record record, Object value, long now) {
        accessRecord(record, now);
        record.setLastUpdateTime(now);
        record.onUpdate();
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

    protected Object copyToHeap(Object value) {
        return value instanceof Data ? toData(value) : value;
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

    protected RecordStoreLoader createRecordStoreLoader(MapStoreContext mapStoreContext) {
        return mapStoreContext.getMapStoreWrapper() == null
                ? RecordStoreLoader.EMPTY_LOADER : new BasicRecordStoreLoader(this);
    }

    protected Data toData(Object value) {
        return mapServiceContext.toData(value);
    }

    public void setSizeEstimator(SizeEstimator sizeEstimator) {
        this.storage.setSizeEstimator(sizeEstimator);
    }

    @Override
    public void dispose() {
        storage.dispose();
    }

    public Storage<Data, ? extends Record> getStorage() {
        return storage;
    }
}
