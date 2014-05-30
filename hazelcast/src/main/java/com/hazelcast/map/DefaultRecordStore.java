/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.eviction.EvictionHelper;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.PutAllOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordFactory;
import com.hazelcast.map.writebehind.DelayedEntry;
import com.hazelcast.map.writebehind.WriteBehindQueue;
import com.hazelcast.map.writebehind.WriteBehindQueues;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.util.ExceptionUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author enesakar 1/17/13
 */
public class DefaultRecordStore implements RecordStore {

    private static final long DEFAULT_TTL = -1L;
    /**
     * Number of reads before clean up.
     */
    private static final byte POST_READ_CHECK_POINT = 0x3F;
    private final String name;
    private final int partitionId;
    private final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    private final MapContainer mapContainer;
    private final MapService mapService;
    private final LockStore lockStore;
    private final RecordFactory recordFactory;
    private final ILogger logger;
    private final SizeEstimator sizeEstimator;
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final WriteBehindQueue<DelayedEntry> writeBehindQueue;
    private long lastEvictionTime;
    /**
     * If there is no clean-up caused by puts after some time,
     * count a number of gets and start eviction.
     */
    private byte readCountBeforeCleanUp;
    /**
     * To check if a key has a delayed delete operation or not.
     */
    private final Set<Data> writeBehindWaitingDeletions;
    /**
     * used for lru eviction.
     */
    private long lruAccessSequenceNumber;

    public DefaultRecordStore(String name, MapService mapService, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
        this.mapService = mapService;
        this.mapContainer = mapService.getMapContainer(name);
        this.logger = mapService.getNodeEngine().getLogger(this.getName());
        this.recordFactory = mapContainer.getRecordFactory();
        this.writeBehindQueue = WriteBehindQueues.createDefaultWriteBehindQueue(mapContainer.isWriteBehindMapStoreEnabled());
        this.writeBehindWaitingDeletions = mapContainer.isWriteBehindMapStoreEnabled()
                ? Collections.newSetFromMap(new ConcurrentHashMap()) : (Set<Data>) Collections.EMPTY_SET;
        NodeEngine nodeEngine = mapService.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null
                : lockService.createLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
        this.sizeEstimator = SizeEstimators.createMapSizeEstimator();
        loadFromMapStore(nodeEngine);
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    public void setLoaded(boolean isLoaded) {
        loaded.set(isLoaded);
    }

    public void checkIfLoaded() {
        if (mapContainer.getStore() != null && !loaded.get()) {
            throw ExceptionUtil.rethrow(new RetryableHazelcastException("Map is not ready!!!"));
        }
    }

    public String getName() {
        return name;
    }

    public void flush() {
        checkIfLoaded();
        final Collection<Data> processedKeys
                = mapContainer.getWriteBehindManager().flush(writeBehindQueue);
        for (Data pkey : processedKeys) {
            final Record record = records.get(pkey);
            if (record != null) {
                record.onStore();
            }
        }
    }

    public MapContainer getMapContainer() {
        return mapContainer;
    }

    public Record getRecord(Data key) {
        return records.get(key);
    }

    public void putForReplication(Data key, Record record) {
        final Record existingRecord = records.put(key, record);
        updateSizeEstimator(-calculateRecordSize(existingRecord));
        updateSizeEstimator(calculateRecordSize(record));
        removeFromWriteBehindWaitingDeletions(key);
    }

    public Record putBackup(Data key, Object value) {
        return putBackup(key, value, DEFAULT_TTL);
    }

    public Record putBackup(Data key, Object value, long ttl) {
        earlyWriteCleanup();

        Record record = records.get(key);
        if (record == null) {
            record = mapService.createRecord(name, key, value, ttl);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
        }
        removeFromWriteBehindWaitingDeletions(key);
        addToDelayedStore(key, record.getValue());
        return record;
    }

    public void deleteRecord(Data key) {
        Record record = records.remove(key);
        if (record != null) {
            record.invalidate();
        }
    }

    public Map<Data, Record> getReadonlyRecordMap() {
        return Collections.unmodifiableMap(records);
    }

    public void clearPartition() {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : records.keySet()) {
                indexService.removeEntryIndex(key);
            }
        }
        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        resetAccessSequenceNumber();
    }

    private void clearRecordsMap(Map<Data, Record> excludeRecords) {
        InMemoryFormat inMemoryFormat = recordFactory.getStorageFormat();
        switch (inMemoryFormat) {
            case BINARY:
            case OBJECT:
                records.clear();
                if (excludeRecords != null && !excludeRecords.isEmpty()) {
                    records.putAll(excludeRecords);
                }
                return;

            case OFFHEAP:
                Iterator<Record> iter = records.values().iterator();
                while (iter.hasNext()) {
                    Record record = iter.next();
                    if (excludeRecords == null || !excludeRecords.containsKey(record.getKey())) {
                        record.invalidate();
                        iter.remove();
                    }
                }
                return;

            default:
                throw new IllegalArgumentException("Unknown storage format: " + inMemoryFormat);
        }
    }

    /**
     * Size may not give precise size at a specific moment
     * due to the expiration logic. But eventually, it should be correct.
     *
     * @return record store size.
     */
    public int size() {
        // do not add checkIfLoaded(), size() is also used internally
        return records.size();
    }

    public boolean isEmpty() {
        checkIfLoaded();
        return records.isEmpty();
    }

    @Override
    public WriteBehindQueue<DelayedEntry> getWriteBehindQueue() {
        return writeBehindQueue;
    }

    @Override
    public List findUnlockedExpiredRecords() {
        checkIfLoaded();

        final long nowInNanos = nowInNanos();
        List<Object> expiredKeyValueSequence = Collections.emptyList();
        boolean createLazy = true;
        for (Map.Entry<Data, Record> entry : records.entrySet()) {
            final Data key = entry.getKey();
            if (isLocked(key)) {
                continue;
            }
            final Record record = entry.getValue();
            if (isReachable(record, nowInNanos)) {
                continue;
            }
            final Object value = record.getValue();
            evict(key);
            if (createLazy) {
                expiredKeyValueSequence = new ArrayList<Object>();
                createLazy = false;
            }
            expiredKeyValueSequence.add(key);
            expiredKeyValueSequence.add(value);
        }
        return expiredKeyValueSequence;
    }

    @Override
    public boolean containsValue(Object value) {
        checkIfLoaded();
        for (Record record : records.values()) {
            if (nullIfExpired(record) == null) {
                continue;
            }
            if (mapService.compare(name, value, record.getValue())) {
                return true;
            }
        }
        postReadCleanUp();
        return false;
    }

    public boolean lock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.lock(key, caller, threadId, ttl);
    }

    public boolean txnLock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.txnLock(key, caller, threadId, ttl);
    }

    public boolean extendLock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    public boolean unlock(Data key, String caller, long threadId) {
        checkIfLoaded();
        return lockStore != null && lockStore.unlock(key, caller, threadId);
    }

    public boolean forceUnlock(Data dataKey) {
        return lockStore != null && lockStore.forceUnlock(dataKey);
    }

    @Override
    public long getHeapCost() {
        return getSizeEstimator().getSize();
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public boolean canAcquireLock(Data key, String caller, long threadId) {
        return lockStore == null || lockStore.canAcquireLock(key, caller, threadId);
    }

    public String getLockOwnerInfo(Data key) {
        return lockStore != null ? lockStore.getOwnerInfo(key) : null;
    }

    public Set<Map.Entry<Data, Data>> entrySetData() {
        checkIfLoaded();
        Map<Data, Data> temp = new HashMap<Data, Data>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, mapService.toData(records.get(key).getValue()));
        }
        return temp.entrySet();
    }

    public Map.Entry<Data, Object> getMapEntry(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null) {
            record = getRecordInternal(dataKey, true);
        } else {
            accessRecord(record);
        }
        final Object data = record != null ? record.getValue() : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, data);
    }


    public Map.Entry<Data, Object> getMapEntryForBackup(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null) {
            record = getRecordInternal(dataKey, false);
        } else {
            accessRecord(record);
        }
        final Object data = record != null ? record.getValue() : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, data);
    }

    private Record getRecordInternal(Data dataKey, boolean enableIndex) {
        Record record = null;
        if (mapContainer.getStore() != null) {
            final Object value = mapContainer.getStore().load(mapService.toObject(dataKey));
            if (value != null) {
                record = mapService.createRecord(name, dataKey, value, DEFAULT_TTL);
                records.put(dataKey, record);
                if (enableIndex) {
                    saveIndex(record);
                }
                updateSizeEstimator(calculateRecordSize(record));
            }
        }
        return record;
    }

    public Set<Data> keySet() {
        checkIfLoaded();
        Set<Data> keySet = new HashSet<Data>(records.size());
        for (Data data : records.keySet()) {
            keySet.add(data);
        }
        return keySet;
    }

    public Collection<Data> valuesData() {
        checkIfLoaded();
        Collection<Data> values = new ArrayList<Data>(records.size());
        for (Record record : records.values()) {
            values.add(mapService.toData(record.getValue()));
        }
        return values;
    }

    public void clear() {
        checkIfLoaded();
        resetSizeEstimator();
        final Collection<Data> lockedKeys = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        final Map<Data, Record> lockedRecords = new HashMap<Data, Record>(lockedKeys.size());
        // Locked records should not be removed!
        for (Data key : lockedKeys) {
            Record record = records.get(key);
            if (record != null) {
                lockedRecords.put(key, record);
                updateSizeEstimator(calculateRecordSize(record));
            }
        }
        Set<Data> keysToDelete = records.keySet();
        keysToDelete.removeAll(lockedRecords.keySet());

        final MapStoreWrapper store = mapContainer.getStore();
        if (store != null) {
            // Use an ArrayList so that we don't trigger calls to equals or hashCode on the key objects
            Collection<Object> keysObject = new ArrayList<Object>(keysToDelete.size());
            for (Data key : keysToDelete) {
                keysObject.add(mapService.toObject(key));
            }

            store.deleteAll(keysObject);
        }

        removeIndex(keysToDelete);

        clearRecordsMap(lockedRecords);
        resetAccessSequenceNumber();
        writeBehindQueue.clear();
    }

    public void reset() {
        checkIfLoaded();

        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        resetAccessSequenceNumber();
        writeBehindQueue.clear();
        writeBehindWaitingDeletions.clear();
    }

    private void resetAccessSequenceNumber() {
        lruAccessSequenceNumber = 0L;
    }

    public Object evict(Data dataKey) {
        checkIfLoaded();

        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record != null) {
            mapService.interceptRemove(name, record.getValue());
            oldValue = record.getValue();
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(dataKey);
            removeIndex(dataKey);
        }
        return oldValue;
    }

    @Override
    public void removeBackup(Data key) {
        earlyWriteCleanup();

        final Record record = records.get(key);
        if (record == null) {
            return;
        }
        // reduce size
        updateSizeEstimator(-calculateRecordSize(record));
        deleteRecord(key);
        addToWriteBehindWaitingDeletions(key);
        addToDelayedStore(key, null);
    }

    @Override
    public boolean remove(Data dataKey, Object testValue) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(dataKey);
        Object oldValue = null;
        boolean removed = false;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
            }
            if (oldValue == null) {
                return false;
            }
        } else {
            oldValue = record.getValue();
        }
        if (mapService.compare(name, testValue, oldValue)) {
            mapService.interceptRemove(name, oldValue);
            removeIndex(dataKey);
            mapStoreDelete(record, dataKey);
            // reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(dataKey);
            removed = true;
        }
        return removed;
    }

    @Override
    public Object remove(Data dataKey) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (oldValue != null) {
                    removeIndex(dataKey);
                    mapStoreDelete(null, dataKey);
                }
            }
        } else {
            oldValue = record.getValue();
            oldValue = mapService.interceptRemove(name, oldValue);
            if (oldValue != null) {
                removeIndex(dataKey);
                mapStoreDelete(record, dataKey);
            }
            // reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(dataKey);
        }
        return oldValue;
    }

    @Override
    public Object get(Data key) {
        checkIfLoaded();
        if (hasWaitingWriteBehindDeleteOperation(key)) {
            // not reachable record.
            return null;
        }
        Record record = records.get(key);
        record = nullIfExpired(record);
        Object value = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                value = mapContainer.getStore().load(mapService.toObject(key));
                if (value != null) {
                    record = mapService.createRecord(name, key, value, DEFAULT_TTL);
                    records.put(key, record);
                    saveIndex(record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }
        } else {
            accessRecord(record);
            value = record.getValue();
        }
        value = mapService.interceptGet(name, value);
        postReadCleanUp();
        return value;
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet) {
        checkIfLoaded();
        final MapEntrySet mapEntrySet = new MapEntrySet();
        Map<Object, Data> keyMapForLoader = Collections.emptyMap();
        if (mapContainer.getStore() != null) {
            keyMapForLoader = new HashMap<Object, Data>();
        }
        for (Data dataKey : keySet) {
            Record record = records.get(dataKey);
            if (hasWaitingWriteBehindDeleteOperation(dataKey)) {
                continue;
            }
            if (record == null) {
                if (mapContainer.getStore() != null) {
                    keyMapForLoader.put(mapService.toObject(dataKey), dataKey);
                }
            } else {
                accessRecord(record);
                Object value = record.getValue();
                value = mapService.interceptGet(name, value);
                if (value != null) {
                    mapEntrySet.add(new AbstractMap.SimpleImmutableEntry(dataKey, mapService.toData(value)));
                }
            }
        }
        if (mapContainer.getStore() == null || keyMapForLoader.size() == 0) {
            return mapEntrySet;
        }
        final Map<Object, Object> loadedKeys = mapContainer.getStore().loadAll(keyMapForLoader.keySet());
        for (Map.Entry entry : loadedKeys.entrySet()) {
            final Object objectKey = entry.getKey();
            Object value = entry.getValue();
            Data dataKey = keyMapForLoader.get(objectKey);
            if (hasWaitingWriteBehindDeleteOperation(dataKey)) {
                continue;
            }
            if (value != null) {
                Record record = mapService.createRecord(name, dataKey, value, DEFAULT_TTL);
                records.put(dataKey, record);
                saveIndex(record);
                updateSizeEstimator(calculateRecordSize(record));
            }
            value = mapService.interceptGet(name, value);
            if (value != null) {
                mapEntrySet.add(new AbstractMap.SimpleImmutableEntry(dataKey, mapService.toData(value)));
            }
        }
        return mapEntrySet;
    }

    @Override
    public boolean containsKey(Data key) {
        checkIfLoaded();

        Record record = records.get(key);
        if (hasWaitingWriteBehindDeleteOperation(key)) {
            // not reachable record.
            return false;
        }
        record = nullIfExpired(record);

        if (record == null) {
            if (mapContainer.getStore() != null) {
                Object value = mapContainer.getStore().load(mapService.toObject(key));
                if (value != null) {
                    record = mapService.createRecord(name, key, value, DEFAULT_TTL);
                    records.put(key, record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }
        }
        boolean contains = record != null;
        if (contains) {
            accessRecord(record);
        }
        postReadCleanUp();
        return contains;
    }

    @Override
    public void put(Map.Entry<Data, Object> entry) {
        checkIfLoaded();
        earlyWriteCleanup();

        Data key = entry.getKey();
        Object value = entry.getValue();
        Record record = records.get(key);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, null);
            record = mapService.createRecord(name, key, value, DEFAULT_TTL);
            records.put(key, record);
            // increase size.
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            final Object oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            value = mapStoreWrite(key, value, record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            // then increase size
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        }
        removeFromWriteBehindWaitingDeletions(key);
    }

    public Object put(Data key, Object value, long ttl) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(key);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(key));
            }
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, null);
            record = mapService.createRecord(name, key, value, ttl);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            value = mapStoreWrite(key, value, record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            // then increase size.
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
            saveIndex(record);
        }

        removeFromWriteBehindWaitingDeletions(key);

        return oldValue;
    }

    public boolean set(Data dataKey, Object value, long ttl) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(dataKey);
        boolean newRecord = false;
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(dataKey, value, null);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
            updateSizeEstimator(calculateRecordSize(record));
            newRecord = true;
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            value = mapStoreWrite(dataKey, value, record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            // then increase size.
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return newRecord;
    }

    public boolean merge(Data key, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(key);
        Object newValue;
        if (record == null) {
            final Object notExistingKey = mapService.toObject(key);
            final EntryView<Object, Object> nullEntryView = EntryViews.createNullEntryView(notExistingKey);
            newValue = mergePolicy.merge(name, mergingEntry, nullEntryView);
            if (newValue == null) {
                return false;
            }
            newValue = mapStoreWrite(key, newValue, null);
            record = mapService.createRecord(name, key, newValue, DEFAULT_TTL);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            Object oldValue = record.getValue();
            EntryView existingEntry = EntryViews.createSimpleEntryView(mapService.toObject(record.getKey()),
                    mapService.toObject(record.getValue()), record);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(key);
                mapStoreDelete(record, key);
                // reduce size.
                updateSizeEstimator(-calculateRecordSize(record));
                //remove from map & invalidate.
                deleteRecord(key);
                return true;
            }
            // same with the existing entry so no need to mapstore etc operations.
            if (mapService.compare(name, newValue, oldValue)) {
                return true;
            }
            newValue = mapStoreWrite(key, newValue, record);
            updateSizeEstimator(-calculateRecordSize(record));
            recordFactory.setValue(record, newValue);
            updateSizeEstimator(calculateRecordSize(record));
        }
        saveIndex(record);
        return newValue != null;
    }

    public Object replace(Data dataKey, Object value) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(dataKey);
        Object oldValue;
        if (record != null && record.getValue() != null) {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            value = mapStoreWrite(dataKey, value, record);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            return null;
        }
        saveIndex(record);

        return oldValue;
    }

    public boolean replace(Data dataKey, Object testValue, Object newValue) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(dataKey);
        if (record == null) {
            return false;
        }
        if (mapService.compare(name, record.getValue(), testValue)) {
            newValue = mapService.interceptPut(name, record.getValue(), newValue);
            newValue = mapStoreWrite(dataKey, newValue, record);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, newValue);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            return false;
        }
        saveIndex(record);

        return true;
    }

    public void putTransient(Data key, Object value, long ttl) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(key);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, key, value, ttl);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);
    }

    public void putFromLoad(Data key, Object value, long ttl) {
        Record record = records.get(key);
        earlyWriteCleanup();

        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, key, value, ttl);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);
    }

    public boolean tryPut(Data key, Object value, long ttl) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(key);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, null);
            record = mapService.createRecord(name, key, value, ttl);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            value = mapStoreWrite(key, value, record);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);

        return true;
    }

    public Object putIfAbsent(Data key, Object value, long ttl) {
        checkIfLoaded();
        earlyWriteCleanup();

        Record record = records.get(key);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(key));
                if (oldValue != null) {
                    record = mapService.createRecord(name, key, oldValue, DEFAULT_TTL);
                    records.put(key, record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }
        } else {
            accessRecord(record);
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, record);
            record = mapService.createRecord(name, key, value, ttl);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);

        return oldValue;
    }

    private void loadFromMapStore(NodeEngine nodeEngine) {
        final AtomicBoolean loadOccurred = loaded;
        if (!mapContainer.isMapStoreEnabled() || loadOccurred.get()) {
            return;
        }
        final Address partitionOwner = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        final boolean isOwner = nodeEngine.getThisAddress().equals(partitionOwner);
        if (!isOwner) {
            loadOccurred.set(true);
            return;
        }
        final Map<Data, Object> loadedKeys = mapContainer.getInitialKeys();
        if (loadedKeys == null || loadedKeys.isEmpty()) {
            loadOccurred.set(true);
            return;
        }
        doChunkedLoad(loadedKeys, nodeEngine);
    }

    private void doChunkedLoad(Map<Data, Object> loadedKeys, NodeEngine nodeEngine) {
        final int mapLoadChunkSize = nodeEngine.getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
        final Queue<Map> chunks = new LinkedList<Map>();
        Map<Data, Object> partitionKeys = new HashMap<Data, Object>();
        Iterator<Map.Entry<Data, Object>> iterator = loadedKeys.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Data, Object> entry = iterator.next();
            final Data data = entry.getKey();
            if (partitionId == nodeEngine.getPartitionService().getPartitionId(data)) {
                partitionKeys.put(data, entry.getValue());
                //split into chunks
                if (partitionKeys.size() >= mapLoadChunkSize) {
                    chunks.add(partitionKeys);
                    partitionKeys = new HashMap<Data, Object>();
                }
                iterator.remove();
            }
        }
        if (!partitionKeys.isEmpty()) {
            chunks.add(partitionKeys);
        }
        if (chunks.isEmpty()) {
            loaded.set(true);
            return;
        }
        try {
            final AtomicInteger checkIfMapLoaded = new AtomicInteger(chunks.size());
            ExecutionService executionService = nodeEngine.getExecutionService();
            Map<Data, Object> chunkedKeys;
            while ((chunkedKeys = chunks.poll()) != null) {
                executionService.submit("hz:map-load", new MapLoadAllTask(chunkedKeys, checkIfMapLoaded));
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    /**
     * TODO make checkEvictable fast by carrying threshold logic to partition.
     * This cleanup adds some latency to write operations.
     * But it sweeps records much better under high write loads.
     * <p/>
     */
    private void earlyWriteCleanup() {
        if (!mapContainer.isEvictionEnabled()) {
            return;
        }
        cleanUp();
    }

    /**
     * If there is no clean-up caused by puts after some time,
     * try to clean-up from gets.
     */
    private void postReadCleanUp() {
        if (!mapContainer.isEvictionEnabled()) {
            return;
        }
        readCountBeforeCleanUp++;
        if ((readCountBeforeCleanUp & POST_READ_CHECK_POINT) == 0) {
            cleanUp();
        }
    }

    private void cleanUp() {
        final long now = System.currentTimeMillis();
        final int evictAfterMs = 1000;
        if (now - lastEvictionTime <= evictAfterMs) {
            return;
        }
        final boolean evictable = EvictionHelper.checkEvictable(mapContainer);
        if (!evictable) {
            return;
        }
        EvictionHelper.removeEvictableRecords(DefaultRecordStore.this,
                mapContainer.getMapConfig(), mapService);
        lastEvictionTime = now;
        readCountBeforeCleanUp = 0;
    }

    private Record nullIfExpired(Record record) {
        return evictIfNotReachable(record);
    }

    private void addToWriteBehindWaitingDeletions(Data key) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        writeBehindWaitingDeletions.add(key);
    }

    @Override
    public void removeFromWriteBehindWaitingDeletions(Data key) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        writeBehindWaitingDeletions.remove(key);
    }

    private boolean hasWaitingWriteBehindDeleteOperation(Data key) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return false;
        }
        return writeBehindWaitingDeletions.contains(key);
    }

    /**
     * Check if record is reachable according to ttl or idle times.
     * If not reachable return null.
     *
     * @param record {@link com.hazelcast.map.record.Record}
     * @return null if evictable.
     */
    private Record evictIfNotReachable(Record record) {
        if (record == null) {
            return null;
        }
        if (isLocked(record.getKey())) {
            return record;
        }
        if (isReachable(record)) {
            return record;
        }
        final Data key = record.getKey();
        final Object value = record.getValue();
        evict(key);
        doPostEvictionOperations(key, value);
        return null;
    }

    private boolean isReachable(Record record) {
        return isReachable(record, nowInNanos());
    }

    private boolean isReachable(Record record, long timeInNanos) {
        final Record result = mapContainer.getReachabilityHandlerChain().isReachable(record,
                -1L, timeInNanos);
        return result != null;
    }

    private void doPostEvictionOperations(Data key, Object value) {
        mapService.interceptAfterRemove(name, value);
        if (mapService.isNearCacheAndInvalidationEnabled(name)) {
            mapService.invalidateAllNearCaches(name, key);
        }
        EvictionHelper.fireEvent(key, value, name, mapService);
    }

    private void accessRecord(Record record) {
        increaseRecordEvictionCriteriaNumber(record, mapContainer.getMapConfig().getEvictionPolicy());
        record.onAccess();
    }

    private void increaseRecordEvictionCriteriaNumber(Record record, MapConfig.EvictionPolicy evictionPolicy) {
        switch (evictionPolicy) {
            case LRU:
                ++lruAccessSequenceNumber;
                record.setEvictionCriteriaNumber(lruAccessSequenceNumber);
                break;
            case LFU:
                record.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber() + 1L);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate eviction policy [" + evictionPolicy + ']');
        }
    }

    private void saveIndex(Record record) {
        Data dataKey = record.getKey();
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            SerializationService ss = mapService.getSerializationService();
            QueryableEntry queryableEntry = new QueryEntry(ss, dataKey, dataKey, record.getValue());
            indexService.saveEntryIndex(queryableEntry);
        }
    }

    private Object mapStoreWrite(Data dataKey, Object recordValue, Record record) {
        final MapStoreWrapper store = mapContainer.getStore();
        if (store == null) {
            return recordValue;
        }
        if (mapContainer.getWriteDelayMillis() < 1L) {
            Object objectValue = mapService.toObject(recordValue);
            store.store(mapService.toObject(dataKey), objectValue);
            if (record != null) {
                record.onStore();
            }
            // if store is not a post-processing map-store, then avoid extra de-serialization phase.
            return store.isPostProcessingMapStore() ? objectValue : recordValue;
        }
        addToDelayedStore(dataKey, recordValue);
        return recordValue;
    }

    private void mapStoreDelete(Record record, Data key) {
        final MapStoreWrapper store = mapContainer.getStore();
        if (store == null) {
            return;
        }
        final long writeDelayMillis = mapContainer.getWriteDelayMillis();
        if (writeDelayMillis < 1L) {
            store.delete(mapService.toObject(key));
            // todo ea record will be deleted then why calling onStore
            if (record != null) {
                record.onStore();
            }
            return;
        }
        addToDelayedStore(key, null);
    }

    /**
     * Constructs and adds a {@link com.hazelcast.map.writebehind.DelayedEntry}
     * instance to write behind queue.
     *
     * @param dataKey
     * @param recordValue
     */
    private void addToDelayedStore(Data dataKey, Object recordValue) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        final long writeDelayMillis = mapContainer.getWriteDelayMillis();
        final DelayedEntry<Data, Object> delayedEntry
                = mapService.constructDelayedEntry(dataKey, recordValue,
                partitionId, writeDelayMillis);
        // if value is null this is a delete operation.
        if (recordValue == null) {
            addToWriteBehindWaitingDeletions(dataKey);
        }
        writeBehindQueue.offer(delayedEntry);
    }

    public SizeEstimator getSizeEstimator() {
        return sizeEstimator;
    }

    private void updateTtl(Record record, long ttlInMillis) {
        if (ttlInMillis < 0L) {
            return;
        }
        final long ttlInNanos = TimeUnit.MILLISECONDS.toNanos(ttlInMillis);
        record.setTtl(ttlInNanos);

        if (record.getStatistics() != null) {
            record.getStatistics().setExpirationTime(System.nanoTime() + ttlInNanos);
        }
    }

    private static long nowInNanos() {
        return System.nanoTime();
    }

    private void updateSizeEstimator(long recordSize) {
        sizeEstimator.add(recordSize);
    }

    private long calculateRecordSize(Record record) {
        return sizeEstimator.getCost(record);
    }

    private void resetSizeEstimator() {
        sizeEstimator.reset();
    }

    private void setRecordValue(Record record, Object value) {
        accessRecord(record);
        record.onUpdate();
        recordFactory.setValue(record, value);
    }

    private void removeIndex(Data key) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            indexService.removeEntryIndex(key);
        }
    }

    private void removeIndex(Set<Data> keys) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : keys) {
                indexService.removeEntryIndex(key);
            }
        }
    }

    private final class MapLoadAllTask implements Runnable {
        private Map<Data, Object> keys;
        private AtomicInteger checkIfMapLoaded;

        private MapLoadAllTask(Map<Data, Object> keys, AtomicInteger checkIfMapLoaded) {
            this.keys = keys;
            this.checkIfMapLoaded = checkIfMapLoaded;
        }

        public void run() {
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            try {
                Map values = mapContainer.getStore().loadAll(keys.values());
                if (values == null || values.isEmpty()) {
                    if (checkIfMapLoaded.decrementAndGet() == 0) {
                        loaded.set(true);
                    }
                    return;
                }

                MapEntrySet entrySet = new MapEntrySet();
                for (Data dataKey : keys.keySet()) {
                    Object key = keys.get(dataKey);
                    Object value = values.get(key);
                    if (value != null) {
                        Data dataValue = mapService.toData(value);
                        entrySet.add(dataKey, dataValue);
                    }
                }
                PutAllOperation operation = new PutAllOperation(name, entrySet, true);
                operation.setNodeEngine(nodeEngine);
                operation.setResponseHandler(new ResponseHandler() {
                    @Override
                    public void sendResponse(Object obj) {
                        if (checkIfMapLoaded.decrementAndGet() == 0) {
                            loaded.set(true);
                        }
                    }

                    public boolean isLocal() {
                        return true;
                    }
                });
                operation.setPartitionId(partitionId);
                OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
                operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
                operation.setServiceName(MapService.SERVICE_NAME);
                nodeEngine.getOperationService().executeOperation(operation);
            } catch (Exception e) {
                logger.warning("Exception while load all task:" + e.toString());
            }
        }
    }
}
